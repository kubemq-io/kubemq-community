package client

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/uuid"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	"github.com/nats-io/nuid"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kubemq-io/kubemq-community/pkg/entities"
	pb "github.com/kubemq-io/protobuf/go"

	"github.com/kubemq-io/kubemq-community/config"
	"go.uber.org/atomic"

	nats "github.com/kubemq-io/broker/client/nats"
	stan "github.com/kubemq-io/broker/client/stan"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
)

const (
	defaultWaitTimout        = 1
	defaultMaxAceMessageTime = 12 * time.Hour
)

type delayedQueueMessage struct {
	raw       *stan.Msg
	msg       *pb.QueueMessage
	delayedTo int64
	id        string
}

type byTime []*delayedQueueMessage

func (d byTime) Less(i, j int) bool {
	if d[i].delayedTo == d[j].delayedTo {
		return d[i].raw.Sequence < d[j].raw.Sequence
	}
	return d[i].delayedTo < d[j].delayedTo
}

func (d byTime) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d byTime) Len() int {
	return len(d)
}

type QueueClient struct {
	mu     sync.Mutex
	logger *logging.Logger

	isUp                      *atomic.Bool
	baseConn                  *nats.Conn
	queueConn                 stan.Conn
	writeDeadline             time.Duration
	policyCfg                 *config.QueueConfig
	delayedMessages           sync.Map
	delayProcessorDone        chan bool
	queueStreamMiddlewareFunc func(qc *QueueClient, msg *pb.StreamQueueMessagesRequest) error
	queuePollMiddlewareFunc   func(qc *QueueClient, req *pb.QueuesDownstreamRequest) error
}

func NewQueueClient(opts *Options, policyCfg *config.QueueConfig) (*QueueClient, error) {
	if opts.ClientID == "" {
		return nil, entities.ErrInvalidClientID
	}

	qc := &QueueClient{
		logger:             logging.GetLogFactory().NewLogger("client-" + opts.ClientID),
		isUp:               atomic.NewBool(false),
		policyCfg:          policyCfg,
		delayProcessorDone: make(chan bool, 1),
	}

	if opts.Deadline == 0 {
		qc.writeDeadline = defaultDeadline
	} else {
		qc.writeDeadline = opts.Deadline
	}
	var err error
	qc.queueConn, err = qc.connect(opts)
	if err != nil {
		return nil, err
	}
	qc.isUp.Store(true)

	return qc, nil
}

func (qc *QueueClient) SetQueueStreamMiddlewareFunc(fn func(qc *QueueClient, msg *pb.StreamQueueMessagesRequest) error) {
	qc.queueStreamMiddlewareFunc = fn
}
func (qc *QueueClient) SetQueuesDownstreamMiddlewareFunc(fn func(qc *QueueClient, req *pb.QueuesDownstreamRequest) error) {
	qc.queuePollMiddlewareFunc = fn
}

func (qc *QueueClient) connect(opts *Options) (stan.Conn, error) {

	natsOpts := nats.Options{
		Url: fmt.Sprintf("nats://0.0.0.0:%d", 4224),
		DisconnectedErrCB: func(conn *nats.Conn, err error) {
			conn.Close()
		},
	}
	if opts.AutoReconnect {
		natsOpts.AllowReconnect = true
		natsOpts.MaxReconnect = -1
		natsOpts.ReconnectWait = 100 * time.Millisecond
		//natsOpts.ReconnectBufSize = 1000
	}
	if opts.MemoryPipe != nil {
		natsOpts.CustomDialer = opts.MemoryPipe
	} else {
		return nil, fmt.Errorf("no memory pipe exist")
	}
	conn, err := natsOpts.Connect()
	if err != nil {
		return nil, err
	}
	qc.baseConn = conn
	storeConn, err := stan.Connect("kubemq", opts.ClientID,
		stan.NatsConn(conn),
		stan.NatsURL(fmt.Sprintf("nats://0.0.0.0:%d", 4224)),
		stan.ConnectWait(storeConnectionTime),
		stan.SetConnectionLostHandler(func(conn stan.Conn, err error) {
			qc.logger.Errorf("queue client %s connection lost", opts.ClientID)
		}),
		stan.Pings(storePingInterval, storePingMaxOut),
		stan.MaxPubAcksInflight(4096),
		stan.PubAckWait(120*time.Second),
	)

	return storeConn, err
}

func (qc *QueueClient) Disconnect() error {

	if !qc.isUp.Load() {
		return nil
	}
	qc.isUp.Store(false)
	qc.mu.Lock()
	defer qc.mu.Unlock()
	err := qc.queueConn.Close()
	if err != nil {
		return err
	}
	qc.baseConn.Close()
	return nil
}

func (qc *QueueClient) checkChannelNameValidity(channel string) error {
	if channel == "" {
		return entities.ErrInvalidQueueName
	}
	if strings.HasSuffix(channel, ".") {
		return entities.ErrInvalidQueueName
	}
	if strings.Contains(channel, " ") {
		return entities.ErrInvalidQueueName
	}
	if strings.Contains(channel, "*") || strings.Contains(channel, ">") {
		return entities.ErrInvalidQueueName
	}
	return nil
}

// function to check and enforce send message policy
func (qc *QueueClient) checkAndEnforceAckAllRequest(req *pb.AckAllQueueMessagesRequest) error {
	if req.ClientID == "" {
		return entities.ErrInvalidClientID
	}

	if err := qc.checkChannelNameValidity(req.Channel); err != nil {
		return err
	}
	if req.WaitTimeSeconds > qc.policyCfg.MaxWaitTimeoutSeconds || req.WaitTimeSeconds < 0 {
		return entities.ErrInvalidWaitTimeout
	}

	if req.WaitTimeSeconds == 0 {
		req.WaitTimeSeconds = qc.policyCfg.DefaultWaitTimeoutSeconds
	}

	return nil
}

// function to check and enforce send message policy
func (qc *QueueClient) checkAndEnforceMessagePolicy(mp *pb.QueueMessagePolicy) error {
	if mp.ExpirationSeconds > qc.policyCfg.MaxExpirationSeconds || mp.ExpirationSeconds < 0 {
		return entities.ErrInvalidExpiration
	}

	if mp.MaxReceiveCount > qc.policyCfg.MaxReceiveCount || mp.MaxReceiveCount < 0 {
		return entities.ErrInvalidMaxReceiveCount
	}

	if mp.MaxReceiveCount == 0 {
		mp.MaxReceiveCount = qc.policyCfg.MaxReceiveCount
	}

	if mp.DelaySeconds > qc.policyCfg.MaxDelaySeconds || mp.DelaySeconds < 0 {
		return entities.ErrInvalidDelay
	}
	if mp.MaxReceiveQueue != "" {
		if err := qc.checkChannelNameValidity(mp.MaxReceiveQueue); err != nil {
			return err
		}
	}
	return nil
}

// function to check and enforce receive queue message request
func (qc *QueueClient) checkAndEnforceReceiveQueueMessageRequest(req *pb.ReceiveQueueMessagesRequest) error {
	err := ValidateReceiveQueueMessageRequest(req)
	if err != nil {
		return err
	}

	if req.MaxNumberOfMessages > qc.policyCfg.MaxNumberOfMessages || req.MaxNumberOfMessages < 1 {
		return entities.ErrInvalidMaxMessages
	}

	if req.WaitTimeSeconds > qc.policyCfg.MaxWaitTimeoutSeconds || req.WaitTimeSeconds < 0 {
		return entities.ErrInvalidWaitTimeout
	}

	if req.WaitTimeSeconds == 0 {
		req.WaitTimeSeconds = qc.policyCfg.DefaultWaitTimeoutSeconds
	}

	return nil
}

// function to check and enforce receive queue message request
func (qc *QueueClient) checkAndEnforceStreamQueueMessageRequest(req *pb.StreamQueueMessagesRequest) error {
	if req.RequestID == "" {
		req.RequestID = nuid.Next()
	}

	if req.ClientID == "" {
		return entities.ErrInvalidClientID
	}

	switch req.StreamRequestTypeData {
	case pb.StreamRequestType_ReceiveMessage:
		if err := qc.checkChannelNameValidity(req.Channel); err != nil {
			return err
		}
		if req.VisibilitySeconds > qc.policyCfg.MaxVisibilitySeconds || req.VisibilitySeconds < 0 {
			return entities.ErrInvalidVisibility
		}
		if req.VisibilitySeconds == 0 {
			req.VisibilitySeconds = qc.policyCfg.DefaultVisibilitySeconds
		}

		if req.WaitTimeSeconds > qc.policyCfg.MaxWaitTimeoutSeconds || req.WaitTimeSeconds < 0 {
			return entities.ErrInvalidWaitTimeout
		}

		if req.WaitTimeSeconds == 0 {
			req.WaitTimeSeconds = qc.policyCfg.DefaultWaitTimeoutSeconds
		}

	case pb.StreamRequestType_AckMessage, pb.StreamRequestType_RejectMessage:
		if req.RefSequence == 0 {
			return entities.ErrInvalidAckSeq
		}
	case pb.StreamRequestType_ResendMessage:
		if err := qc.checkChannelNameValidity(req.Channel); err != nil {
			return err
		}
	case pb.StreamRequestType_ModifyVisibility:
		if req.VisibilitySeconds > qc.policyCfg.MaxVisibilitySeconds || req.VisibilitySeconds < 0 {
			return entities.ErrInvalidVisibility
		}
		if req.VisibilitySeconds == 0 {
			req.VisibilitySeconds = qc.policyCfg.DefaultVisibilitySeconds
		}
	case pb.StreamRequestType_SendModifiedMessage:
		if req.ModifiedMessage == nil {
			return entities.ErrInvalidQueueMessage
		}
		if req.ModifiedMessage.Attributes == nil {
			req.ModifiedMessage.Attributes = &pb.QueueMessageAttributes{
				Timestamp:         0,
				Sequence:          0,
				MD5OfBody:         "",
				ReceiveCount:      0,
				ReRouted:          false,
				ReRoutedFromQueue: "",
				ExpirationAt:      0,
				DelayedTo:         0,
			}
		}
		if req.ModifiedMessage.Policy == nil {
			req.ModifiedMessage.Policy = &pb.QueueMessagePolicy{
				ExpirationSeconds: 0,
				DelaySeconds:      0,
				MaxReceiveCount:   0,
				MaxReceiveQueue:   "",
			}
		}
		if err := ValidateQueueMessage(req.ModifiedMessage); err != nil {
			return err
		}

		if err := qc.checkAndEnforceMessagePolicy(req.ModifiedMessage.Policy); err != nil {
			return err
		}
		if req.VisibilitySeconds > qc.policyCfg.MaxVisibilitySeconds || req.VisibilitySeconds < 0 {
			return entities.ErrInvalidVisibility
		}

	default:
		return entities.ErrInvalidStreamRequestType
	}

	return nil

}

func (qc *QueueClient) sendCopyQueueMessage(msg *pb.QueueMessage, rawMsg *stan.Msg) error {
	newMessage := copyQueueMessage(msg)
	addReceiveCount(msg)
	if !qc.processMessagePolicy(msg) {
		err := rawMsg.Ack()
		return err
	}
	channel := prefixQueues + newMessage.Channel
	if newMessage.Attributes.DelayedTo > 0 {
		channel = delayQueueChannel
	}
	data, _ := newMessage.Marshal()
	return qc.queueConn.Publish(channel, data)
}

//
//func (qc *QueueClient) sendQueueMessage(ctx context.Context, msg *pb.QueueMessage) error {

//	//errChan := make(chan error, 1)
//	_, err := qc.queueConn.PublishAsync(channel, data, func(s string, err error) {
//		qc.senderAck.Inc() //	errChan <- err
//	})
//	if err != nil {
//		return err
//	}
//	//
//	return nil
//}
//
//func (qc *QueueClient) sendDelayedQueueMessage(ctx context.Context, msg *pb.QueueMessage) error {
//	data, _ := msg.Marshal()
//	return qc.queueConn.Publish(delayQueueChannel, data)
//}

func (qc *QueueClient) SetDelayMessagesProcessor(ctx context.Context) {
	go func() {
		_ = qc.startDelayedQueueProcessor(ctx)
	}()
}

func (qc *QueueClient) ShutdownDelayedMessagesProcessor() {
	qc.delayProcessorDone <- true
	time.Sleep(100 * time.Millisecond)
}

func (qc *QueueClient) sendBatchDelayedMessages(ctx context.Context, list []*delayedQueueMessage) {
	batch := &pb.QueueMessagesBatchRequest{
		BatchID:  uuid.New(),
		Messages: nil,
	}
	sort.Sort(byTime(list))
	for i := 0; i < len(list); i++ {
		batch.Messages = append(batch.Messages, list[i].msg)
	}
	_, err := qc.SendQueueMessagesBatch(ctx, batch)
	if err != nil {
		qc.logger.Errorf("error sending batch delay messages, %s", err.Error())
	}
	for _, item := range list {
		if err := item.raw.Ack(); err != nil {
			qc.logger.Errorf("error acking delayed message id %s, %s", item.id, err.Error())
		}
	}
}
func (qc *QueueClient) startDelayedQueueProcessor(ctx context.Context) error {

	rawMsgCh := make(chan *stan.Msg, 100)
	sub, err := qc.queueConn.QueueSubscribe(delayQueueChannel, delayQueueChannel, func(msg *stan.Msg) {
		select {
		case rawMsgCh <- msg:
		case <-ctx.Done():
			return
		}

	}, stan.StartAt(4), stan.DurableName(delayQueueChannel), stan.AckWait(24*time.Hour), stan.SetManualAckMode())
	if err != nil {
		qc.logger.Errorw("process start delayed queue message failed with error", "error", err.Error())
		return err
	}
	defer func() {
		err := sub.Close()
		if err != nil {
			qc.logger.Errorw("close queue subscription for delayed message processor failed with error", "error", err.Error())
		}

	}()

	go func() {

		for {
			select {
			case <-time.After(500 * time.Millisecond):
				var list []*delayedQueueMessage
				qc.delayedMessages.Range(func(key interface{}, value interface{}) bool {
					id, delayedMsg := key.(string), value.(*delayedQueueMessage)
					ts := delayedMsg.delayedTo
					// if time is not elapsed
					if ts < time.Now().UnixNano() {
						list = append(list, delayedMsg)

						qc.delayedMessages.Delete(id)
						metrics.ReportDelayed(delayedMsg.msg.Channel, -1)
					}
					return true
				})
				if len(list) > 0 {
					go qc.sendBatchDelayedMessages(ctx, list)
				}
			//	fmt.Println(len(list))
			case <-ctx.Done():
				return

			}
		}
	}()

	for {
		select {
		case rawMsg := <-rawMsgCh:
			currentQueueMsg, err := unmarshalToQueueMessage(rawMsg.Data, rawMsg.Timestamp, rawMsg.Sequence)
			if err != nil {
				continue
			}

			// storing the ts and clean the field for sending later
			ts := currentQueueMsg.Attributes.DelayedTo
			currentQueueMsg.Attributes = &pb.QueueMessageAttributes{}
			currentQueueMsg.Policy.DelaySeconds = 0
			newDelayedMsg := &delayedQueueMessage{
				raw:       rawMsg,
				msg:       currentQueueMsg,
				delayedTo: ts,
				id:        currentQueueMsg.MessageID,
			}
			qc.delayedMessages.Store(newDelayedMsg.id, newDelayedMsg)
			metrics.ReportDelayed(currentQueueMsg.Channel, 1)
		case <-qc.delayProcessorDone:
			return nil
		case <-ctx.Done():
			return nil

		}
	}

}
func (qc *QueueClient) preSendQueueMessage(msg *pb.QueueMessage) *pb.SendQueueMessageResult {

	if msg.MessageID == "" {
		msg.MessageID = nuid.Next()
	}
	sendTime := time.Now()
	msg.Attributes = &pb.QueueMessageAttributes{
		Timestamp:         0,
		Sequence:          0,
		MD5OfBody:         "",
		ReceiveCount:      0,
		ReRouted:          false,
		ReRoutedFromQueue: "",
		ExpirationAt:      0,
		DelayedTo:         0,
	}

	if msg.Policy == nil {
		msg.Policy = &pb.QueueMessagePolicy{
			ExpirationSeconds: 0,
			DelaySeconds:      0,
			MaxReceiveCount:   0,
			MaxReceiveQueue:   "",
		}
	}

	if msg.Policy.ExpirationSeconds > 0 {
		msg.Attributes.ExpirationAt = sendTime.Add(time.Duration(msg.Policy.ExpirationSeconds+msg.Policy.DelaySeconds) * time.Second).UnixNano()
	}
	if msg.Policy.DelaySeconds > 0 {
		msg.Attributes.DelayedTo = sendTime.Add(time.Duration(msg.Policy.DelaySeconds) * time.Second).UnixNano()
	}
	result := &pb.SendQueueMessageResult{
		MessageID:    msg.MessageID,
		SentAt:       0,
		ExpirationAt: 0,
		DelayedTo:    0,
		IsError:      false,
		Error:        "",
	}
	if !qc.isUp.Load() {
		result.IsError = true
		result.Error = entities.ErrConnectionNoAvailable.Error()
		return result
	}

	if err := ValidateQueueMessage(msg); err != nil {
		result.IsError = true
		result.Error = err.Error()
		return result
	}

	if err := qc.checkAndEnforceMessagePolicy(msg.Policy); err != nil {
		result.IsError = true
		result.Error = err.Error()
		return result
	}
	return result
}

func (qc *QueueClient) dispatchSync(msg *pb.QueueMessage, result *pb.SendQueueMessageResult) {
	channel := prefixQueues + msg.Channel
	if msg.Attributes.DelayedTo > 0 {
		channel = delayQueueChannel
	}
	data, _ := msg.Marshal()
	err := qc.queueConn.Publish(channel, data)
	if err != nil {
		result.IsError = true
		result.Error = err.Error()
		return
	}
	result.SentAt = time.Now().UnixNano()
	result.ExpirationAt = msg.Attributes.ExpirationAt
	result.DelayedTo = msg.Attributes.DelayedTo
}

func (qc *QueueClient) SendQueueMessage(ctx context.Context, msg *pb.QueueMessage) *pb.SendQueueMessageResult {
	result := qc.preSendQueueMessage(msg)
	if result.IsError {
		return result
	}

	qc.dispatchSync(msg, result)
	return result
}

func (qc *QueueClient) SendQueueMessagesBatch(ctx context.Context, batch *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error) {
	response := &pb.QueueMessagesBatchResponse{
		BatchID:    batch.BatchID,
		Results:    []*pb.SendQueueMessageResult{},
		HaveErrors: false,
	}
	if len(batch.Messages) == 0 {
		return response, fmt.Errorf("no messages to send")
	}

	var itemsToSend []*DispatchItem
	for i := 0; i < len(batch.Messages); i++ {
		msg := batch.Messages[i]
		result := qc.preSendQueueMessage(msg)
		response.Results = append(response.Results, result)
		if result.IsError {
			continue
		}
		channel := prefixQueues + msg.Channel
		if msg.Attributes.DelayedTo > 0 {
			channel = delayQueueChannel
		}
		data, _ := msg.Marshal()
		itemsToSend = append(itemsToSend, &DispatchItem{
			id:      i,
			channel: channel,
			data:    data,
		})
	}
	if len(itemsToSend) == 0 {
		return response, nil
	}
	ad := NewDispatcher(qc.queueConn)
	dispatchResult := ad.DispatchAsync(itemsToSend)

	for i, err := range dispatchResult {
		resultRec := response.Results[i]
		if err != nil {
			resultRec.IsError = true
			resultRec.Error = err.Error()
			response.HaveErrors = true
		} else {
			resultRec.SentAt = time.Now().UnixNano()
			resultRec.ExpirationAt = batch.Messages[i].Attributes.ExpirationAt
			resultRec.DelayedTo = batch.Messages[i].Attributes.DelayedTo
		}
	}

	return response, nil
}

func (qc *QueueClient) processMessagePolicy(msg *pb.QueueMessage) bool {
	if msg.Policy.MaxReceiveCount <= 0 {
		return true
	}
	if msg.Attributes.ReceiveCount <= msg.Policy.MaxReceiveCount {
		return true
	}

	// message exceed count policy but we don't have new queue to resend so we just return to ack the old one and continue
	if msg.Policy.MaxReceiveQueue == "" {
		return false
	}
	newMsg := copyQueueMessage(msg)
	newMsg.Attributes = &pb.QueueMessageAttributes{
		Timestamp:         0,
		Sequence:          0,
		MD5OfBody:         msg.Attributes.MD5OfBody,
		ReceiveCount:      0,
		ReRouted:          true,
		ReRoutedFromQueue: msg.Channel,
		ExpirationAt:      msg.Attributes.ExpirationAt,
		DelayedTo:         msg.Attributes.DelayedTo,
	}

	newMsg.Policy = &pb.QueueMessagePolicy{
		ExpirationSeconds: 0,
		DelaySeconds:      0,
		MaxReceiveCount:   0,
		MaxReceiveQueue:   "",
	}
	newMsg.Channel = msg.Policy.MaxReceiveQueue
	channel := prefixQueues + newMsg.Channel
	data, _ := newMsg.Marshal()
	respForMetrics := &pb.SendQueueMessageResult{}
	err := qc.queueConn.Publish(channel, data)
	if err != nil {
		respForMetrics.IsError = true
		respForMetrics.Error = err.Error()
		qc.logger.Errorw("process send message policy failed with error", "error", err.Error())
	}
	metrics.ReportSendQueueMessage(newMsg, respForMetrics)
	return false
}
func (qc *QueueClient) MonitorQueueMessages(ctx context.Context, channel string, msgCh chan *pb.QueueMessage, errCh chan error, done chan bool) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	if !qc.isUp.Load() {
		errCh <- entities.ErrConnectionNoAvailable
		return
	}
	if err := qc.checkChannelNameValidity(channel); err != nil {
		errCh <- err
		return
	}

	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	subChannel := prefixQueues + channel
	incomingMsgCh := make(chan *stan.Msg, 1)
	sub, err := qc.queueConn.QueueSubscribe(subChannel, "", func(msg *stan.Msg) {

		select {
		case incomingMsgCh <- msg:

		case <-waitCtx.Done():
		}

	}, stan.StartAt(0))
	if err != nil {
		errCh <- entities.ErrRegisterQueueSubscription
		return
	}

	defer func() {
		err = sub.Close()
		if err != nil {
			qc.logger.Errorw("close queue subscription failed with error", "queue", channel, "error", err.Error())
		}
		select {
		case done <- true:

		default:

		}
	}()
	for {
		select {
		case rawMsg := <-incomingMsgCh:
			msg, err := unmarshalToQueueMessage(rawMsg.Data, rawMsg.Timestamp, rawMsg.Sequence)
			if err != nil {
				errCh <- err
				return

			}
			select {
			case msgCh <- msg:

			case <-time.After(time.Second):

			case <-waitCtx.Done():

			}
		case <-waitCtx.Done():
			return
		}
	}

}
func (qc *QueueClient) ReceiveQueueMessages(ctx context.Context, request *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error) {
	if request.IsPeak {
		return qc.receiveQueueMessagesForPeek(ctx, request)
	}

	return qc.receiveQueueMessages(ctx, request)
}

func (qc *QueueClient) receiveQueueMessages(ctx context.Context, request *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error) {
	if !qc.isUp.Load() {
		return nil, entities.ErrConnectionNoAvailable
	}
	qc.mu.Lock()
	defer qc.mu.Unlock()

	if request.RequestID == "" {
		request.RequestID = nuid.Next()
	}
	response := &pb.ReceiveQueueMessagesResponse{
		RequestID:            request.RequestID,
		Messages:             []*pb.QueueMessage{},
		MessagesReceived:     0,
		MessagesExpired:      0,
		IsPeak:               request.IsPeak,
		IsError:              false,
		Error:                "",
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	err := qc.checkAndEnforceReceiveQueueMessageRequest(request)
	if err != nil {
		response.IsError = true
		response.Error = err.Error()
		return response, nil
	}

	subChannel := prefixQueues + request.Channel
	var waitTimeout time.Duration
	if request.WaitTimeSeconds == 0 {
		waitTimeout = time.Duration(defaultWaitTimout) * time.Second
	} else {
		waitTimeout = time.Duration(request.WaitTimeSeconds) * time.Second
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	allMessagesReceived := atomic.NewBool(false)
	incomingMsgCh := make(chan *stan.Msg, request.MaxNumberOfMessages)
	sub, err := qc.queueConn.QueueSubscribe(subChannel, subChannel, func(msg *stan.Msg) {
		if allMessagesReceived.Load() {
			return
		}
		select {
		case incomingMsgCh <- msg:

		case <-waitCtx.Done():

		case <-ctx.Done():
		}

	}, stan.StartAt(4), stan.DurableName(subChannel), stan.MaxInflight(1), stan.AckWait(120*time.Second), stan.SetManualAckMode())
	if err != nil {
		response.IsError = true
		response.Error = entities.ErrRegisterQueueSubscription.Error()
		return response, nil
	}
	defer func() {
		err = sub.Close()
		if err != nil {
			qc.logger.Errorw("close queue subscription failed with error", "queue", request.Channel, "error", err.Error())
		}
	}()
	for {

		select {
		case rawMsg := <-incomingMsgCh:
			msg, err := unmarshalToQueueMessage(rawMsg.Data, rawMsg.Timestamp, rawMsg.Sequence)
			if err != nil {
				response.IsError = true
				response.Error = entities.ErrUnmarshalUnknownType.Error()
				return response, nil
			}
			addReceiveCount(msg)
			if !qc.processMessagePolicy(msg) {
				_ = rawMsg.Ack()
				continue
			}
			if isExpired(msg) {
				response.MessagesExpired++
				metrics.ReportExpired(msg.Channel, 1)
				qc.logger.Infow("queue message expired", "queue", msg.Channel, "message_id", msg.MessageID, "metadata", msg.Metadata, "message_sequence", msg.Attributes.Sequence, "message_timestamp", msg.Attributes.Timestamp)
				_ = rawMsg.Ack()
				continue
			}

			response.MessagesReceived++
			response.Messages = append(response.Messages, msg)

			if response.MessagesReceived == request.MaxNumberOfMessages {
				allMessagesReceived.Store(true)
				_ = rawMsg.Ack()
				return response, nil
			} else {
				_ = rawMsg.Ack()
			}

		case <-waitCtx.Done():
			return response, nil
		case <-ctx.Done():
			response.MessagesReceived = 0
			response.MessagesExpired = 0
			return response, nil
		}
	}
}
func (qc *QueueClient) receiveQueueMessagesForPeek(ctx context.Context, request *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	if !qc.isUp.Load() {
		return nil, entities.ErrConnectionNoAvailable
	}
	if request.RequestID == "" {
		request.RequestID = nuid.Next()
	}
	response := &pb.ReceiveQueueMessagesResponse{
		RequestID:            request.RequestID,
		Messages:             []*pb.QueueMessage{},
		MessagesReceived:     0,
		MessagesExpired:      0,
		IsPeak:               request.IsPeak,
		IsError:              false,
		Error:                "",
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	err := qc.checkAndEnforceReceiveQueueMessageRequest(request)
	if err != nil {
		response.IsError = true
		response.Error = err.Error()
		return response, nil
	}

	subChannel := prefixQueues + request.Channel
	var waitTimeout time.Duration
	if request.WaitTimeSeconds == 0 {
		waitTimeout = time.Duration(defaultWaitTimout) * time.Second
	} else {
		waitTimeout = time.Duration(request.WaitTimeSeconds) * time.Second
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	incomingMsgCh := make(chan *stan.Msg)
	allMessagesReceived := atomic.NewBool(false)

	sub, err := qc.queueConn.QueueSubscribe(subChannel, subChannel, func(msg *stan.Msg) {
		if allMessagesReceived.Load() {
			return
		}
		select {
		case incomingMsgCh <- msg:

		case <-waitCtx.Done():

		case <-ctx.Done():
		}

	}, stan.StartAt(4), stan.DurableName(subChannel), stan.MaxInflight(int(request.MaxNumberOfMessages)), stan.AckWait(waitTimeout+time.Second), stan.SetManualAckMode())
	if err != nil {
		response.IsError = true
		response.Error = entities.ErrRegisterQueueSubscription.Error()
		return response, nil
	}
	defer func() {
		err = sub.Close()
		if err != nil {
			qc.logger.Errorw("close queue subscription failed with error", "queue", request.Channel, "error", err.Error())
		}
	}()
	var unAckMessages []*stan.Msg
	for {
		select {
		case rawMsg := <-incomingMsgCh:

			msg, err := unmarshalToQueueMessage(rawMsg.Data, rawMsg.Timestamp, rawMsg.Sequence)
			if err != nil {
				response.IsError = true
				response.Error = entities.ErrUnmarshalUnknownType.Error()
				return response, nil
			}
			addReceiveCount(msg)

			passPolicy := qc.processMessagePolicy(msg)
			if !passPolicy {
				err := rawMsg.Ack()
				if err != nil {
					qc.logger.Errorw("ack message failed with error", "seq", rawMsg.Sequence, "queue", request.Channel, "error", err.Error())
				}
				continue
			}
			if isExpired(msg) {
				response.MessagesExpired++

				qc.logger.Infow("queue message expired", "queue", msg.Channel, "message_id", msg.MessageID, "metadata", msg.Metadata, "message_sequence", msg.Attributes.Sequence, "message_timestamp", msg.Attributes.Timestamp)
				err := rawMsg.Ack()
				if err != nil {
					qc.logger.Errorw("ack message failed with error", "seq", rawMsg.Sequence, "queue", request.Channel, "error", err.Error())
				}
				metrics.ReportExpired(msg.Channel, 1)
				//unAckMessages = append(unAckMessages, rawMsg)
				continue
			} else {
				response.MessagesReceived++
				response.Messages = append(response.Messages, msg)
			}

			unAckMessages = append(unAckMessages, rawMsg)
			if response.MessagesReceived == request.MaxNumberOfMessages {
				allMessagesReceived.Store(true)
				if !request.IsPeak {
					for _, unAckMsg := range unAckMessages {
						err := unAckMsg.Ack()
						if err != nil {
							qc.logger.Errorw("ack message failed with error", "seq", unAckMsg.Sequence, "queue", request.Channel, "error", err.Error())
						}
					}
				}

				return response, nil
			}

		case <-waitCtx.Done():
			if !request.IsPeak {
				for _, unAckMsg := range unAckMessages {
					err := unAckMsg.Ack()
					if err != nil {
						qc.logger.Errorw("ack message failed with error", "seq", unAckMsg.Sequence, "queue", request.Channel, "error", err.Error())
					}
				}
			}
			return response, nil
		case <-ctx.Done():
			response.MessagesReceived = 0
			response.MessagesExpired = 0
			return response, nil
		}
	}
}

func (qc *QueueClient) AckAllQueueMessages(ctx context.Context, req *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	if !qc.isUp.Load() {
		return nil, entities.ErrConnectionNoAvailable
	}
	if req.RequestID == "" {
		req.RequestID = nuid.Next()
	}
	response := &pb.AckAllQueueMessagesResponse{
		RequestID:        req.RequestID,
		AffectedMessages: 0,
		IsError:          false,
		Error:            "",
	}
	err := qc.checkAndEnforceAckAllRequest(req)
	if err != nil {
		response.IsError = true
		response.Error = err.Error()
		return response, nil
	}

	ackContext, cancel := context.WithTimeout(ctx, time.Duration(req.WaitTimeSeconds)*time.Second)
	defer cancel()
	subChannel := prefixQueues + req.Channel
	sub, err := qc.queueConn.QueueSubscribe(subChannel, subChannel, func(msg *stan.Msg) {
		response.AffectedMessages++
	}, stan.StartAt(4), stan.DurableName(subChannel))

	if err != nil {
		response.IsError = true
		response.Error = entities.ErrRegisterQueueSubscription.Error()
		return response, nil
	}
	defer func() {
		err = sub.Close()
		if err != nil {
			qc.logger.Errorw("peek queue close subscription failed with error", "queue", req.Channel, "error", err.Error())
		}
	}()
	<-ackContext.Done()
	return response, nil
}

func (qc *QueueClient) StreamQueueMessage(parentCtx context.Context, requests chan *pb.StreamQueueMessagesRequest, response chan *pb.StreamQueueMessagesResponse, done chan bool) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	var sub stan.Subscription
	var rawMsg *stan.Msg
	var currentQueueMsg *pb.QueueMessage
	var currentRequest *pb.StreamQueueMessagesRequest
	var err error
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	visibilityTimer := time.NewTimer(time.Hour * 24)
	waitTimeoutTimer := time.NewTimer(time.Hour * 24)
	msgCh := make(chan *stan.Msg)
	responseInternalCh := make(chan *pb.StreamQueueMessagesResponse, 1)
	isSubActive := false

	messageInProcess := atomic.NewBool(false)
	activeQueueName := "undefined"
	go func() {
		for {
			select {
			case resp := <-responseInternalCh:
				select {
				case response <- resp:

					msg := resp.Message
					if msg != nil {
						qc.logger.Debugw("send queue message to receiver", "queue", msg.Channel, "client_id", msg.ClientID, "message_id", msg.MessageID, "metadata", msg.Metadata, "message_sequence", msg.Attributes.Sequence, "message_timestamp", msg.Attributes.Timestamp)
					} else {
						qc.logger.Debugw("send response to stream queue request", "queue", activeQueueName, "request_id", resp.RequestID, "type", resp.StreamRequestTypeData, "is_error", resp.IsError, "error", resp.Error)
					}

				case <-ctx.Done():
					qc.logger.Debugw("stream queue message completed", "queue", activeQueueName)
					return
				}
			case <-ctx.Done():
				qc.logger.Debugw("stream queue message completed", "queue", activeQueueName)
				return
			}
		}
	}()
	defer func() {
		if isSubActive {
			err := sub.Close()
			if err != nil {
				qc.logger.Errorw("stream queue message completed with error", "queue", activeQueueName, "err", err.Error())
			}

		} else {
			qc.logger.Debugw("stream queue message completed without active subscription", "queue", activeQueueName)
		}

	}()

	defer func() {
		done <- true
	}()
	for {
		select {
		case currentRequest = <-requests:
			// checking if we got nil as data typically happens after disconnect oc ctx cancellation
			if currentRequest == nil {
				continue
			}
			if currentRequest.RequestID == "" {
				currentRequest.RequestID = nuid.Next()
			}
			qc.logger.Debugw("stream queue request received:",
				"queue", activeQueueName,
				"channel", currentRequest.Channel,
				"client_id", currentRequest.ClientID,
				"request_id", currentRequest.RequestID,
				"type", currentRequest.StreamRequestTypeData,
				"message_sequence", currentRequest.RefSequence,
				"visibility_seconds", currentRequest.VisibilitySeconds,
				"wait_seconds", currentRequest.WaitTimeSeconds,
				"resend_queue", currentRequest.Channel)

			if qc.queueStreamMiddlewareFunc != nil {
				err := qc.queueStreamMiddlewareFunc(qc, currentRequest)
				if err != nil {
					qc.logger.Errorw("middleware request error", "queue", activeQueueName, "request", currentRequest, "error", err.Error())
					responseInternalCh <- createResponseWithError(currentRequest, err)
					return
				}
			}

			err := qc.checkAndEnforceStreamQueueMessageRequest(currentRequest)
			if err != nil {
				qc.logger.Errorw("invalid request error", "queue", activeQueueName, "request", currentRequest, "error", err.Error())
				responseInternalCh <- createResponseWithError(currentRequest, err)
				continue
			}
			switch currentRequest.StreamRequestTypeData {

			// start sub to a queue and set visibility time
			case pb.StreamRequestType_ReceiveMessage:
				subQueueName := prefixQueues + currentRequest.Channel
				activeQueueName = subQueueName
				// check if we already in subscription mode
				if isSubActive {
					qc.logger.Errorw("waiting for stream queue message subscription is active already ", "queue", activeQueueName)
					responseInternalCh <- createResponseWithError(currentRequest, entities.ErrSubscriptionIsActive)
					continue
				}

				// subscribe to durable channel
				sub, err = qc.queueConn.QueueSubscribe(subQueueName, subQueueName, func(msg *stan.Msg) {
					if !messageInProcess.Load() {
						select {
						case msgCh <- msg:
							messageInProcess.Swap(true)
						case <-ctx.Done():
						}
					} else {
						return
					}

				}, stan.StartAt(4), stan.DurableName(subQueueName), stan.MaxInflight(1), stan.AckWait(defaultMaxAceMessageTime), stan.SetManualAckMode())

				if err != nil {
					// subscribe failed
					qc.logger.Errorw("stream queue message subscribe error", "queue", activeQueueName, "error", err.Error())
					responseInternalCh <- createResponseWithError(currentRequest, entities.ErrRegisterQueueSubscription)
					continue
				}
				// subscription was successful
				isSubActive = true
				waitTimeoutTimer.Stop()
				waitTimeoutTimer.Reset(time.Duration(currentRequest.WaitTimeSeconds) * time.Second)

			// Handling Ack Requests
			case pb.StreamRequestType_AckMessage:

				// checking if we have already currentQueueMsg form the queue
				if rawMsg != nil {
					// check if the seq matches in order not to ack a wrong currentQueueMsg
					if rawMsg.Sequence != currentRequest.RefSequence {
						qc.logger.Errorw("ack - invalid message seq request", "queue", activeQueueName, "request_seq", currentRequest.RefSequence, "current_seq", rawMsg.Sequence)
						responseInternalCh <- createResponseWithError(currentRequest, entities.ErrInvalidAckSeq)
						continue
					}

					err := rawMsg.Ack()
					if err != nil {
						qc.logger.Errorw("ack queue message error", "queue", activeQueueName, "message_seq", currentRequest.RefSequence, "error", err.Error())
						responseInternalCh <- createResponseWithError(currentRequest, entities.ErrAckQueueMsg)
						continue
					}
					responseInternalCh <- createResponse(currentRequest)
					// we ack the message and we are done
					return
				} else {
					qc.logger.Errorw("ack queue message error", "queue", activeQueueName, "message_seq", currentRequest.RefSequence, "error", entities.ErrNoCurrentMsgToAck.Error())
					responseInternalCh <- createResponseWithError(currentRequest, entities.ErrNoCurrentMsgToAck)
					continue
				}
				// Handling Reject Requests
			case pb.StreamRequestType_RejectMessage:
				if rawMsg != nil {
					// check if the seq matches in order not to ack a wrong currentQueueMsg
					if rawMsg.Sequence != currentRequest.RefSequence {
						qc.logger.Errorw("reject - invalid message seq request", "queue", activeQueueName, "request_seq", currentRequest.RefSequence, "current_seq", rawMsg.Sequence)
						responseInternalCh <- createResponseWithError(currentRequest, entities.ErrInvalidAckSeq)
						continue
					}
					err = qc.sendCopyQueueMessage(currentQueueMsg, rawMsg)
					if err != nil {
						qc.logger.Errorw("reject - sending back to the queue message failed", "queue", activeQueueName, "error", err.Error())
						responseInternalCh <- createResponseWithError(currentRequest, entities.ErrSendingQueueMessage)
					}
					responseInternalCh <- createResponse(currentRequest)
					err := rawMsg.Ack()
					if err != nil {
						qc.logger.Errorw("reject - ack queue message error", "queue", activeQueueName, "message_seq", currentRequest.RefSequence, "error", err.Error())
						responseInternalCh <- createResponseWithError(currentRequest, entities.ErrAckQueueMsg)

						return
					}
					return
				} else {
					qc.logger.Errorw("reject - no active message to reject error", "queue", activeQueueName, "error", entities.ErrNoActiveMessageToReject)
					responseInternalCh <- createResponseWithError(currentRequest, entities.ErrNoActiveMessageToReject)
					continue
				}

			case pb.StreamRequestType_ResendMessage:
				if rawMsg != nil {
					newMessage := copyQueueMessage(currentQueueMsg)
					newMessage.Attributes.ReceiveCount = 0
					newMessage.Channel = currentRequest.Channel
					result := qc.SendQueueMessage(ctx, newMessage)
					if result.IsError {
						qc.logger.Errorw("resend - send to new a queue failed", "queue", activeQueueName, "error", result.Error)
						responseInternalCh <- createResponseWithError(currentRequest, entities.ErrSendingQueueMessage)
						continue
					}
					responseInternalCh <- createResponse(currentRequest)
					err := rawMsg.Ack()
					if err != nil {
						qc.logger.Errorw("resend - ack before resend seq error", "queue", activeQueueName, "seq", rawMsg.Sequence, "error", err.Error())
						responseInternalCh <- createResponseWithError(currentRequest, entities.ErrAckQueueMsg)
						continue
					}

					rawMsg = nil
					return

				} else {
					qc.logger.Errorw("resend - ack and resend error", "queue", activeQueueName, "error", entities.ErrNoCurrentMsgToSend.Error())
					responseInternalCh <- createResponseWithError(currentRequest, entities.ErrNoCurrentMsgToSend)
					continue
				}

			case pb.StreamRequestType_SendModifiedMessage:
				if rawMsg != nil {
					currentRequest.ModifiedMessage.Attributes = &pb.QueueMessageAttributes{}
					result := qc.SendQueueMessage(ctx, currentRequest.ModifiedMessage)
					if result.IsError {
						qc.logger.Errorw("send modified - send to new queue failed", "queue", activeQueueName, "error", result.Error)
						responseInternalCh <- createResponseWithError(currentRequest, entities.ErrSendingQueueMessage)
						continue
					}
					responseInternalCh <- createResponse(currentRequest)
					err := rawMsg.Ack()
					if err != nil {
						qc.logger.Debugw("send modified - ack error", "queue", activeQueueName, "seq", rawMsg.Sequence, "error", err.Error())
						responseInternalCh <- createResponseWithError(currentRequest, entities.ErrAckQueueMsg)
						continue
					}
					return
				} else {
					qc.logger.Debugw("send modified - ack and resend error", "queue", activeQueueName, "error", entities.ErrNoCurrentMsgToAck.Error())
					responseInternalCh <- createResponseWithError(currentRequest, entities.ErrNoCurrentMsgToAck)
					continue
				}

			case pb.StreamRequestType_ModifyVisibility:
				visibilityTimer.Stop()
				visibilityTimer.Reset(time.Duration(currentRequest.VisibilitySeconds) * time.Second)
				responseInternalCh <- createResponse(currentRequest)
				continue
			default:

			}
		case <-visibilityTimer.C:
			visibilityTimer.Stop()
			qc.logger.Debugw("visibility timer for expired", "queue", activeQueueName, "request_id", currentRequest.RequestID)
			responseInternalCh <- createResponseWithError(currentRequest, entities.ErrVisibilityExpired)
			if rawMsg != nil {
				err := rawMsg.Ack()
				if err != nil {
					qc.logger.Errorw("visibility - ack seq error", "queue", activeQueueName, "seq", rawMsg.Sequence, "error", err.Error())
					responseInternalCh <- createResponseWithError(currentRequest, entities.ErrAckQueueMsg)

					return
				}
				err = qc.sendCopyQueueMessage(currentQueueMsg, rawMsg)
				if err != nil {
					qc.logger.Errorw("visibility - send message back to the queue failed", "queue", activeQueueName, "error", err.Error())
					responseInternalCh <- createResponseWithError(currentRequest, entities.ErrSendingQueueMessage)
				}
				return
			}
			return
		case <-waitTimeoutTimer.C:
			waitTimeoutTimer.Stop()
			visibilityTimer.Stop()
			qc.logger.Debugw("wait timeout - timer for request expired", "queue", activeQueueName, "request_id", currentRequest.RequestID)
			responseInternalCh <- createResponseWithError(currentRequest, entities.ErrNoNewMessageQueue)
			return

		case rawMsg = <-msgCh:
			messageInProcess.Store(true)

			waitTimeoutTimer.Stop()
			// starting visibility timer
			visibilityTimer.Stop()
			visibilityTimer = time.NewTimer(time.Duration(currentRequest.VisibilitySeconds) * time.Second)

			//			qc.logger.Debugw(fmt.Sprintf("new raw message receive for process %v", rawMsg), "queue", activeQueueName)
			currentQueueMsg, err = unmarshalToQueueMessage(rawMsg.Data, rawMsg.Timestamp, rawMsg.Sequence)
			if err != nil {
				// invalid message format
				qc.logger.Errorw("error unmarshal receive current queue message", "queue", activeQueueName, "error", err.Error())
				responseInternalCh <- createResponseWithError(currentRequest, entities.ErrUnmarshalUnknownType)
				messageInProcess.Store(false)
				continue
			}
			currentQueueMsg = addReceiveCount(currentQueueMsg)
			// check policy and if the current message comply we proceed
			passPolicy := qc.processMessagePolicy(currentQueueMsg)
			if !passPolicy {
				// the received message policy failed so we ack the message and wait for the next one
				err := rawMsg.Ack()
				if err != nil {
					qc.logger.Errorw("ack seq failed with error", "queue", activeQueueName, "seq", rawMsg.Sequence, "error", err.Error())
				}
				rawMsg = nil
				messageInProcess.Store(false)
				continue
			}
			// we check if the received message expired, if yes , we ack and wait for the next one
			if isExpired(currentQueueMsg) {
				err := rawMsg.Ack()
				if err != nil {
					qc.logger.Errorw("ack seq failed with error", "queue", activeQueueName, "seq", rawMsg.Sequence, "error", err.Error())
				}
				rawMsg = nil
				metrics.ReportExpired(currentQueueMsg.Channel, 1)
				messageInProcess.Store(false)
				continue
			}

			responseInternalCh <- createResponseWithMessage(currentRequest, currentQueueMsg)

		case <-ctx.Done():
			qc.logger.Debugw("context cancelled", "queue", activeQueueName)
			if rawMsg != nil {
				qc.logger.Debugw("context canceled, redeliver active msg", "queue", activeQueueName)
				err := rawMsg.Ack()
				if err != nil {
					qc.logger.Errorw("ack seq failed with error", "queue", activeQueueName, "seq", rawMsg.Sequence, "error", err.Error())
					responseInternalCh <- createResponseWithError(currentRequest, entities.ErrAckQueueMsg)
					return
				}
				err = qc.sendCopyQueueMessage(currentQueueMsg, rawMsg)
				if err != nil {
					qc.logger.Error("sending back to queue message failed", "queue", activeQueueName, "error", err.Error())
					responseInternalCh <- createResponseWithError(currentRequest, entities.ErrSendingQueueMessage)
				}

				return
			}
			return
		}
	}

}

func unmarshalToQueueMessage(data []byte, timestamp int64, seq uint64) (*pb.QueueMessage, error) {
	msg := &pb.QueueMessage{
		MessageID:  "",
		ClientID:   "",
		Channel:    "",
		Metadata:   "",
		Body:       nil,
		Attributes: nil,
		Policy:     nil,
	}
	err := msg.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	msg.Attributes.Timestamp = timestamp
	msg.Attributes.Sequence = seq
	return msg, nil
}

func isExpired(qm *pb.QueueMessage) bool {
	if qm.Attributes.ExpirationAt > 0 {
		return qm.Attributes.ExpirationAt < time.Now().UnixNano()
	}
	return false
}
func copyQueueMessage(qm *pb.QueueMessage) *pb.QueueMessage {
	msg := &pb.QueueMessage{
		MessageID:  qm.MessageID,
		ClientID:   qm.ClientID,
		Channel:    qm.Channel,
		Metadata:   qm.Metadata,
		Body:       qm.Body,
		Tags:       nil,
		Attributes: nil,
		Policy:     nil,
	}
	if qm.Tags != nil {
		msg.Tags = make(map[string]string)
		for key, value := range qm.Tags {
			msg.Tags[key] = value
		}
	}

	if qm.Attributes != nil {
		attr := qm.Attributes
		msg.Attributes = &pb.QueueMessageAttributes{
			Timestamp:         attr.Timestamp,
			Sequence:          attr.Sequence,
			MD5OfBody:         attr.MD5OfBody,
			ReceiveCount:      attr.ReceiveCount,
			ReRouted:          attr.ReRouted,
			ReRoutedFromQueue: attr.ReRoutedFromQueue,
			ExpirationAt:      attr.ExpirationAt,
			DelayedTo:         attr.DelayedTo,
		}

	}
	if qm.Policy != nil {
		p := qm.Policy
		msg.Policy = &pb.QueueMessagePolicy{
			ExpirationSeconds: p.ExpirationSeconds,
			DelaySeconds:      p.DelaySeconds,
			MaxReceiveCount:   p.MaxReceiveCount,
			MaxReceiveQueue:   p.MaxReceiveQueue,
		}
	}

	return msg
}

func addReceiveCount(qm *pb.QueueMessage) *pb.QueueMessage {
	qm.Attributes.ReceiveCount++
	return qm
}

func createResponse(sqr *pb.StreamQueueMessagesRequest) *pb.StreamQueueMessagesResponse {
	return &pb.StreamQueueMessagesResponse{
		RequestID:             sqr.RequestID,
		StreamRequestTypeData: sqr.StreamRequestTypeData,
		Message:               nil,
		IsError:               false,
		Error:                 "",
		XXX_NoUnkeyedLiteral:  struct{}{},
		XXX_sizecache:         0,
	}
}
func createResponseWithError(sqr *pb.StreamQueueMessagesRequest, err error) *pb.StreamQueueMessagesResponse {
	return &pb.StreamQueueMessagesResponse{
		RequestID:             sqr.RequestID,
		StreamRequestTypeData: sqr.StreamRequestTypeData,
		Message:               nil,
		IsError:               true,
		Error:                 err.Error(),
		XXX_NoUnkeyedLiteral:  struct{}{},
		XXX_sizecache:         0,
	}
}
func createResponseWithMessage(sqr *pb.StreamQueueMessagesRequest, msg *pb.QueueMessage) *pb.StreamQueueMessagesResponse {
	return &pb.StreamQueueMessagesResponse{
		RequestID:             sqr.RequestID,
		StreamRequestTypeData: sqr.StreamRequestTypeData,
		Message:               msg,
		IsError:               false,
		Error:                 "",
		XXX_NoUnkeyedLiteral:  struct{}{},
		XXX_sizecache:         0,
	}
}

func (qc *QueueClient) Poll(ctx context.Context, request *QueueDownstreamRequest) (*QueueDownstreamResponse, error) {

	qc.mu.Lock()
	defer qc.mu.Unlock()

	if !qc.isUp.Load() {
		return nil, entities.ErrConnectionNoAvailable
	}

	request.applyDefaults()
	if err := ValidatePollRequest(request.QueuesDownstreamRequest); err != nil {
		return nil, err
	}
	if qc.queuePollMiddlewareFunc != nil {
		err := qc.queuePollMiddlewareFunc(qc, request.QueuesDownstreamRequest)
		if err != nil {
			qc.logger.Errorw("middleware request error", "queue", request.Channel, "request", request.TransactionId, "error", err.Error())
			return nil, err
		}
	}
	response := NewPollResponse(request).
		setSendCopyFunc(qc.sendCopyQueueMessage).
		setReRouteFunc(qc.SendQueueMessage)
	subChannel := prefixQueues + request.Channel
	var waitTimeout time.Duration
	if request.WaitTimeout == 0 {
		waitTimeout = time.Duration(defaultWaitTimout) * time.Second
	} else {
		waitTimeout = time.Duration(request.WaitTimeout) * time.Millisecond
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), waitTimeout)
	defer waitCancel()

	sub, err := qc.queueConn.QueueSubscribe(subChannel, subChannel, func(msg *stan.Msg) {

		select {

		case <-waitCtx.Done():
			//select {
			//case <-response.subscriptionWait:
			//}

			return
		default:

		}
		if response.acceptMessages.Load() && response.items() <= int(request.MaxItems) {
			response.addMessage(msg)
		} else {
			<-response.subscriptionWait
			return
		}

	}, stan.StartAt(4), stan.DurableName(subChannel), stan.MaxInflight(int(request.MaxItems)), stan.AckWait(365*24*time.Hour), stan.SetManualAckMode())
	if err != nil {
		return nil, entities.ErrRegisterQueueSubscription
	}

	response.setSubscription(sub)
	select {
	case <-response.itemsReady:

	case <-waitCtx.Done():

	case <-ctx.Done():
		response.Close()
		return nil, fmt.Errorf("context canceled during poll process")
	}

	if response.items() > 0 {

		response.processRawMessages(qc.processMessagePolicy)
		if request.AutoAck {

			err := response.AckAll()
			if err != nil {
				response.Close()
				return nil, err
			}
		}
	} else {
		response.Close()
	}
	return response, nil
}
