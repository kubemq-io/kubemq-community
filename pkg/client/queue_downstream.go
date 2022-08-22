package client

import (
	"context"
	"fmt"
	"github.com/kubemq-io/broker/client/stan"
	"github.com/kubemq-io/kubemq-community/pkg/uuid"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	pb "github.com/kubemq-io/protobuf/go"
	"go.uber.org/atomic"
	"sync"
)

const minWaitTime = 1000
const maxMaxItems = 200000

type QueueDownstreamRequest struct {
	*pb.QueuesDownstreamRequest
	TransactionId string
	//OnCompleteFunc func(response *QueueDownstreamResponse)
}

func NewPollRequest(pb *pb.QueuesDownstreamRequest, transactionId string) *QueueDownstreamRequest {
	return &QueueDownstreamRequest{
		QueuesDownstreamRequest: pb,
		TransactionId:           transactionId,
	}
}
func (p *QueueDownstreamRequest) applyDefaults() {
	if p.TransactionId == "" {
		p.TransactionId = uuid.New()
	}
	if p.MaxItems <= 0 {
		p.MaxItems = maxMaxItems
	}
	if p.WaitTimeout < minWaitTime {
		p.WaitTimeout = minWaitTime
	}
}
func (p *QueueDownstreamRequest) SetChannel(channel string) *QueueDownstreamRequest {
	p.Channel = channel
	return p
}

func (p *QueueDownstreamRequest) SetMaxItems(maxItems int) *QueueDownstreamRequest {
	p.MaxItems = int32(maxItems)
	return p
}

func (p *QueueDownstreamRequest) SetWaitTimeout(waitTimeout int) *QueueDownstreamRequest {
	p.WaitTimeout = int32(waitTimeout)
	return p
}

func (p *QueueDownstreamRequest) SetAutoAck(autoAck bool) *QueueDownstreamRequest {
	p.AutoAck = autoAck
	return p
}

type QueueDownstreamResponse struct {
	sync.Mutex
	*pb.QueuesDownstreamResponse
	request            *QueueDownstreamRequest
	subscription       stan.Subscription
	unAckedList        []int64
	unAckedRawMessages map[int64]*stan.Msg
	subscriptionWait   chan struct{}
	itemsReady         chan struct{}
	sessionDone        *atomic.Bool
	sendCopyFunc       func(msg *pb.QueueMessage, rawMsg *stan.Msg) error
	reRouteFunc        func(ctx context.Context, msg *pb.QueueMessage) *pb.SendQueueMessageResult
	closeResponseCh    chan bool
	closeComplete      chan bool
	acceptMessages     *atomic.Bool
}

func NewPollResponse(request *QueueDownstreamRequest) *QueueDownstreamResponse {
	p := &QueueDownstreamResponse{
		Mutex: sync.Mutex{},
		QueuesDownstreamResponse: &pb.QueuesDownstreamResponse{
			TransactionId:   request.TransactionId,
			RefRequestId:    request.RequestID,
			RequestTypeData: pb.QueuesDownstreamRequestType_Get,
			Messages:        nil,
			IsError:         false,
			Error:           "",
			Metadata:        nil,
		},
		request:            request,
		subscription:       nil,
		unAckedList:        nil,
		unAckedRawMessages: make(map[int64]*stan.Msg),
		subscriptionWait:   make(chan struct{}, 1),
		itemsReady:         make(chan struct{}, 1),
		sessionDone:        atomic.NewBool(false),
		sendCopyFunc:       nil,
		reRouteFunc:        nil,
		closeResponseCh:    make(chan bool, 2),
		closeComplete:      make(chan bool, 2),
		acceptMessages:     atomic.NewBool(true),
	}

	return p
}
func (p *QueueDownstreamResponse) setSendCopyFunc(sendCopyFunc func(msg *pb.QueueMessage, rawMsg *stan.Msg) error) *QueueDownstreamResponse {
	p.sendCopyFunc = sendCopyFunc
	return p
}
func (p *QueueDownstreamResponse) setReRouteFunc(reRouteFunc func(ctx context.Context, msg *pb.QueueMessage) *pb.SendQueueMessageResult) *QueueDownstreamResponse {
	p.reRouteFunc = reRouteFunc
	return p
}

func (p *QueueDownstreamResponse) TransactionId() string {
	return p.request.TransactionId
}

func (p *QueueDownstreamResponse) setSubscription(subscription stan.Subscription) {
	p.subscription = subscription
}

func (p *QueueDownstreamResponse) items() int {
	p.Lock()
	defer p.Unlock()
	return len(p.unAckedRawMessages)
}
func (p *QueueDownstreamResponse) addMessage(msg *stan.Msg) {
	if p.sessionDone.Load() {
		return
	}
	p.Lock()
	defer p.Unlock()

	p.unAckedRawMessages[int64(msg.Sequence)] = msg
	p.unAckedList = append(p.unAckedList, int64(msg.Sequence))
	if p.request.MaxItems > 0 {
		if len(p.unAckedRawMessages) >= int(p.request.MaxItems) {

			p.acceptMessages.Store(false)
			p.itemsReady <- struct{}{}
		}
	}
}
func (p *QueueDownstreamResponse) NackAll() error {
	if p.sessionDone.Load() {
		return fmt.Errorf("cannot nack messages with closed response ")
	}
	p.Lock()
	defer p.Unlock()
	for seq, msg := range p.unAckedRawMessages {
		nackedMsg, err := unmarshalToQueueMessage(msg.Data, msg.Timestamp, msg.Sequence)
		if err != nil {
			continue
		}
		nackedMsg = addReceiveCount(nackedMsg)
		err = p.sendCopyFunc(nackedMsg, msg)
		if err != nil {
			return fmt.Errorf("nack message with seq %d failed, %s", seq, err.Error())
		}
		err = msg.Ack()
		if err != nil {
			return err
		}
		delete(p.unAckedRawMessages, seq)
	}
	p.close()
	return nil
}
func (p *QueueDownstreamResponse) NackRange(seqRange []int64) error {
	if p.sessionDone.Load() {
		return fmt.Errorf("cannot nack messages with closed response ")
	}
	if len(seqRange) == 0 {
		return fmt.Errorf("cannot nack empty list of of offsets")
	}
	p.Lock()
	defer p.Unlock()
	for _, seq := range seqRange {
		msg, ok := p.unAckedRawMessages[seq]
		if ok {
			nackedMsg, err := unmarshalToQueueMessage(msg.Data, msg.Timestamp, msg.Sequence)
			if err != nil {
				continue
			}
			nackedMsg = addReceiveCount(nackedMsg)
			err = p.sendCopyFunc(nackedMsg, msg)
			if err != nil {
				return fmt.Errorf("nack message with seq %d failed, %s", seq, err.Error())
			}
			err = msg.Ack()
			if err != nil {
				return err
			}
			delete(p.unAckedRawMessages, seq)
		} else {
			return fmt.Errorf("invalid nack sequence id %d", seq)
		}

	}
	if len(p.unAckedRawMessages) == 0 {
		p.close()
	}
	return nil
}
func (p *QueueDownstreamResponse) AckAll() error {
	if p.sessionDone.Load() {
		return fmt.Errorf("cannot ack messages with closed response ")
	}
	p.Lock()
	defer p.Unlock()

	for seq, msg := range p.unAckedRawMessages {
		err := msg.Ack()
		if err != nil {
			return err
		}
		delete(p.unAckedRawMessages, seq)
	}
	p.close()
	return nil
}

func (p *QueueDownstreamResponse) AckRange(seqRange []int64) error {
	if p.sessionDone.Load() {
		return fmt.Errorf("cannot ack messages with closed response ")
	}
	if len(seqRange) == 0 {
		return fmt.Errorf("cannot ack empty list of of offsets")
	}
	p.Lock()
	defer p.Unlock()

	for _, seq := range seqRange {
		msg, ok := p.unAckedRawMessages[seq]
		if ok {
			err := msg.Ack()
			if err != nil {
				return err
			}
			delete(p.unAckedRawMessages, seq)
		} else {
			return fmt.Errorf("invalid ack sequence id %d", seq)
		}
	}
	if len(p.unAckedRawMessages) == 0 {
		p.close()
	}
	return nil
}
func (p *QueueDownstreamResponse) ReQueueAll(channel string) error {
	if p.sessionDone.Load() {
		return fmt.Errorf("cannot ack messages with closed response ")
	}
	if channel == "" {
		return fmt.Errorf("cannot requeue messages with empty channel destination ")
	}
	p.Lock()
	defer p.Unlock()

	for seq, msg := range p.unAckedRawMessages {
		currentMsg, err := unmarshalToQueueMessage(msg.Data, msg.Timestamp, msg.Sequence)
		if err != nil {
			continue
		}
		newMessage := copyQueueMessage(currentMsg)
		newMessage.Attributes.ReceiveCount = 0
		newMessage.Attributes.ReRouted = true
		newMessage.Attributes.ReRoutedFromQueue = currentMsg.Channel
		newMessage.Channel = channel
		result := p.reRouteFunc(context.Background(), newMessage)
		if result.IsError {
			return fmt.Errorf("re-queue message with seq %d failed, %s", seq, result.Error)
		}

		err = msg.Ack()
		if err != nil {
			return err
		}
		delete(p.unAckedRawMessages, seq)
	}
	p.close()
	return nil
}
func (p *QueueDownstreamResponse) ReQueueRange(seqRange []int64, channel string) error {
	if p.sessionDone.Load() {
		return fmt.Errorf("cannot requeue messages with closed response ")
	}
	if channel == "" {
		return fmt.Errorf("cannot requeue messages with empty channel destination ")
	}
	if len(seqRange) == 0 {
		return fmt.Errorf("cannot requeue empty list of of offsets")
	}
	p.Lock()
	defer p.Unlock()
	for _, seq := range seqRange {
		msg, ok := p.unAckedRawMessages[seq]
		if !ok {
			return fmt.Errorf("invalid ack sequence id %d", seq)
		}
		currentMsg, err := unmarshalToQueueMessage(msg.Data, msg.Timestamp, msg.Sequence)
		if err != nil {
			continue
		}
		newMessage := copyQueueMessage(currentMsg)
		newMessage.Attributes.ReceiveCount = 0
		newMessage.Attributes.ReRouted = true
		newMessage.Attributes.ReRoutedFromQueue = currentMsg.Channel
		newMessage.Channel = channel
		result := p.reRouteFunc(context.Background(), newMessage)
		if result.IsError {
			return fmt.Errorf("re-queue message with seq %d failed, %s", seq, result.Error)
		}
		err = msg.Ack()
		if err != nil {
			return err
		}
		delete(p.unAckedRawMessages, seq)
	}
	if len(p.unAckedRawMessages) == 0 {
		p.close()
	}
	return nil
}
func (p *QueueDownstreamResponse) ActiveOffsets() ([]int64, error) {
	if p.sessionDone.Load() {
		return nil, fmt.Errorf("cannot get active offsets with closed response ")
	}
	p.Lock()
	defer p.Unlock()
	var list []int64
	for seq := range p.unAckedRawMessages {
		list = append(list, seq)
	}
	return list, nil
}
func (p *QueueDownstreamResponse) processRawMessages(policyFunc func(msg *pb.QueueMessage) bool) {
	p.Lock()
	defer p.Unlock()
	var removeBadMessages []*stan.Msg
	for _, seq := range p.unAckedList {
		rawMsg, ok := p.unAckedRawMessages[seq]
		if ok {
			msg, err := unmarshalToQueueMessage(rawMsg.Data, rawMsg.Timestamp, rawMsg.Sequence)
			if err != nil {
				removeBadMessages = append(removeBadMessages, rawMsg)
				continue
			}
			if isExpired(msg) {
				removeBadMessages = append(removeBadMessages, rawMsg)
				metrics.ReportExpired(msg.Channel, 1)
				continue
			}
			msg = addReceiveCount(msg)
			if !policyFunc(msg) {
				removeBadMessages = append(removeBadMessages, rawMsg)
				continue
			}

			p.Messages = append(p.Messages, msg)
		}
	}

	for _, badMsg := range removeBadMessages {
		_ = badMsg.Ack()
		delete(p.unAckedRawMessages, int64(badMsg.Sequence))
	}
}
func (p *QueueDownstreamResponse) IsActive() bool {
	return !p.sessionDone.Load()
}
func (p *QueueDownstreamResponse) close() {
	if p.sessionDone.Load() {
		return
	}
	p.sessionDone.Store(true)
	p.subscriptionWait <- struct{}{}
	_ = p.subscription.Close()

}
func (p *QueueDownstreamResponse) Close() {
	p.Lock()
	defer p.Unlock()
	p.close()
}
