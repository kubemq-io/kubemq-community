package client

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/fortytw2/leaktest"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/nats-io/nuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"sync"
	"testing"
	"time"
)

func randStringBytes(n int) []byte {
	blk := make([]byte, n)
	_, err := rand.Read(blk)
	if err != nil {
		panic(err)
	}
	return blk
}

type testQueueClient struct {
	c        *QueueClient
	reqCh    chan *pb.StreamQueueMessagesRequest
	resCh    chan *pb.StreamQueueMessagesResponse
	doneCh   chan bool
	clientID string
}

func newTestQueueClient(opts *Options, policyCfg *config.QueueConfig) (*testQueueClient, error) {
	var err error
	tqc := &testQueueClient{
		reqCh:    make(chan *pb.StreamQueueMessagesRequest, 1),
		resCh:    make(chan *pb.StreamQueueMessagesResponse, 1),
		doneCh:   make(chan bool, 1),
		clientID: nuid.Next(),
	}

	tqc.c, err = NewQueueClient(opts, policyCfg)
	if err != nil {
		return nil, err
	}
	return tqc, nil
}
func (tqc *testQueueClient) disconnect() {
	_ = tqc.c.Disconnect()
}

func (tqc *testQueueClient) getQueueMessage(id int, queue string, size int, exp, delay, maxCnt int32, maxQueue string) *pb.QueueMessage {
	return &pb.QueueMessage{
		MessageID:  fmt.Sprintf("%d", id),
		ClientID:   tqc.clientID,
		Channel:    queue,
		Metadata:   "",
		Body:       randStringBytes(size),
		Tags:       nil,
		Attributes: nil,
		Policy: &pb.QueueMessagePolicy{
			ExpirationSeconds: exp,
			DelaySeconds:      delay,
			MaxReceiveCount:   maxCnt,
			MaxReceiveQueue:   maxQueue,
		},
	}
}

func (tqc *testQueueClient) sendMessagesWithPolicy(ctx context.Context, n int, queue string, size int, exp, delay, maxCnt int32, maxQueue string) error {
	if n == 1 {
		msg := tqc.getQueueMessage(1, queue, size, exp, delay, maxCnt, maxQueue)
		res := tqc.c.SendQueueMessage(ctx, msg)
		if res.IsError {
			return errors.New(res.Error)
		}
		return nil
	}
	batchReq := &pb.QueueMessagesBatchRequest{}
	for i := 1; i <= n; i++ {
		msg := tqc.getQueueMessage(i, queue, size, exp, delay, maxCnt, maxQueue)
		batchReq.Messages = append(batchReq.Messages, msg)
	}
	res, err := tqc.c.SendQueueMessagesBatch(ctx, batchReq)
	if err != nil {
		return err
	}
	if res.HaveErrors {
		return errors.New("error on sending batch messages")
	}
	return nil
}

func (tqc *testQueueClient) streamReceiveMessage(ctx context.Context, queue string, visibility, wait int32) (*pb.QueueMessage, bool, error) {
	req := &pb.StreamQueueMessagesRequest{
		RequestID:             nuid.Next(),
		ClientID:              tqc.clientID,
		StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
		Channel:               queue,
		VisibilitySeconds:     visibility,
		WaitTimeSeconds:       wait,
		RefSequence:           0,
		ModifiedMessage:       nil,
	}
	go tqc.c.StreamQueueMessage(ctx, tqc.reqCh, tqc.resCh, tqc.doneCh)
	tqc.reqCh <- req
	select {
	case res := <-tqc.resCh:
		if res.RequestID != req.RequestID {
			return nil, false, errors.New("invalid requestID, should be equal to req original id")
		}
		if res.StreamRequestTypeData != pb.StreamRequestType_ReceiveMessage {
			return nil, false, errors.New("invalid response type, should be ReceiveMessage")
		}
		if res.IsError {
			if res.Error == "Error 138: no new message in queue, wait time expired" {
				return nil, true, nil
			}
			return nil, false, fmt.Errorf("got error %s, should not get an error", res.Error)
		}
		if res.Message == nil {
			return nil, false, errors.New("invalid response data, should get a message from queue but got nil")
		}
		return res.Message, false, nil
	//case <-tqc.doneCh:
	//	return nil, false, errors.New("stream done, should get a response with message")
	case <-time.After(time.Duration(int(wait)+1) * time.Second):
		return nil, false, errors.New("time out, no response received")
	case <-ctx.Done():
		return nil, false, errors.New("context done, should get a response with message")
	}
}
func (tqc *testQueueClient) streamAckMessage(ctx context.Context, seq uint64) error {
	req := &pb.StreamQueueMessagesRequest{
		RequestID:             nuid.Next(),
		ClientID:              tqc.clientID,
		StreamRequestTypeData: pb.StreamRequestType_AckMessage,
		Channel:               "",
		VisibilitySeconds:     0,
		WaitTimeSeconds:       0,
		RefSequence:           seq,
		ModifiedMessage:       nil,
	}
	tqc.reqCh <- req
	select {
	case res := <-tqc.resCh:
		if res.RequestID != req.RequestID {
			return errors.New("invalid requestID, should be equal to req original id")
		}
		if res.StreamRequestTypeData != pb.StreamRequestType_AckMessage {
			return errors.New("invalid response type, should be AckMessage")
		}
		if res.IsError {
			return fmt.Errorf("got error %s, should not get an error", res.Error)
		}
		if res.Message != nil {
			return errors.New("invalid response, got message, should be nil")
		}
		return nil
	//case <-tqc.doneCh:
	//	return errors.New("stream done, should get a response of ack message")
	case <-time.After(10 * time.Second):
		return errors.New("time out, no response received")
	case <-ctx.Done():
		return errors.New("context done, should get a response of ack message")
	}
}
func (tqc *testQueueClient) streamRejectMessage(ctx context.Context, seq uint64) error {
	req := &pb.StreamQueueMessagesRequest{
		RequestID:             nuid.Next(),
		ClientID:              tqc.clientID,
		StreamRequestTypeData: pb.StreamRequestType_RejectMessage,
		Channel:               "",
		VisibilitySeconds:     0,
		WaitTimeSeconds:       0,
		RefSequence:           seq,
		ModifiedMessage:       nil,
	}
	tqc.reqCh <- req
	select {
	case res := <-tqc.resCh:
		if res.RequestID != req.RequestID {
			return errors.New("invalid requestID, should be equal to req original id")
		}
		if res.StreamRequestTypeData != pb.StreamRequestType_RejectMessage {
			return errors.New("invalid response type, should be RejectMessage")
		}
		if res.IsError {
			return fmt.Errorf("got error %s, should not get an error", res.Error)
		}
		if res.Message != nil {
			return errors.New("invalid response, got message, should be nil")
		}
		return nil
	//case <-tqc.doneCh:
	//	return errors.New("stream done, should get a response of reject message")
	case <-time.After(10 * time.Second):
		return errors.New("time out, no response received")
	case <-ctx.Done():
		return errors.New("context done, should get a response of reject message")
	}

}

func (tqc *testQueueClient) streamModifyVisibilityMessage(ctx context.Context, visibility int32) error {
	req := &pb.StreamQueueMessagesRequest{
		RequestID:             nuid.Next(),
		ClientID:              tqc.clientID,
		StreamRequestTypeData: pb.StreamRequestType_ModifyVisibility,
		Channel:               "",
		VisibilitySeconds:     visibility,
		WaitTimeSeconds:       0,
		RefSequence:           0,
		ModifiedMessage:       nil,
	}
	tqc.reqCh <- req
	select {
	case res := <-tqc.resCh:
		if res.RequestID != req.RequestID {
			return errors.New("invalid requestID, should be equal to req original id")
		}
		if res.StreamRequestTypeData != pb.StreamRequestType_ModifyVisibility {
			return errors.New("invalid response type, should be ModifyVisibility")
		}
		if res.IsError {
			return fmt.Errorf("got error %s, should not get an error", res.Error)
		}
		if res.Message != nil {
			return errors.New("invalid response, got message, should be nil")
		}
		return nil
	//case <-tqc.doneCh:
	//	return errors.New("stream done, should get a response of modify visibility message")
	case <-time.After(10 * time.Second):
		return errors.New("time out, no response received")
	case <-ctx.Done():
		return errors.New("context done, should get a response of modify visibility message")
	}

}

func (tqc *testQueueClient) streamModifyResendMessage(ctx context.Context, seq uint64, queue string) error {
	req := &pb.StreamQueueMessagesRequest{
		RequestID:             nuid.Next(),
		ClientID:              tqc.clientID,
		StreamRequestTypeData: pb.StreamRequestType_ResendMessage,
		Channel:               queue,
		VisibilitySeconds:     0,
		WaitTimeSeconds:       0,
		RefSequence:           seq,
		ModifiedMessage:       nil,
	}
	tqc.reqCh <- req
	select {
	case res := <-tqc.resCh:
		if res.RequestID != req.RequestID {
			return errors.New("invalid requestID, should be equal to req original id")
		}
		if res.StreamRequestTypeData != pb.StreamRequestType_ResendMessage {
			return errors.New("invalid response type, should be ResendMessage")
		}
		if res.IsError {
			return fmt.Errorf("got error %s, should not get an error", res.Error)
		}
		if res.Message != nil {
			return errors.New("invalid response, got message, should be nil")
		}
		return nil
	//case <-tqc.doneCh:
	//	return errors.New("stream done, should get a response of resend message")
	case <-time.After(10 * time.Second):
		return errors.New("time out, no response received")
	case <-ctx.Done():
		return errors.New("context done, should get a response of resend message")
	}
}

func (tqc *testQueueClient) streamModifyMessage(ctx context.Context, newMessage *pb.QueueMessage) error {
	req := &pb.StreamQueueMessagesRequest{
		RequestID:             nuid.Next(),
		ClientID:              tqc.clientID,
		StreamRequestTypeData: pb.StreamRequestType_SendModifiedMessage,
		Channel:               "",
		VisibilitySeconds:     0,
		WaitTimeSeconds:       0,
		RefSequence:           0,
		ModifiedMessage:       newMessage,
	}
	tqc.reqCh <- req
	select {
	case res := <-tqc.resCh:
		if res.RequestID != req.RequestID {
			return errors.New("invalid requestID, should be equal to req original id")
		}
		if res.StreamRequestTypeData != pb.StreamRequestType_SendModifiedMessage {
			return errors.New("invalid response type, should be SendModifiedMessage")
		}
		if res.IsError {
			return fmt.Errorf("got error %s, should not get an error", res.Error)
		}
		if res.Message != nil {
			return errors.New("invalid response, got message, should be nil")
		}
		return nil
	//case <-tqc.doneCh:
	//	return errors.New("stream done, should get a response of modify message")
	case <-time.After(10 * time.Second):
		return errors.New("time out, no response received")
	case <-ctx.Done():
		return errors.New("context done, should get a response of modify message")
	}
}

func (tqc *testQueueClient) streamGetDone(ctx context.Context) error {
	select {
	case <-tqc.resCh:
		return errors.New("got response instead of done signal ")

	case <-tqc.doneCh:
		return nil
	case <-time.After(10 * time.Second):
		return errors.New("time out, no done signal received")
	case <-ctx.Done():
		return errors.New("context done, should get a done signal")
	}
}

func (tqc *testQueueClient) verifyQueueItems(ctx context.Context, n int32, queue string, withSeqVerification bool) error {
	req := &pb.ReceiveQueueMessagesRequest{
		RequestID:           nuid.Next(),
		ClientID:            tqc.clientID,
		Channel:             queue,
		MaxNumberOfMessages: n,
		WaitTimeSeconds:     30,
		IsPeak:              false,
	}
	res, err := tqc.c.ReceiveQueueMessages(ctx, req)
	if err != nil {
		return err
	}
	if res.RequestID != req.RequestID {
		return errors.New("invalid requestID, should be equal to req original id")
	}
	if res.IsError {
		return errors.New("got error, should not get an error")
	}
	if res.MessagesReceived != n {
		return fmt.Errorf("didn't get the required amount of items, want %d, got %d", n, res.MessagesReceived)
	}
	if withSeqVerification {
		for i := 1; i <= int(n); i++ {
			if res.Messages[i-1].MessageID != fmt.Sprintf("%d", i) {
				return fmt.Errorf("didn't get the required message id seq, want %d, got %s", i, res.Messages[i-1].MessageID)
			}

			if i == 1 {
				continue
			}
			currentSeq := res.Messages[i-1].Attributes.Sequence
			prevSeq := res.Messages[i-2].Attributes.Sequence
			seqGap := currentSeq - prevSeq
			if seqGap != 1 {
				return fmt.Errorf("sequence verification failed, gap: %d, current seq: %d, previues seq: %d", seqGap, currentSeq, prevSeq)
			}
		}

	}

	return nil
}

func getQueueMessage(queue string, body []byte) *pb.QueueMessage {
	return &pb.QueueMessage{
		MessageID:  nuid.Next(),
		ClientID:   nuid.Next(),
		Channel:    queue,
		Metadata:   "",
		Body:       body,
		Tags:       nil,
		Attributes: nil,
		Policy: &pb.QueueMessagePolicy{
			ExpirationSeconds: 0,
			DelaySeconds:      0,
			MaxReceiveCount:   0,
			MaxReceiveQueue:   "",
		},
	}
}

func getReceiveMessageRequest(queueName string, visibility int32) *pb.StreamQueueMessagesRequest {
	return &pb.StreamQueueMessagesRequest{
		RequestID:             nuid.Next(),
		ClientID:              nuid.Next(),
		StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
		Channel:               queueName,
		VisibilitySeconds:     visibility,
		WaitTimeSeconds:       60,
		RefSequence:           0,
		ModifiedMessage:       nil,
	}

}

func getAckMessageRequest(seq uint64) *pb.StreamQueueMessagesRequest {
	return &pb.StreamQueueMessagesRequest{
		RequestID:             nuid.Next(),
		ClientID:              nuid.Next(),
		StreamRequestTypeData: pb.StreamRequestType_AckMessage,
		Channel:               "",
		VisibilitySeconds:     0,
		WaitTimeSeconds:       60,
		RefSequence:           seq,
		ModifiedMessage:       nil,
	}

}

func getRejectMessageRequest(seq uint64) *pb.StreamQueueMessagesRequest {
	return &pb.StreamQueueMessagesRequest{
		RequestID:             nuid.Next(),
		ClientID:              nuid.Next(),
		StreamRequestTypeData: pb.StreamRequestType_RejectMessage,
		Channel:               "",
		VisibilitySeconds:     0,
		WaitTimeSeconds:       60,
		RefSequence:           seq,
		ModifiedMessage:       nil,
	}

}

func getModifyVisibilityRequest(visibility int32) *pb.StreamQueueMessagesRequest {
	return &pb.StreamQueueMessagesRequest{
		RequestID:             nuid.Next(),
		ClientID:              nuid.Next(),
		StreamRequestTypeData: pb.StreamRequestType_ModifyVisibility,
		Channel:               "",
		VisibilitySeconds:     visibility,
		RefSequence:           0,
		WaitTimeSeconds:       60,
		ModifiedMessage:       nil,
	}
}

func getModifyQueueMessage(newMsg *pb.QueueMessage) *pb.StreamQueueMessagesRequest {
	return &pb.StreamQueueMessagesRequest{
		RequestID:             nuid.Next(),
		ClientID:              nuid.Next(),
		StreamRequestTypeData: pb.StreamRequestType_SendModifiedMessage,
		Channel:               "",
		VisibilitySeconds:     0,
		WaitTimeSeconds:       60,
		RefSequence:           0,
		ModifiedMessage:       newMsg,
	}
}

func getResendQueueMessage(queue string) *pb.StreamQueueMessagesRequest {
	return &pb.StreamQueueMessagesRequest{
		RequestID:             nuid.Next(),
		ClientID:              nuid.Next(),
		StreamRequestTypeData: pb.StreamRequestType_ResendMessage,
		Channel:               queue,
		VisibilitySeconds:     0,
		WaitTimeSeconds:       60,
		RefSequence:           0,
		ModifiedMessage:       nil,
	}
}

func TestQueueClient_SendReceiveMessageQueue(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	require.NoError(t, err)
	defer func() {
		_ = c.Disconnect()
	}()
	require.NoError(t, err)
	require.True(t, c.isUp.Load())

	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	messagesList := &pb.QueueMessagesBatchRequest{
		BatchID:  nuid.Next(),
		Messages: []*pb.QueueMessage{},
	}
	msgs := 1000
	for i := 0; i < msgs; i++ {
		messagesList.Messages = append(messagesList.Messages, &pb.QueueMessage{
			MessageID: fmt.Sprintf("%d", i),
			ClientID:  "some-client-id",
			Channel:   "some-queue-channel",
			Metadata:  "",
			Body:      []byte(fmt.Sprintf("msg #%d", i)),
		})
	}
	sendResponse, err := c.SendQueueMessagesBatch(ctx, messagesList)
	require.NoError(t, err)

	require.False(t, sendResponse.HaveErrors)

	c2, err := NewQueueClient(getClientOptions(t, a), a.Queue)
	require.NoError(t, err)
	defer func() {
		_ = c2.Disconnect()
	}()
	response, err := c2.ReceiveQueueMessages(ctx,
		&pb.ReceiveQueueMessagesRequest{
			ClientID:             "some-receiving-clientid",
			Channel:              "some-queue-channel",
			MaxNumberOfMessages:  int32(msgs),
			WaitTimeSeconds:      60,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		})

	require.NoError(t, err)
	require.Equal(t, 1000, len(response.Messages))
	for i, message := range response.Messages {
		require.Equal(t, i+1, int(message.Attributes.Sequence))
		require.Equal(t, fmt.Sprintf("%d", i), message.MessageID)

	}

}
func TestQueueClient_SingleSenderSingleReceiverWaitTimeSecondLongPoll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	messagesList := &pb.QueueMessagesBatchRequest{
		BatchID:  nuid.Next(),
		Messages: []*pb.QueueMessage{},
	}

	for i := 0; i < 10; i++ {
		messagesList.Messages = append(messagesList.Messages, &pb.QueueMessage{
			MessageID: nuid.Next(),
			ClientID:  "some-client-id",
			Channel:   "some-queue-channel",
			Metadata:  "",
			Body:      []byte(fmt.Sprintf("msg #%d", i)),
		})
	}
	sendResponse, err := c.SendQueueMessagesBatch(ctx, messagesList)
	require.NoError(t, err)
	require.False(t, sendResponse.HaveErrors)
	_ = c.Disconnect()

	c2, err := NewQueueClient(getClientOptions(t, a), a.Queue)
	require.NoError(t, err)
	defer func() {
		_ = c2.Disconnect()
	}()
	response, err := c2.ReceiveQueueMessages(ctx,
		&pb.ReceiveQueueMessagesRequest{
			ClientID:             "some-receiving-clientid",
			Channel:              "some-queue-channel",
			MaxNumberOfMessages:  20,
			WaitTimeSeconds:      1,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		})

	require.NoError(t, err)
	require.Equal(t, 10, len(response.Messages))

}
func TestQueueClient_SendReceiveMessageQueueWithExpiration(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()
	require.NoError(t, err)
	require.True(t, c.isUp.Load())

	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	queue := "some-queue"
	sendMsg := getQueueMessage(queue, []byte("1"))
	sendMsg.Policy.ExpirationSeconds = 10
	sendResponse := c.SendQueueMessage(ctx, sendMsg)
	require.False(t, sendResponse.IsError)
	require.NotZero(t, sendResponse.ExpirationAt)
	require.NotZero(t, sendResponse.SentAt)
	require.True(t, sendResponse.ExpirationAt > sendResponse.SentAt)

	time.Sleep(time.Second)
	c2, _ := NewQueueClient(getClientOptions(t, a), a.Queue)
	defer func() {
		_ = c2.Disconnect()
	}()
	rxReq := &pb.ReceiveQueueMessagesRequest{
		RequestID:            nuid.Next(),
		ClientID:             nuid.Next(),
		Channel:              queue,
		MaxNumberOfMessages:  1,
		WaitTimeSeconds:      1,
		IsPeak:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	response, err := c2.ReceiveQueueMessages(ctx, rxReq)
	require.NoError(t, err)
	require.Equal(t, 1, len(response.Messages))
	require.Equal(t, rxReq.RequestID, response.RequestID)
	require.Equal(t, int32(1), response.MessagesReceived)
	require.Equal(t, int32(0), response.MessagesExpired)

	rxMsg := response.Messages[0]

	require.Equal(t, sendMsg.MessageID, rxMsg.MessageID)
	require.Equal(t, sendMsg.Policy.ExpirationSeconds, rxMsg.Policy.ExpirationSeconds)
	require.Equal(t, sendResponse.ExpirationAt, rxMsg.Attributes.ExpirationAt)
	require.Equal(t, int32(1), rxMsg.Attributes.ReceiveCount)
	require.NotZero(t, rxMsg.Attributes.Sequence)
	require.NotZero(t, rxMsg.Attributes.Timestamp)

	sendMsg = getQueueMessage(queue, []byte("2"))
	sendMsg.Policy.ExpirationSeconds = 1
	sendResponse = c.SendQueueMessage(ctx, sendMsg)
	require.False(t, sendResponse.IsError)
	require.NotZero(t, sendResponse.ExpirationAt)
	require.NotZero(t, sendResponse.SentAt)
	require.True(t, sendResponse.ExpirationAt > sendResponse.SentAt)

	time.Sleep(2 * time.Second)
	rxReq = &pb.ReceiveQueueMessagesRequest{
		RequestID:            nuid.Next(),
		ClientID:             nuid.Next(),
		Channel:              queue,
		MaxNumberOfMessages:  1,
		WaitTimeSeconds:      1,
		IsPeak:               true,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	response, err = c2.ReceiveQueueMessages(ctx, rxReq)
	require.NoError(t, err)
	require.Equal(t, 0, len(response.Messages))
	require.Equal(t, rxReq.RequestID, response.RequestID)
	require.Equal(t, int32(0), response.MessagesReceived)
	require.Equal(t, int32(1), response.MessagesExpired)

	time.Sleep(1 * time.Second)
	rxReq = &pb.ReceiveQueueMessagesRequest{
		RequestID:            nuid.Next(),
		ClientID:             nuid.Next(),
		Channel:              queue,
		MaxNumberOfMessages:  1,
		WaitTimeSeconds:      1,
		IsPeak:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	response, err = c2.ReceiveQueueMessages(ctx, rxReq)
	require.NoError(t, err)
	require.Equal(t, 0, len(response.Messages))
	require.Equal(t, rxReq.RequestID, response.RequestID)
	require.Equal(t, int32(0), response.MessagesReceived)
	require.Equal(t, int32(0), response.MessagesExpired)

}
func TestQueueClient_SendReceiveMessageQueueWithDelay(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	require.NoError(t, err)
	defer func() {
		_ = c.Disconnect()
	}()
	require.True(t, c.isUp.Load())
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	queue := "some-queue"
	sendMsg := getQueueMessage(queue, []byte("1"))
	sendMsg.Policy.DelaySeconds = 5
	sendResponse := c.SendQueueMessage(ctx, sendMsg)
	require.False(t, sendResponse.IsError)
	require.NotZero(t, sendResponse.DelayedTo)
	require.NotZero(t, sendResponse.SentAt)
	require.True(t, sendResponse.DelayedTo > sendResponse.SentAt)

	c2, err := NewQueueClient(getClientOptions(t, a), a.Queue)
	require.NoError(t, err)
	c2.SetDelayMessagesProcessor(ctx)
	defer func() {
		_ = c2.Disconnect()
	}()
	rxReq := &pb.ReceiveQueueMessagesRequest{
		RequestID:            nuid.Next(),
		ClientID:             nuid.Next(),
		Channel:              queue,
		MaxNumberOfMessages:  1,
		WaitTimeSeconds:      4,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	response, err := c2.ReceiveQueueMessages(ctx, rxReq)
	require.NoError(t, err)
	require.False(t, response.IsError)
	require.Equal(t, 0, len(response.Messages))
	require.Equal(t, rxReq.RequestID, response.RequestID)
	require.Equal(t, int32(0), response.MessagesReceived)
	require.Equal(t, int32(0), response.MessagesExpired)

	time.Sleep(2 * time.Second)
	rxReq = &pb.ReceiveQueueMessagesRequest{
		RequestID:            nuid.Next(),
		ClientID:             nuid.Next(),
		Channel:              queue,
		MaxNumberOfMessages:  10,
		WaitTimeSeconds:      1,
		IsPeak:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	response, err = c2.ReceiveQueueMessages(ctx, rxReq)
	require.NoError(t, err)
	require.False(t, response.IsError)
	require.Equal(t, 1, len(response.Messages))
	assert.Equal(t, rxReq.RequestID, response.RequestID)
	assert.Equal(t, int32(1), response.MessagesReceived)
	assert.Equal(t, int32(0), response.MessagesExpired)

	rxMsg := response.Messages[0]

	assert.Equal(t, sendMsg.MessageID, rxMsg.MessageID)
	assert.Equal(t, int32(0), rxMsg.Policy.DelaySeconds)
	assert.Equal(t, int64(0), rxMsg.Attributes.DelayedTo)
	assert.Equal(t, int32(1), rxMsg.Attributes.ReceiveCount)

	assert.NotZero(t, rxMsg.Attributes.Sequence)
	assert.NotZero(t, rxMsg.Attributes.Timestamp)

}
func TestQueueClient_SendReceiveMessageQueueWithDelayAndExpiration(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()

	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()
	require.NoError(t, err)
	require.True(t, c.isUp.Load())

	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	queue := "some-queue"
	sendMsg := getQueueMessage(queue, []byte("1"))
	sendMsg.Policy.DelaySeconds = 3
	sendMsg.Policy.ExpirationSeconds = 1
	sendResponse := c.SendQueueMessage(ctx, sendMsg)
	require.False(t, sendResponse.IsError)
	require.NotZero(t, sendResponse.DelayedTo)
	require.NotZero(t, sendResponse.ExpirationAt)
	require.NotZero(t, sendResponse.SentAt)
	require.True(t, sendResponse.DelayedTo > sendResponse.SentAt)
	require.True(t, sendResponse.ExpirationAt > sendResponse.DelayedTo)

	c2, err := NewQueueClient(getClientOptions(t, a), a.Queue)
	require.NoError(t, err)
	c2.SetDelayMessagesProcessor(ctx)
	defer func() {
		_ = c2.Disconnect()
	}()
	rxReq := &pb.ReceiveQueueMessagesRequest{
		RequestID:            nuid.Next(),
		ClientID:             nuid.Next(),
		Channel:              queue,
		MaxNumberOfMessages:  1,
		WaitTimeSeconds:      1,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	response, err := c2.ReceiveQueueMessages(ctx, rxReq)
	require.NoError(t, err)
	require.False(t, response.IsError)
	require.Equal(t, 0, len(response.Messages))
	require.Equal(t, rxReq.RequestID, response.RequestID)
	require.Equal(t, int32(0), response.MessagesReceived)
	require.Equal(t, int32(0), response.MessagesExpired)

	time.Sleep(5 * time.Second)
	rxReq = &pb.ReceiveQueueMessagesRequest{
		RequestID:            nuid.Next(),
		ClientID:             nuid.Next(),
		Channel:              queue,
		MaxNumberOfMessages:  10,
		WaitTimeSeconds:      2,
		IsPeak:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	response, err = c2.ReceiveQueueMessages(ctx, rxReq)
	require.NoError(t, err)
	require.False(t, response.IsError)
	require.Equal(t, 0, len(response.Messages))
	assert.Equal(t, int32(0), response.MessagesReceived)
	assert.Equal(t, int32(1), response.MessagesExpired)

}
func TestQueueClient_SendReceiveStreamMessageQueueWithReRoute(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()

	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()
	require.NoError(t, err)
	require.True(t, c.isUp.Load())

	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	queue := "some-queue"
	queueReRoute := "some-reroute-queue"

	sendMsg := getQueueMessage(queue, []byte("1"))
	sendMsg.Policy.ExpirationSeconds = 10
	sendMsg.Policy.MaxReceiveCount = 2
	sendMsg.Policy.MaxReceiveQueue = queueReRoute
	sendResponse := c.SendQueueMessage(ctx, sendMsg)
	require.False(t, sendResponse.IsError)
	require.NotZero(t, sendResponse.SentAt)

	go func() {
		c2, err := NewQueueClient(getClientOptions(t, a), a.Queue)
		if err != nil {
			return
		}
		defer func() {
			_ = c2.Disconnect()
		}()
		for {
			reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
			resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
			done := make(chan bool, 1)
			newCtx, cancel := context.WithCancel(ctx)
			go c2.StreamQueueMessage(newCtx, reqCh, resCh, done)
			require.NoError(t, err)
			newReq := getReceiveMessageRequest(sendMsg.Channel, 10)
			reqCh <- newReq
			select {

			case <-resCh:

			case <-ctx.Done():
				cancel()
				return
			}
			time.Sleep(time.Second)
			cancel()
		}
	}()
	time.Sleep(4 * time.Second)
	rxReq := &pb.ReceiveQueueMessagesRequest{
		RequestID:           nuid.Next(),
		ClientID:            nuid.Next(),
		Channel:             queueReRoute,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
	}
	response, err := c.ReceiveQueueMessages(ctx, rxReq)
	require.NoError(t, err)
	require.Equal(t, 1, len(response.Messages))
	assert.Equal(t, rxReq.RequestID, response.RequestID)
	assert.Equal(t, int32(1), response.MessagesReceived)
	assert.Equal(t, int32(0), response.MessagesExpired)
	rxMsg := response.Messages[0]
	assert.True(t, rxMsg.Attributes.ReRouted)
	assert.Equal(t, queue, rxMsg.Attributes.ReRoutedFromQueue)
	assert.Equal(t, int32(1), rxMsg.Attributes.ReceiveCount)
	assert.Equal(t, int32(0), rxMsg.Policy.MaxReceiveCount)
	assert.Equal(t, "", rxMsg.Policy.MaxReceiveQueue)
	assert.Equal(t, sendResponse.ExpirationAt, rxMsg.Attributes.ExpirationAt)
	assert.Equal(t, uint64(1), rxMsg.Attributes.Sequence)

}
func TestQueueClient_SendReceiveStreamMessageQueueWithReRouteWithExpiration(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()

	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()

	require.NoError(t, err)
	require.True(t, c.isUp.Load())

	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	queue := "some-queue"
	queueReRoute := "some-reroute-queue"

	sendMsg := getQueueMessage(queue, []byte("1"))
	sendMsg.Policy.ExpirationSeconds = 1
	sendMsg.Policy.MaxReceiveCount = 2
	sendMsg.Policy.MaxReceiveQueue = queueReRoute
	sendResponse := c.SendQueueMessage(ctx, sendMsg)
	require.False(t, sendResponse.IsError)
	require.NotZero(t, sendResponse.SentAt)

	go func() {
		c2, err := NewQueueClient(getClientOptions(t, a), a.Queue)
		if err != nil {
			return
		}
		defer func() {
			_ = c2.Disconnect()
		}()
		for {

			reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
			resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
			done := make(chan bool, 2)
			go c2.StreamQueueMessage(ctx, reqCh, resCh, done)
			require.NoError(t, err)
			newReq := getReceiveMessageRequest(sendMsg.Channel, 2)
			select {
			case reqCh <- newReq:
			case <-resCh:

			case <-ctx.Done():

				return
			}
			time.Sleep(1 * time.Second)
			select {
			case <-resCh:

			case <-done:

			case <-ctx.Done():
				_ = c2.Disconnect()
				return
			}
			_ = c2.Disconnect()
		}
	}()
	time.Sleep(3 * time.Second)
	rxReq := &pb.ReceiveQueueMessagesRequest{
		RequestID:           nuid.Next(),
		ClientID:            nuid.Next(),
		Channel:             queueReRoute,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
	}
	response, err := c.ReceiveQueueMessages(ctx, rxReq)
	require.NoError(t, err)
	require.Equal(t, 0, len(response.Messages))
	assert.Equal(t, rxReq.RequestID, response.RequestID)
	assert.Equal(t, int32(0), response.MessagesReceived)

}
func TestQueueClient_SingleSenderMultiReceivers(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()

	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	require.NoError(t, err)
	defer func() {
		_ = c.Disconnect()
	}()
	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	counter := atomic.NewInt64(0)
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	//
	messagesList := &pb.QueueMessagesBatchRequest{
		BatchID:  nuid.Next(),
		Messages: []*pb.QueueMessage{},
	}

	for i := 0; i < 10; i++ {
		messagesList.Messages = append(messagesList.Messages, &pb.QueueMessage{
			MessageID: nuid.Next(),
			ClientID:  "some-client-id",
			Channel:   "some-queue-channel",
			Metadata:  "",
			Body:      []byte(fmt.Sprintf("msg #%d", i)),
		})
	}
	response, err := c.SendQueueMessagesBatch(ctx, messagesList)
	require.NoError(t, err)

	require.False(t, response.HaveErrors)

	go func() {
		c2, err := NewQueueClient(getClientOptions(t, a), a.Queue)
		require.NoError(t, err)
		defer func() {
			_ = c2.Disconnect()
		}()
		response, err := c2.ReceiveQueueMessages(ctx,
			&pb.ReceiveQueueMessagesRequest{
				ClientID:             "some-receiving-clientid1",
				Channel:              "some-queue-channel",
				MaxNumberOfMessages:  5,
				WaitTimeSeconds:      20,
				XXX_NoUnkeyedLiteral: struct{}{},
				XXX_sizecache:        0,
			})

		require.NoError(t, err)
		require.NotEmpty(t, len(response.Messages))
		counter.Add(int64(len(response.Messages)))

	}()
	go func() {
		c2, err := NewQueueClient(getClientOptions(t, a), a.Queue)
		require.NoError(t, err)
		defer func() {
			_ = c2.Disconnect()
		}()
		response, err := c2.ReceiveQueueMessages(ctx,
			&pb.ReceiveQueueMessagesRequest{
				ClientID:            "some-receiving-clientid2",
				Channel:             "some-queue-channel",
				MaxNumberOfMessages: 5,
				WaitTimeSeconds:     20,
			})
		require.NoError(t, err)
		require.NotEmpty(t, len(response.Messages))
		counter.Add(int64(len(response.Messages)))

	}()

	time.Sleep(3 * time.Second)
	require.Equal(t, int64(10), counter.Load())
}
func TestQueueClient_SingleSenderMultiReceiversSingleMessage(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()

	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	require.NoError(t, err)
	defer func() {
		_ = c.Disconnect()
	}()
	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	counter := atomic.NewInt64(0)
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	//
	messagesList := &pb.QueueMessagesBatchRequest{
		BatchID:  nuid.Next(),
		Messages: []*pb.QueueMessage{},
	}

	for i := 0; i < 10; i++ {
		messagesList.Messages = append(messagesList.Messages, &pb.QueueMessage{
			MessageID: nuid.Next(),
			ClientID:  "some-client-id",
			Channel:   "some-queue-channel",
			Metadata:  "",
			Body:      []byte(fmt.Sprintf("msg #%d", i)),
		})
	}
	response, err := c.SendQueueMessagesBatch(ctx, messagesList)
	require.NoError(t, err)
	require.False(t, response.HaveErrors)
	time.Sleep(1 * time.Second)

	go func() {
		c2, err := NewQueueClient(getClientOptions(t, a), a.Queue)
		require.NoError(t, err)
		defer func() {
			_ = c2.Disconnect()
		}()
		for {

			select {
			case <-ctx.Done():
				return
			default:
				response, err := c2.ReceiveQueueMessages(ctx,
					&pb.ReceiveQueueMessagesRequest{
						ClientID:             "some-receiving-clientid1",
						Channel:              "some-queue-channel",
						MaxNumberOfMessages:  1,
						WaitTimeSeconds:      1,
						XXX_NoUnkeyedLiteral: struct{}{},
						XXX_sizecache:        0,
					})

				require.NoError(t, err)
				counter.Add(int64(response.MessagesReceived))

			}
		}
	}()
	go func() {
		c2, err := NewQueueClient(getClientOptions(t, a), a.Queue)
		require.NoError(t, err)
		defer func() {
			_ = c2.Disconnect()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				response, err := c2.ReceiveQueueMessages(ctx,
					&pb.ReceiveQueueMessagesRequest{
						ClientID:             "some-receiving-clientid2",
						Channel:              "some-queue-channel",
						MaxNumberOfMessages:  1,
						WaitTimeSeconds:      1,
						XXX_NoUnkeyedLiteral: struct{}{},
						XXX_sizecache:        0,
					})

				require.NoError(t, err)
				counter.Add(int64(response.MessagesReceived))

			}
		}
	}()

	time.Sleep(1 * time.Second)
	require.Equal(t, int64(10), counter.Load())
}
func TestQueueClient_StreamQueueMessage_BaseFlow(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()

	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()

	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	sendMsg := &pb.QueueMessage{
		MessageID:  nuid.Next(),
		ClientID:   "some-client-id",
		Channel:    "some-queue-channel",
		Metadata:   "",
		Body:       []byte("1"),
		Attributes: nil,
		Policy:     nil,
	}
	response := c.SendQueueMessage(ctx, sendMsg)
	require.False(t, response.IsError)
	time.Sleep(time.Second)
	counter := atomic.NewInt64(0)
	c2, _ := NewQueueClient(getClientOptions(t, a), a.Queue)
	defer func() {
		_ = c2.Disconnect()
	}()
	reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
	resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
	done := make(chan bool, 1)

	go c2.StreamQueueMessage(ctx, reqCh, resCh, done)
	require.NoError(t, err)
	newReq := getReceiveMessageRequest(sendMsg.Channel, 10)
	reqCh <- newReq

	var rxRes *pb.StreamQueueMessagesResponse
	select {
	case rxRes = <-resCh:

	case <-time.After(1 * time.Second):
		t.Error("timeout receiving message")
	}
	require.NotNil(t, rxRes)
	assert.Equal(t, newReq.RequestID, rxRes.RequestID)
	assert.Equal(t, newReq.StreamRequestTypeData, rxRes.StreamRequestTypeData)
	assert.False(t, rxRes.IsError)
	assert.Empty(t, rxRes.Error)
	assert.NotNil(t, rxRes.Message)
	rxMsg := rxRes.Message
	assert.Equal(t, sendMsg.Body, rxMsg.Body)
	assert.NotZero(t, rxMsg.Attributes.Sequence)
	assert.NotZero(t, rxMsg.Attributes.Timestamp)
	assert.Equal(t, int32(1), rxMsg.Attributes.ReceiveCount)
	newReq = getAckMessageRequest(rxMsg.Attributes.Sequence)
	reqCh <- newReq
	select {
	case rxRes = <-resCh:
		require.NotNil(t, rxRes)
		assert.Equal(t, newReq.StreamRequestTypeData, rxRes.StreamRequestTypeData)
		assert.Equal(t, newReq.RequestID, rxRes.RequestID)
		assert.False(t, rxRes.IsError)
		assert.Empty(t, rxRes.Error)
		counter.Inc()
	case <-done:
		counter.Inc()
	case <-time.After(2 * time.Second):
		t.Error("timeout receiving message")
	}
	require.Equal(t, int64(1), counter.Load())
}
func TestQueueClient_StreamQueueMessage_BaseFlowWithIncreaseVisibility(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()

	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	sendMsg := &pb.QueueMessage{
		MessageID:  nuid.Next(),
		ClientID:   "some-client-id",
		Channel:    "some-queue-channel",
		Metadata:   "",
		Body:       []byte("1"),
		Attributes: nil,
		Policy:     nil,
	}
	response := c.SendQueueMessage(ctx, sendMsg)
	require.False(t, response.IsError)
	time.Sleep(time.Second)
	counter := atomic.NewInt64(0)
	c2, _ := NewQueueClient(getClientOptions(t, a), a.Queue)
	defer func() {
		_ = c2.Disconnect()
	}()
	reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
	resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
	done := make(chan bool, 1)

	go c2.StreamQueueMessage(ctx, reqCh, resCh, done)
	require.NoError(t, err)
	newReq := getReceiveMessageRequest(sendMsg.Channel, 5)
	reqCh <- newReq

	var rxRes *pb.StreamQueueMessagesResponse
	select {
	case rxRes = <-resCh:

	case <-time.After(1 * time.Second):
		t.Error("timeout receiving message")
	}
	require.NotNil(t, rxRes)
	assert.Equal(t, newReq.RequestID, rxRes.RequestID)
	assert.Equal(t, newReq.StreamRequestTypeData, rxRes.StreamRequestTypeData)
	assert.False(t, rxRes.IsError)
	assert.Empty(t, rxRes.Error)
	assert.NotNil(t, rxRes.Message)
	rxMsg := rxRes.Message
	assert.Equal(t, sendMsg.Body, rxMsg.Body)
	assert.NotZero(t, rxMsg.Attributes.Sequence)
	assert.NotZero(t, rxMsg.Attributes.Timestamp)
	assert.Equal(t, int32(1), rxMsg.Attributes.ReceiveCount)
	time.Sleep(3 * time.Second)

	newReq = getModifyVisibilityRequest(5)
	reqCh <- newReq
	select {
	case rxRes = <-resCh:
		require.NotNil(t, rxRes)
		assert.Equal(t, newReq.StreamRequestTypeData, rxRes.StreamRequestTypeData)
		assert.Equal(t, newReq.RequestID, rxRes.RequestID)
		assert.False(t, rxRes.IsError)
		assert.Empty(t, rxRes.Error)
	case <-done:
		counter.Inc()
	case <-time.After(2 * time.Second):
		t.Error("timeout receiving message")
	}
	require.Equal(t, int64(0), counter.Load())
	time.Sleep(3 * time.Second)
	newReq = getAckMessageRequest(rxMsg.Attributes.Sequence)
	reqCh <- newReq
	select {
	case rxRes = <-resCh:
		require.NotNil(t, rxRes)
		assert.Equal(t, newReq.StreamRequestTypeData, rxRes.StreamRequestTypeData)
		assert.Equal(t, newReq.RequestID, rxRes.RequestID)
		assert.False(t, rxRes.IsError)
		assert.Empty(t, rxRes.Error)
		counter.Inc()
	case <-done:
		counter.Inc()
	case <-time.After(2 * time.Second):
		t.Error("timeout receiving message")
	}
	require.Equal(t, int64(1), counter.Load())
}
func TestQueueClient_StreamQueueMessage_MultipleReceiveRequests(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()

	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	sendMsg := &pb.QueueMessage{
		MessageID:  nuid.Next(),
		ClientID:   "some-client-id",
		Channel:    "some-queue-channel",
		Metadata:   "",
		Body:       []byte("1"),
		Attributes: nil,
		Policy:     nil,
	}
	response := c.SendQueueMessage(ctx, sendMsg)
	require.False(t, response.IsError)
	time.Sleep(time.Second)
	counter := atomic.NewInt64(0)
	c2, _ := NewQueueClient(getClientOptions(t, a), a.Queue)
	defer func() {
		_ = c2.Disconnect()
	}()
	reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
	resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
	done := make(chan bool, 1)

	go c2.StreamQueueMessage(ctx, reqCh, resCh, done)
	require.NoError(t, err)
	newReq := getReceiveMessageRequest(sendMsg.Channel, 5)
	reqCh <- newReq

	var rxRes *pb.StreamQueueMessagesResponse
	select {
	case rxRes = <-resCh:

	case <-time.After(1 * time.Second):
		t.Error("timeout receiving message")
	}
	require.NotNil(t, rxRes)
	assert.Equal(t, newReq.RequestID, rxRes.RequestID)
	assert.False(t, rxRes.IsError)
	assert.Empty(t, rxRes.Error)
	require.NotNil(t, rxRes.Message)
	rxMsg := rxRes.Message
	assert.Equal(t, sendMsg.Body, rxMsg.Body)
	assert.NotZero(t, rxMsg.Attributes.Sequence)
	assert.NotZero(t, rxMsg.Attributes.Timestamp)
	assert.Equal(t, int32(1), rxMsg.Attributes.ReceiveCount)
	time.Sleep(1 * time.Second)
	newReq = getReceiveMessageRequest(sendMsg.Channel, 5)
	reqCh <- newReq
	select {
	case rxRes = <-resCh:
		require.NotNil(t, rxRes)
		assert.Equal(t, newReq.RequestID, rxRes.RequestID)
		assert.Equal(t, newReq.StreamRequestTypeData, rxRes.StreamRequestTypeData)
		assert.True(t, rxRes.IsError)
		assert.Equal(t, entities.ErrSubscriptionIsActive.Error(), rxRes.Error)
	case <-done:
		counter.Inc()
	case <-time.After(2 * time.Second):
		t.Error("timeout receiving message")
	}
	require.Equal(t, int64(0), counter.Load())
	newReq = getAckMessageRequest(rxMsg.Attributes.Sequence)
	reqCh <- newReq
	select {
	case rxRes = <-resCh:
		require.NotNil(t, rxRes)
		assert.Equal(t, newReq.RequestID, rxRes.RequestID)
		assert.Equal(t, newReq.StreamRequestTypeData, rxRes.StreamRequestTypeData)
		assert.False(t, rxRes.IsError)
		assert.Empty(t, rxRes.Error)
		counter.Inc()
	case <-done:
		counter.Inc()
	case <-time.After(2 * time.Second):
		t.Error("timeout receiving message")
	}
	require.Equal(t, int64(1), counter.Load())
}
func TestQueueClient_StreamQueueMessage_SendInvalidAck(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()

	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	sendMsg := &pb.QueueMessage{
		MessageID:  nuid.Next(),
		ClientID:   "some-client-id",
		Channel:    "some-queue-channel",
		Metadata:   "",
		Body:       []byte("1"),
		Attributes: nil,
		Policy:     nil,
	}
	response := c.SendQueueMessage(ctx, sendMsg)
	require.False(t, response.IsError)
	time.Sleep(time.Second)
	counter := atomic.NewInt64(0)
	c2, _ := NewQueueClient(getClientOptions(t, a), a.Queue)
	defer func() {
		_ = c2.Disconnect()
	}()
	reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
	resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
	done := make(chan bool, 1)
	var rxRes *pb.StreamQueueMessagesResponse
	go c2.StreamQueueMessage(ctx, reqCh, resCh, done)

	newReq := getAckMessageRequest(200)
	reqCh <- newReq
	select {
	case rxRes = <-resCh:
		require.NotNil(t, rxRes)
		assert.Equal(t, newReq.RequestID, rxRes.RequestID)
		assert.Equal(t, newReq.StreamRequestTypeData, rxRes.StreamRequestTypeData)
		assert.True(t, rxRes.IsError)
		assert.Equal(t, entities.ErrNoCurrentMsgToAck.Error(), rxRes.Error)
	case <-done:
		counter.Inc()
	case <-time.After(2 * time.Second):
		t.Error("timeout receiving message")
	}
	require.Equal(t, int64(0), counter.Load())

	newReq = getReceiveMessageRequest(sendMsg.Channel, 5)
	reqCh <- newReq

	select {
	case rxRes = <-resCh:

	case <-time.After(1 * time.Second):
		t.Error("timeout receiving message")
	}
	require.NotNil(t, rxRes)
	assert.Equal(t, newReq.RequestID, rxRes.RequestID)
	assert.False(t, rxRes.IsError)
	assert.Empty(t, rxRes.Error)
	require.NotNil(t, rxRes.Message)
	rxMsg := rxRes.Message
	assert.Equal(t, sendMsg.Body, rxMsg.Body)
	assert.NotZero(t, rxMsg.Attributes.Sequence)
	assert.NotZero(t, rxMsg.Attributes.Timestamp)
	assert.Equal(t, int32(1), rxMsg.Attributes.ReceiveCount)
	time.Sleep(1 * time.Second)
	newReq = getAckMessageRequest(200)
	reqCh <- newReq
	select {
	case rxRes = <-resCh:
		require.NotNil(t, rxRes)
		assert.Equal(t, newReq.RequestID, rxRes.RequestID)
		assert.Equal(t, newReq.StreamRequestTypeData, rxRes.StreamRequestTypeData)
		assert.True(t, rxRes.IsError)
		assert.Equal(t, entities.ErrInvalidAckSeq.Error(), rxRes.Error)
	case <-done:
		counter.Inc()
	case <-time.After(2 * time.Second):
		t.Error("timeout receiving message")
	}
	require.Equal(t, int64(0), counter.Load())
	newReq = getAckMessageRequest(rxMsg.Attributes.Sequence)
	reqCh <- newReq
	select {
	case rxRes = <-resCh:
		require.NotNil(t, rxRes)
		assert.Equal(t, newReq.RequestID, rxRes.RequestID)
		assert.Equal(t, newReq.StreamRequestTypeData, rxRes.StreamRequestTypeData)
		assert.False(t, rxRes.IsError)
		assert.Empty(t, rxRes.Error)
		counter.Inc()
	case <-done:
		counter.Inc()
	case <-time.After(2 * time.Second):
		t.Error("timeout receiving message")
	}
	require.Equal(t, int64(1), counter.Load())
}
func TestQueueClient_StreamQueueMessage_VisibilityTimeout(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()

	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	failedReceive := 2
	queue := "some-queue-channel"
	for i := 0; i < 1; i++ {
		sendMsg := &pb.QueueMessage{
			MessageID:  nuid.Next(),
			ClientID:   "some-client-id",
			Channel:    queue,
			Metadata:   "",
			Body:       []byte("1"),
			Attributes: nil,
			Policy: &pb.QueueMessagePolicy{
				ExpirationSeconds: 0,
				DelaySeconds:      0,
				MaxReceiveCount:   int32(failedReceive),
				MaxReceiveQueue:   "",
			},
		}
		response := c.SendQueueMessage(ctx, sendMsg)
		require.False(t, response.IsError)
	}

	time.Sleep(time.Second)

	c2, _ := NewQueueClient(getClientOptions(t, a), a.Queue)
	defer func() {
		_ = c2.Disconnect()
	}()
	counter := atomic.NewInt64(0)
	// ignoring the receive messages
	for i := 0; i < failedReceive; i++ {
		counter.Store(0)
		reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
		resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
		done := make(chan bool, 1)
		var rxRes *pb.StreamQueueMessagesResponse
		go c2.StreamQueueMessage(ctx, reqCh, resCh, done)
		newReq := getReceiveMessageRequest(queue, 1)
		reqCh <- newReq

		select {
		case rxRes = <-resCh:
			require.NotNil(t, rxRes)
			counter.Inc()

		case <-time.After(2 * time.Second):
			t.Error("timeout receiving message")
		}

		select {
		case <-resCh:
			counter.Inc()
		case <-time.After(2 * time.Second):
			t.Error("timeout receiving message")
		}

		select {

		case <-done:
			counter.Inc()
		case <-time.After(2 * time.Second):
			t.Error("timeout receiving message")
		}

		require.Equal(t, int64(3), counter.Load())

	}

	response, err := c2.ReceiveQueueMessages(ctx,
		&pb.ReceiveQueueMessagesRequest{
			ClientID:             "some-receiving-clientid",
			Channel:              "some-queue-channel",
			MaxNumberOfMessages:  10,
			WaitTimeSeconds:      1,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		})

	require.NoError(t, err)
	require.Equal(t, 0, len(response.Messages))

}
func TestQueueClient_StreamQueueMessage_WaitTimeout(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()

	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()

	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	require.NoError(t, err)
	time.Sleep(1 * time.Second)

	c2, _ := NewQueueClient(getClientOptions(t, a), a.Queue)
	defer func() {
		_ = c2.Disconnect()
	}()
	counter := atomic.NewInt64(0)
	// ignoring the receive messages
	reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
	resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
	done := make(chan bool, 1)
	var rxRes *pb.StreamQueueMessagesResponse
	go c2.StreamQueueMessage(ctx, reqCh, resCh, done)
	newReq := getReceiveMessageRequest("some-queue", 1)
	newReq.WaitTimeSeconds = 3
	reqCh <- newReq

	select {
	case rxRes = <-resCh:
		require.NotNil(t, rxRes)
		assert.Equal(t, newReq.RequestID, rxRes.RequestID)
		assert.True(t, rxRes.IsError)
		assert.NotEmpty(t, rxRes.Error)
		counter.Inc()
	case <-done:
		counter.Inc()
	case <-time.After(5 * time.Second):
		t.Error("timeout receiving message")
	}
	require.Equal(t, int64(1), counter.Load())

}
func TestQueueClient_StreamQueueMessage_WaitTimeoutAndVisibility(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()

	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	require.NoError(t, err)
	time.Sleep(1 * time.Second)

	c2, _ := NewQueueClient(getClientOptions(t, a), a.Queue)
	defer func() {
		_ = c2.Disconnect()
	}()
	counter := atomic.NewInt64(0)
	// ignoring the receive messages
	reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
	resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
	done := make(chan bool, 1)
	var rxRes *pb.StreamQueueMessagesResponse
	go c2.StreamQueueMessage(ctx, reqCh, resCh, done)
	newReq := getReceiveMessageRequest("some-queue", 1)
	newReq.WaitTimeSeconds = 5
	newReq.VisibilitySeconds = 5

	reqCh <- newReq
	go func() {
		time.Sleep(4 * time.Second)
		sendMsg := &pb.QueueMessage{
			MessageID:  nuid.Next(),
			ClientID:   "some-client-id",
			Channel:    "some-queue",
			Metadata:   "",
			Body:       []byte("1"),
			Attributes: nil,
			Policy: &pb.QueueMessagePolicy{
				ExpirationSeconds: 0,
				DelaySeconds:      0,
				MaxReceiveCount:   0,
				MaxReceiveQueue:   "",
			},
		}
		response := c.SendQueueMessage(ctx, sendMsg)
		require.False(t, response.IsError)
	}()
	select {
	case rxRes = <-resCh:
		require.NotNil(t, rxRes)
		assert.Equal(t, newReq.RequestID, rxRes.RequestID)
		assert.False(t, rxRes.IsError)
		assert.Empty(t, rxRes.Error)
		require.NotNil(t, rxRes.Message)
		rxMsg := rxRes.Message
		assert.NotZero(t, rxMsg.Attributes.Sequence)
		assert.NotZero(t, rxMsg.Attributes.Timestamp)
		assert.Equal(t, int32(1), rxMsg.Attributes.ReceiveCount)
		time.Sleep(4 * time.Second)
		newReq = getAckMessageRequest(rxMsg.Attributes.Sequence)
		reqCh <- newReq
	case <-done:
		counter.Inc()
	case <-time.After(10 * time.Second):
		t.Error("timeout receiving message")
	}
	require.Equal(t, int64(0), counter.Load())

	select {
	case <-resCh:
		counter.Inc()
	case <-done:
		counter.Inc()
	case <-time.After(10 * time.Second):
		t.Error("timeout receiving message")
	}

	require.Equal(t, int64(1), counter.Load())

}
func TestQueueClient_StreamQueueMessage_ClientDisconnect(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()

	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	failedReceive := 2
	queue := "some-queue-channel"
	for i := 0; i < 1; i++ {
		sendMsg := &pb.QueueMessage{
			MessageID:  nuid.Next(),
			ClientID:   "some-client-id",
			Channel:    queue,
			Metadata:   "",
			Body:       []byte("1"),
			Attributes: nil,
			Policy: &pb.QueueMessagePolicy{
				ExpirationSeconds: 0,
				DelaySeconds:      0,
				MaxReceiveCount:   int32(failedReceive),
				MaxReceiveQueue:   "",
			},
		}
		response := c.SendQueueMessage(ctx, sendMsg)
		require.False(t, response.IsError)

	}

	time.Sleep(time.Second)

	c2, _ := NewQueueClient(getClientOptions(t, a), a.Queue)
	defer func() {
		_ = c2.Disconnect()
	}()
	counter := atomic.NewInt64(0)
	// ignoring the receive messages
	for i := 0; i < failedReceive; i++ {
		counter.Store(0)
		reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
		resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
		done := make(chan bool, 1)
		var rxRes *pb.StreamQueueMessagesResponse
		newCtx, cancel := context.WithCancel(ctx)
		go c2.StreamQueueMessage(newCtx, reqCh, resCh, done)
		newReq := getReceiveMessageRequest(queue, 10)
		reqCh <- newReq
		select {
		case rxRes = <-resCh:
			require.NotNil(t, rxRes)
			counter.Inc()
		case <-time.After(2 * time.Second):
			t.Error("timeout receiving message")
		}
		cancel()
		reqCh <- nil
		select {

		case <-done:
			counter.Inc()

		case <-time.After(2 * time.Second):
			t.Error("timeout receiving message")
		}
		require.Equal(t, int64(2), counter.Load())
	}
	response, err := c2.ReceiveQueueMessages(ctx,
		&pb.ReceiveQueueMessagesRequest{
			ClientID:             "some-receiving-clientid",
			Channel:              queue,
			MaxNumberOfMessages:  1,
			WaitTimeSeconds:      1,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		})

	require.NoError(t, err)
	require.Equal(t, 0, len(response.Messages))

}
func TestQueueClient_StreamQueueMessage_Reject(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()

	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	failedReceive := 2
	queue := "some-queue-channel"
	for i := 0; i < 1; i++ {
		sendMsg := &pb.QueueMessage{
			MessageID:  nuid.Next(),
			ClientID:   "some-client-id",
			Channel:    queue,
			Metadata:   "",
			Body:       []byte("1"),
			Attributes: nil,
			Policy: &pb.QueueMessagePolicy{
				ExpirationSeconds: 0,
				DelaySeconds:      0,
				MaxReceiveCount:   int32(failedReceive),
				MaxReceiveQueue:   "",
			},
		}
		response := c.SendQueueMessage(ctx, sendMsg)
		require.False(t, response.IsError)
	}

	time.Sleep(time.Second)

	c2, _ := NewQueueClient(getClientOptions(t, a), a.Queue)
	defer func() {
		_ = c2.Disconnect()
	}()
	counter := atomic.NewInt64(0)
	// ignoring the receive messages
	for i := 0; i < failedReceive; i++ {
		counter.Store(0)
		reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
		resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
		done := make(chan bool, 1)
		var rxRes *pb.StreamQueueMessagesResponse
		go c2.StreamQueueMessage(ctx, reqCh, resCh, done)
		newReq := getReceiveMessageRequest(queue, 10)
		reqCh <- newReq
		select {
		case rxRes = <-resCh:
			require.NotNil(t, rxRes)
			counter.Inc()
		case <-time.After(2 * time.Second):
			t.Error("timeout receiving message")
		}
		require.NotNil(t, rxRes.Message)
		rxMsg := rxRes.Message
		assert.NotZero(t, rxMsg.Attributes.Sequence)
		assert.NotZero(t, rxMsg.Attributes.Timestamp)
		newReq = getRejectMessageRequest(rxMsg.Attributes.Sequence)
		reqCh <- newReq
		select {
		case <-resCh:
			counter.Inc()
		case <-time.After(2 * time.Second):
			t.Error("timeout receiving message")
		}
		select {
		case <-done:
			counter.Inc()
		case <-time.After(2 * time.Second):
			t.Error("timeout receiving message")
		}
		require.Equal(t, int64(3), counter.Load())
	}
	response, err := c2.ReceiveQueueMessages(ctx,
		&pb.ReceiveQueueMessagesRequest{
			ClientID:             "some-receiving-clientid",
			Channel:              "some-queue-channel",
			MaxNumberOfMessages:  1,
			WaitTimeSeconds:      1,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		})

	require.NoError(t, err)
	require.Equal(t, 0, len(response.Messages))

}
func TestQueueClient_StreamQueueMessage_ResendMessage(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()

	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	queue := "some-queue-channel"
	for i := 0; i < 1; i++ {
		sendMsg := &pb.QueueMessage{
			MessageID:  nuid.Next(),
			ClientID:   "some-client-id",
			Channel:    queue,
			Metadata:   "",
			Body:       []byte("1"),
			Attributes: nil,
			Policy: &pb.QueueMessagePolicy{
				ExpirationSeconds: 0,
				DelaySeconds:      0,
				MaxReceiveCount:   0,
				MaxReceiveQueue:   "",
			},
		}
		response := c.SendQueueMessage(ctx, sendMsg)
		require.False(t, response.IsError)
	}

	time.Sleep(time.Second)

	c2, _ := NewQueueClient(getClientOptions(t, a), a.Queue)
	defer func() {
		_ = c2.Disconnect()
	}()
	counter := atomic.NewInt64(0)
	reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
	resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
	done := make(chan bool, 1)
	go c2.StreamQueueMessage(ctx, reqCh, resCh, done)
	require.NoError(t, err)
	newReq := getReceiveMessageRequest(queue, 10)
	reqCh <- newReq
	var rxRes *pb.StreamQueueMessagesResponse
	select {
	case rxRes = <-resCh:
		require.NotNil(t, rxRes)
		assert.Equal(t, newReq.RequestID, rxRes.RequestID)
		assert.Equal(t, newReq.StreamRequestTypeData, rxRes.StreamRequestTypeData)
		assert.False(t, rxRes.IsError)
		assert.Empty(t, rxRes.Error)
		assert.NotNil(t, rxRes.Message)
		rxMsg := rxRes.Message
		assert.NotZero(t, rxMsg.Attributes.Sequence)
		assert.NotZero(t, rxMsg.Attributes.Timestamp)
		assert.Equal(t, int32(1), rxMsg.Attributes.ReceiveCount)
	case <-time.After(2 * time.Second):
		t.Error("timeout receiving message")
	}
	newReq = getResendQueueMessage(queue + "new")
	reqCh <- newReq
	for {
		if counter.Load() == 2 {
			break
		}
		select {
		case rxRes = <-resCh:
			require.NotNil(t, rxRes)
			require.Equal(t, newReq.RequestID, rxRes.RequestID)
			require.False(t, rxRes.IsError)
			assert.Empty(t, rxRes.Error)
			counter.Inc()
		case <-done:
			counter.Inc()
		case <-time.After(2 * time.Second):
			t.Fatal("timeout receiving message")

		}
	}
	require.Equal(t, int64(2), counter.Load())
	counter.Store(0)
	reqCh = make(chan *pb.StreamQueueMessagesRequest, 1)
	resCh = make(chan *pb.StreamQueueMessagesResponse, 1)
	done = make(chan bool, 1)
	go c2.StreamQueueMessage(ctx, reqCh, resCh, done)
	newReq = getReceiveMessageRequest(queue+"new", 1)
	reqCh <- newReq
	select {
	case rxRes = <-resCh:
		require.NotNil(t, rxRes)
		assert.Equal(t, newReq.RequestID, rxRes.RequestID)
		assert.Equal(t, newReq.StreamRequestTypeData, rxRes.StreamRequestTypeData)
		assert.False(t, rxRes.IsError)
		assert.Empty(t, rxRes.Error)
		assert.NotNil(t, rxRes.Message)
		rxMsg := rxRes.Message
		assert.NotZero(t, rxMsg.Attributes.Sequence)
		assert.NotZero(t, rxMsg.Attributes.Timestamp)
		assert.Equal(t, int32(1), rxMsg.Attributes.ReceiveCount)
	case <-done:
		counter.Inc()
	case <-time.After(2 * time.Second):
		t.Fatal("timeout receiving message")

	}
	require.Equal(t, int64(0), counter.Load())
}
func TestQueueClient_StreamQueueMessage_ModifiedMessage(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	defer func() {
		_ = c.Disconnect()
	}()

	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	queue := "some-queue-channel"
	for i := 0; i < 1; i++ {
		sendMsg := &pb.QueueMessage{
			MessageID:  nuid.Next(),
			ClientID:   "some-client-id",
			Channel:    queue,
			Metadata:   "",
			Body:       []byte("1"),
			Attributes: nil,
			Policy: &pb.QueueMessagePolicy{
				ExpirationSeconds: 0,
				DelaySeconds:      0,
				MaxReceiveCount:   0,
				MaxReceiveQueue:   "",
			},
		}
		response := c.SendQueueMessage(ctx, sendMsg)
		require.False(t, response.IsError)
	}

	time.Sleep(time.Second)

	c2, _ := NewQueueClient(getClientOptions(t, a), a.Queue)
	defer func() {
		_ = c2.Disconnect()
	}()
	counter := atomic.NewInt64(0)
	reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
	resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
	done := make(chan bool, 1)
	go c2.StreamQueueMessage(ctx, reqCh, resCh, done)
	require.NoError(t, err)
	newReq := getReceiveMessageRequest(queue, 10)
	reqCh <- newReq
	var rxRes *pb.StreamQueueMessagesResponse
	select {
	case rxRes = <-resCh:
		require.NotNil(t, rxRes)
		assert.Equal(t, newReq.RequestID, rxRes.RequestID)
		assert.Equal(t, newReq.StreamRequestTypeData, rxRes.StreamRequestTypeData)
		assert.False(t, rxRes.IsError)
		assert.Empty(t, rxRes.Error)
		assert.NotNil(t, rxRes.Message)

	case <-time.After(2 * time.Second):
		t.Error("timeout receiving message")
	}
	rxMsg := rxRes.Message
	assert.NotZero(t, rxMsg.Attributes.Sequence)
	assert.NotZero(t, rxMsg.Attributes.Timestamp)
	assert.Equal(t, int32(1), rxMsg.Attributes.ReceiveCount)
	rxMsg.Channel = queue + "new"
	rxMsg.Body = []byte("2")
	newReq = getModifyQueueMessage(rxMsg)
	reqCh <- newReq
	for {
		if counter.Load() == 2 {
			break
		}
		select {
		case rxRes = <-resCh:
			require.NotNil(t, rxRes)
			require.Equal(t, newReq.RequestID, rxRes.RequestID)
			require.False(t, rxRes.IsError)
			assert.Empty(t, rxRes.Error)
			counter.Inc()
		case <-done:
			counter.Inc()
		case <-time.After(2 * time.Second):
			t.Fatal("timeout receiving message")

		}
	}
	require.Equal(t, int64(2), counter.Load())
	counter.Store(0)
	reqCh = make(chan *pb.StreamQueueMessagesRequest, 1)
	resCh = make(chan *pb.StreamQueueMessagesResponse, 1)
	done = make(chan bool, 1)
	go c2.StreamQueueMessage(ctx, reqCh, resCh, done)
	newReq = getReceiveMessageRequest(queue+"new", 1)
	reqCh <- newReq
	select {
	case rxRes = <-resCh:
		require.NotNil(t, rxRes)
		assert.Equal(t, newReq.RequestID, rxRes.RequestID)
		assert.Equal(t, newReq.StreamRequestTypeData, rxRes.StreamRequestTypeData)
		assert.False(t, rxRes.IsError)
		assert.Empty(t, rxRes.Error)
		assert.NotNil(t, rxRes.Message)
		rxMsg := rxRes.Message
		assert.NotZero(t, rxMsg.Attributes.Sequence)
		assert.NotZero(t, rxMsg.Attributes.Timestamp)
		assert.Equal(t, int32(1), rxMsg.Attributes.ReceiveCount)
		assert.Equal(t, []byte("2"), rxMsg.Body)
	case <-done:
		counter.Inc()
	case <-time.After(2 * time.Second):
		t.Fatal("timeout receiving message")

	}
	require.Equal(t, int64(0), counter.Load())
}
func TestQueueClient_StreamQueueMessageErrors(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	time.Sleep(1 * time.Second)

	tests := []struct {
		name string
		req  *pb.StreamQueueMessagesRequest
		res  *pb.StreamQueueMessagesResponse
	}{
		{
			name: "blank_request",
			req: &pb.StreamQueueMessagesRequest{
				RequestID:             "some-request-id",
				ClientID:              "",
				StreamRequestTypeData: 0,
				Channel:               "",
				WaitTimeSeconds:       0,
				VisibilitySeconds:     0,
				RefSequence:           0,
				ModifiedMessage:       nil,
			},
			res: &pb.StreamQueueMessagesResponse{
				RequestID:             "some-request-id",
				StreamRequestTypeData: 0,
				Message:               nil,
				IsError:               true,
				Error:                 entities.ErrInvalidClientID.Error(),
			},
		},
		{
			name: "receive_message_without_queue_name",
			req: &pb.StreamQueueMessagesRequest{
				RequestID:             "some-request-id",
				ClientID:              "some-client-id",
				StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
				Channel:               "",
				VisibilitySeconds:     0,
				WaitTimeSeconds:       0,
				RefSequence:           0,
				ModifiedMessage:       nil,
			},
			res: &pb.StreamQueueMessagesResponse{
				RequestID:             "some-request-id",
				StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
				Message:               nil,
				IsError:               true,
				Error:                 entities.ErrInvalidQueueName.Error(),
			},
		},
		{
			name: "receive_message_invalid_wait_timeout1",
			req: &pb.StreamQueueMessagesRequest{
				RequestID:             "some-request-id",
				ClientID:              "some-client-id",
				StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
				Channel:               "some-queue",
				VisibilitySeconds:     0,
				WaitTimeSeconds:       1000000000,
				RefSequence:           0,
				ModifiedMessage:       nil,
			},
			res: &pb.StreamQueueMessagesResponse{
				RequestID:             "some-request-id",
				StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
				Message:               nil,
				IsError:               true,
				Error:                 entities.ErrInvalidWaitTimeout.Error(),
				XXX_NoUnkeyedLiteral:  struct{}{},
				XXX_sizecache:         0,
			},
		},
		{
			name: "receive_message_invalid_wait_timeout2",
			req: &pb.StreamQueueMessagesRequest{
				RequestID:             "some-request-id",
				ClientID:              "some-client-id",
				StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
				Channel:               "some-queue",
				VisibilitySeconds:     0,
				WaitTimeSeconds:       -1,
				RefSequence:           0,
				ModifiedMessage:       nil,
			},
			res: &pb.StreamQueueMessagesResponse{
				RequestID:             "some-request-id",
				StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
				Message:               nil,
				IsError:               true,
				Error:                 entities.ErrInvalidWaitTimeout.Error(),
				XXX_NoUnkeyedLiteral:  struct{}{},
				XXX_sizecache:         0,
			},
		},
		{
			name: "receive_message_invalid_visibility1",
			req: &pb.StreamQueueMessagesRequest{
				RequestID:             "some-request-id",
				ClientID:              "some-client-id",
				StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
				Channel:               "some-queue",
				VisibilitySeconds:     1000000000,
				WaitTimeSeconds:       0,
				RefSequence:           0,
				ModifiedMessage:       nil,
			},
			res: &pb.StreamQueueMessagesResponse{
				RequestID:             "some-request-id",
				StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
				Message:               nil,
				IsError:               true,
				Error:                 entities.ErrInvalidVisibility.Error(),
				XXX_NoUnkeyedLiteral:  struct{}{},
				XXX_sizecache:         0,
			},
		},
		{
			name: "receive_message_invalid_visibility2",
			req: &pb.StreamQueueMessagesRequest{
				RequestID:             "some-request-id",
				ClientID:              "some-client-id",
				StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
				Channel:               "some-queue",
				VisibilitySeconds:     -1,
				WaitTimeSeconds:       0,
				RefSequence:           0,
				ModifiedMessage:       nil,
			},
			res: &pb.StreamQueueMessagesResponse{
				RequestID:             "some-request-id",
				StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
				Message:               nil,
				IsError:               true,
				Error:                 entities.ErrInvalidVisibility.Error(),
				XXX_NoUnkeyedLiteral:  struct{}{},
				XXX_sizecache:         0,
			},
		},
		{
			name: "receive_message_invalid_visibility3",
			req: &pb.StreamQueueMessagesRequest{
				RequestID:             "some-request-id",
				ClientID:              "some-client-id",
				StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
				Channel:               "some-queue",
				VisibilitySeconds:     43201,
				WaitTimeSeconds:       0,
				RefSequence:           0,
				ModifiedMessage:       nil,
			},
			res: &pb.StreamQueueMessagesResponse{
				RequestID:             "some-request-id",
				StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
				Message:               nil,
				IsError:               true,
				Error:                 entities.ErrInvalidVisibility.Error(),
				XXX_NoUnkeyedLiteral:  struct{}{},
				XXX_sizecache:         0,
			},
		},
		{
			name: "receive_message_invalid_ack",
			req: &pb.StreamQueueMessagesRequest{
				RequestID:             "some-request-id",
				ClientID:              "some-client-id",
				StreamRequestTypeData: pb.StreamRequestType_AckMessage,
				Channel:               "some-queue",
				VisibilitySeconds:     0,
				WaitTimeSeconds:       0,
				RefSequence:           0,
				ModifiedMessage:       nil,
			},
			res: &pb.StreamQueueMessagesResponse{
				RequestID:             "some-request-id",
				StreamRequestTypeData: pb.StreamRequestType_AckMessage,
				Message:               nil,
				IsError:               true,
				Error:                 entities.ErrInvalidAckSeq.Error(),
				XXX_NoUnkeyedLiteral:  struct{}{},
				XXX_sizecache:         0,
			},
		},
		{
			name: "receive_message_invalid_reject",
			req: &pb.StreamQueueMessagesRequest{
				RequestID:             "some-request-id",
				ClientID:              "some-client-id",
				StreamRequestTypeData: pb.StreamRequestType_RejectMessage,
				Channel:               "some-queue",
				VisibilitySeconds:     0,
				WaitTimeSeconds:       0,
				RefSequence:           0,
				ModifiedMessage:       nil,
			},
			res: &pb.StreamQueueMessagesResponse{
				RequestID:             "some-request-id",
				StreamRequestTypeData: pb.StreamRequestType_RejectMessage,
				Message:               nil,
				IsError:               true,
				Error:                 entities.ErrInvalidAckSeq.Error(),
				XXX_NoUnkeyedLiteral:  struct{}{},
				XXX_sizecache:         0,
			},
		},
		{
			name: "receive_message_invalid_modify_visibility1",
			req: &pb.StreamQueueMessagesRequest{
				RequestID:             "some-request-id",
				ClientID:              "some-client-id",
				StreamRequestTypeData: pb.StreamRequestType_ModifyVisibility,
				Channel:               "some-queue",
				VisibilitySeconds:     100000000,
				WaitTimeSeconds:       0,
				RefSequence:           0,
				ModifiedMessage:       nil,
			},
			res: &pb.StreamQueueMessagesResponse{
				RequestID:             "some-request-id",
				StreamRequestTypeData: pb.StreamRequestType_ModifyVisibility,
				Message:               nil,
				IsError:               true,
				Error:                 entities.ErrInvalidVisibility.Error(),
				XXX_NoUnkeyedLiteral:  struct{}{},
				XXX_sizecache:         0,
			},
		},
		{
			name: "receive_message_invalid_modify_visibility2",
			req: &pb.StreamQueueMessagesRequest{
				RequestID:             "some-request-id",
				ClientID:              "some-client-id",
				StreamRequestTypeData: pb.StreamRequestType_ModifyVisibility,
				Channel:               "some-queue",
				VisibilitySeconds:     -1,
				WaitTimeSeconds:       0,
				RefSequence:           0,
				ModifiedMessage:       nil,
			},
			res: &pb.StreamQueueMessagesResponse{
				RequestID:             "some-request-id",
				StreamRequestTypeData: pb.StreamRequestType_ModifyVisibility,
				Message:               nil,
				IsError:               true,
				Error:                 entities.ErrInvalidVisibility.Error(),
				XXX_NoUnkeyedLiteral:  struct{}{},
				XXX_sizecache:         0,
			},
		},
		{
			name: "receive_message_invalid_resend",
			req: &pb.StreamQueueMessagesRequest{
				RequestID:             "some-request-id",
				ClientID:              "some-client-id",
				StreamRequestTypeData: pb.StreamRequestType_ResendMessage,
				Channel:               "some-queue",
				VisibilitySeconds:     0,
				WaitTimeSeconds:       0,
				RefSequence:           0,
				ModifiedMessage:       nil,
			},
			res: &pb.StreamQueueMessagesResponse{
				RequestID:             "some-request-id",
				StreamRequestTypeData: pb.StreamRequestType_ResendMessage,
				Message:               nil,
				IsError:               true,
				Error:                 entities.ErrNoCurrentMsgToSend.Error(),
				XXX_NoUnkeyedLiteral:  struct{}{},
				XXX_sizecache:         0,
			},
		},
		{
			name: "receive_message_invalid_resend_new",
			req: &pb.StreamQueueMessagesRequest{
				RequestID:             "some-request-id",
				ClientID:              "some-client-id",
				StreamRequestTypeData: pb.StreamRequestType_SendModifiedMessage,
				Channel:               "",
				VisibilitySeconds:     0,
				WaitTimeSeconds:       0,
				RefSequence:           1,
				ModifiedMessage:       nil,
			},
			res: &pb.StreamQueueMessagesResponse{
				RequestID:             "some-request-id",
				StreamRequestTypeData: pb.StreamRequestType_SendModifiedMessage,
				Message:               nil,
				IsError:               true,
				Error:                 entities.ErrInvalidQueueMessage.Error(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			o := getClientOptions(t, a)
			c, err := NewQueueClient(o, a.Queue)
			require.NoError(t, err)
			defer func() {
				_ = c.Disconnect()
			}()

			reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
			resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
			done := make(chan bool, 1)

			go c.StreamQueueMessage(ctx, reqCh, resCh, done)
			require.NoError(t, err)
			reqCh <- tt.req
			var rxRes *pb.StreamQueueMessagesResponse
			timer := time.NewTimer(time.Second)
			select {
			case rxRes = <-resCh:
			case <-done:
				require.True(t, false)
			case <-timer.C:
				require.True(t, false)
			}
			require.EqualValues(t, tt.res, rxRes)

		})
	}
}
func TestQueueClient_SendQueueMessageErrors(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	time.Sleep(1 * time.Second)

	tests := []struct {
		name string
		msg  *pb.QueueMessage
		res  *pb.SendQueueMessageResult
	}{
		{
			name: "blank_request",
			msg: &pb.QueueMessage{
				MessageID:  "some-message-id",
				ClientID:   "",
				Channel:    "",
				Metadata:   "",
				Body:       nil,
				Tags:       nil,
				Attributes: nil,
				Policy:     nil,
			},
			res: &pb.SendQueueMessageResult{
				MessageID:    "some-message-id",
				SentAt:       0,
				ExpirationAt: 0,
				DelayedTo:    0,
				IsError:      true,
				Error:        entities.ErrInvalidClientID.Error(),
			},
		},
		{
			name: "no_queue_name",
			msg: &pb.QueueMessage{
				MessageID:  "some-message-id",
				ClientID:   "some-clientID",
				Channel:    "",
				Metadata:   "",
				Body:       nil,
				Tags:       nil,
				Attributes: nil,
				Policy:     nil,
			},
			res: &pb.SendQueueMessageResult{
				MessageID:    "some-message-id",
				SentAt:       0,
				ExpirationAt: 0,
				DelayedTo:    0,
				IsError:      true,
				Error:        entities.ErrInvalidQueueName.Error(),
			},
		},
		{
			name: "bad_queue_name",
			msg: &pb.QueueMessage{
				MessageID:  "some-message-id",
				ClientID:   "some-clientID",
				Channel:    "some-bad-queue-name.*",
				Metadata:   "",
				Body:       nil,
				Tags:       nil,
				Attributes: nil,
				Policy:     nil,
			},
			res: &pb.SendQueueMessageResult{
				MessageID:    "some-message-id",
				SentAt:       0,
				ExpirationAt: 0,
				DelayedTo:    0,
				IsError:      true,
				Error:        entities.ErrInvalidQueueName.Error(),
			},
		},
		{
			name: "no_data",
			msg: &pb.QueueMessage{
				MessageID:  "some-message-id",
				ClientID:   "some-clientID",
				Channel:    "some-queue-name",
				Metadata:   "",
				Body:       nil,
				Tags:       nil,
				Attributes: nil,
				Policy:     nil,
			},
			res: &pb.SendQueueMessageResult{
				MessageID:    "some-message-id",
				SentAt:       0,
				ExpirationAt: 0,
				DelayedTo:    0,
				IsError:      true,
				Error:        entities.ErrMessageEmpty.Error(),
			},
		},
		{
			name: "bad_policy_expiration1",
			msg: &pb.QueueMessage{
				MessageID:  "some-message-id",
				ClientID:   "some-clientID",
				Channel:    "some-queue-name",
				Metadata:   "some-metadata",
				Body:       nil,
				Tags:       nil,
				Attributes: nil,
				Policy: &pb.QueueMessagePolicy{
					ExpirationSeconds:    -1,
					DelaySeconds:         0,
					MaxReceiveCount:      0,
					MaxReceiveQueue:      "",
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_sizecache:        0,
				},
			},
			res: &pb.SendQueueMessageResult{
				MessageID:    "some-message-id",
				SentAt:       0,
				ExpirationAt: 0,
				DelayedTo:    0,
				IsError:      true,
				Error:        entities.ErrInvalidExpiration.Error(),
			},
		},
		{
			name: "bad_policy_expiration2",
			msg: &pb.QueueMessage{
				MessageID:  "some-message-id",
				ClientID:   "some-clientID",
				Channel:    "some-queue-name",
				Metadata:   "some-metadata",
				Body:       nil,
				Tags:       nil,
				Attributes: nil,
				Policy: &pb.QueueMessagePolicy{
					ExpirationSeconds:    1000000000,
					DelaySeconds:         0,
					MaxReceiveCount:      0,
					MaxReceiveQueue:      "",
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_sizecache:        0,
				},
			},
			res: &pb.SendQueueMessageResult{
				MessageID:    "some-message-id",
				SentAt:       0,
				ExpirationAt: 0,
				DelayedTo:    0,
				IsError:      true,
				Error:        entities.ErrInvalidExpiration.Error(),
			},
		},
		{
			name: "bad_policy_delay1",
			msg: &pb.QueueMessage{
				MessageID:  "some-message-id",
				ClientID:   "some-clientID",
				Channel:    "some-queue-name",
				Metadata:   "some-metadata",
				Body:       nil,
				Tags:       nil,
				Attributes: nil,
				Policy: &pb.QueueMessagePolicy{
					ExpirationSeconds:    0,
					DelaySeconds:         -1,
					MaxReceiveCount:      0,
					MaxReceiveQueue:      "",
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_sizecache:        0,
				},
			},
			res: &pb.SendQueueMessageResult{
				MessageID:    "some-message-id",
				SentAt:       0,
				ExpirationAt: 0,
				DelayedTo:    0,
				IsError:      true,
				Error:        entities.ErrInvalidDelay.Error(),
			},
		},
		{
			name: "bad_policy_delay2",
			msg: &pb.QueueMessage{
				MessageID:  "some-message-id",
				ClientID:   "some-clientID",
				Channel:    "some-queue-name",
				Metadata:   "some-metadata",
				Body:       nil,
				Tags:       nil,
				Attributes: nil,
				Policy: &pb.QueueMessagePolicy{
					ExpirationSeconds:    0,
					DelaySeconds:         100000000,
					MaxReceiveCount:      0,
					MaxReceiveQueue:      "",
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_sizecache:        0,
				},
			},
			res: &pb.SendQueueMessageResult{
				MessageID:    "some-message-id",
				SentAt:       0,
				ExpirationAt: 0,
				DelayedTo:    0,
				IsError:      true,
				Error:        entities.ErrInvalidDelay.Error(),
			},
		},
		{
			name: "bad_policy_max_receive_count1",
			msg: &pb.QueueMessage{
				MessageID:  "some-message-id",
				ClientID:   "some-clientID",
				Channel:    "some-queue-name",
				Metadata:   "some-metadata",
				Body:       nil,
				Tags:       nil,
				Attributes: nil,
				Policy: &pb.QueueMessagePolicy{
					ExpirationSeconds:    0,
					DelaySeconds:         0,
					MaxReceiveCount:      -1,
					MaxReceiveQueue:      "",
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_sizecache:        0,
				},
			},
			res: &pb.SendQueueMessageResult{
				MessageID:    "some-message-id",
				SentAt:       0,
				ExpirationAt: 0,
				DelayedTo:    0,
				IsError:      true,
				Error:        entities.ErrInvalidMaxReceiveCount.Error(),
			},
		},
		{
			name: "bad_policy_max_receive_count2",
			msg: &pb.QueueMessage{
				MessageID:  "some-message-id",
				ClientID:   "some-clientID",
				Channel:    "some-queue-name",
				Metadata:   "some-metadata",
				Body:       nil,
				Tags:       nil,
				Attributes: nil,
				Policy: &pb.QueueMessagePolicy{
					ExpirationSeconds:    0,
					DelaySeconds:         0,
					MaxReceiveCount:      1000000000,
					MaxReceiveQueue:      "",
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_sizecache:        0,
				},
			},
			res: &pb.SendQueueMessageResult{
				MessageID:    "some-message-id",
				SentAt:       0,
				ExpirationAt: 0,
				DelayedTo:    0,
				IsError:      true,
				Error:        entities.ErrInvalidMaxReceiveCount.Error(),
			},
		},
		{
			name: "bad_policy_max_receive_queue_bad_queue1",
			msg: &pb.QueueMessage{
				MessageID:  "some-message-id",
				ClientID:   "some-clientID",
				Channel:    "some-queue-name",
				Metadata:   "some-metadata",
				Body:       nil,
				Tags:       nil,
				Attributes: nil,
				Policy: &pb.QueueMessagePolicy{
					ExpirationSeconds:    0,
					DelaySeconds:         0,
					MaxReceiveCount:      3,
					MaxReceiveQueue:      "bad_qeueu_name_*",
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_sizecache:        0,
				},
			},
			res: &pb.SendQueueMessageResult{
				MessageID:    "some-message-id",
				SentAt:       0,
				ExpirationAt: 0,
				DelayedTo:    0,
				IsError:      true,
				Error:        entities.ErrInvalidQueueName.Error(),
			},
		},
		{
			name: "bad_policy_max_receive_queue_bad_queue2",
			msg: &pb.QueueMessage{
				MessageID:  "some-message-id",
				ClientID:   "some-clientID",
				Channel:    "some-queue-name",
				Metadata:   "some-metadata",
				Body:       nil,
				Tags:       nil,
				Attributes: nil,
				Policy: &pb.QueueMessagePolicy{
					ExpirationSeconds:    0,
					DelaySeconds:         0,
					MaxReceiveCount:      3,
					MaxReceiveQueue:      "bad_qeueu_name_.",
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_sizecache:        0,
				},
			},
			res: &pb.SendQueueMessageResult{
				MessageID:    "some-message-id",
				SentAt:       0,
				ExpirationAt: 0,
				DelayedTo:    0,
				IsError:      true,
				Error:        entities.ErrInvalidQueueName.Error(),
			},
		},
		{
			name: "bad_policy_max_receive_queue_bad_queue3",
			msg: &pb.QueueMessage{
				MessageID:  "some-message-id",
				ClientID:   "some-clientID",
				Channel:    "some-queue-name",
				Metadata:   "some-metadata",
				Body:       nil,
				Tags:       nil,
				Attributes: nil,
				Policy: &pb.QueueMessagePolicy{
					ExpirationSeconds:    0,
					DelaySeconds:         0,
					MaxReceiveCount:      3,
					MaxReceiveQueue:      "bad_qeueu_name_  ",
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_sizecache:        0,
				},
			},
			res: &pb.SendQueueMessageResult{
				MessageID:    "some-message-id",
				SentAt:       0,
				ExpirationAt: 0,
				DelayedTo:    0,
				IsError:      true,
				Error:        entities.ErrInvalidQueueName.Error(),
			},
		},
		{
			name: "bad_policy_max_receive_queue_bad_queue4",
			msg: &pb.QueueMessage{
				MessageID:  "some-message-id",
				ClientID:   "some-clientID",
				Channel:    "some-queue-name",
				Metadata:   "some-metadata",
				Body:       nil,
				Tags:       nil,
				Attributes: nil,
				Policy: &pb.QueueMessagePolicy{
					ExpirationSeconds:    0,
					DelaySeconds:         0,
					MaxReceiveCount:      3,
					MaxReceiveQueue:      "bad_qeueu_name_>",
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_sizecache:        0,
				},
			},
			res: &pb.SendQueueMessageResult{
				MessageID:    "some-message-id",
				SentAt:       0,
				ExpirationAt: 0,
				DelayedTo:    0,
				IsError:      true,
				Error:        entities.ErrInvalidQueueName.Error(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			o := getClientOptions(t, a)
			c, err := NewQueueClient(o, a.Queue)
			require.NoError(t, err)
			defer func() {
				_ = c.Disconnect()
			}()

			result := c.SendQueueMessage(ctx, tt.msg)
			assert.EqualValues(t, tt.res, result)
		})
	}
}
func TestQueueClient_ReceiveQueueMessagesRequestErrors(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()

	time.Sleep(1 * time.Second)

	tests := []struct {
		name string
		req  *pb.ReceiveQueueMessagesRequest
		res  *pb.ReceiveQueueMessagesResponse
	}{
		{
			name: "blank_request",
			req: &pb.ReceiveQueueMessagesRequest{
				RequestID:           "some-request-id",
				ClientID:            "",
				Channel:             "",
				MaxNumberOfMessages: 0,
				WaitTimeSeconds:     0,
			},
			res: &pb.ReceiveQueueMessagesResponse{
				RequestID:        "some-request-id",
				Messages:         []*pb.QueueMessage{},
				MessagesReceived: 0,
				MessagesExpired:  0,
				IsError:          true,
				Error:            entities.ErrInvalidClientID.Error(),
			},
		},
		{
			name: "blank_queue_name",
			req: &pb.ReceiveQueueMessagesRequest{
				RequestID:           "some-request-id",
				ClientID:            "some-client-id",
				Channel:             "",
				MaxNumberOfMessages: 0,
				WaitTimeSeconds:     0,
			},
			res: &pb.ReceiveQueueMessagesResponse{
				RequestID:        "some-request-id",
				Messages:         []*pb.QueueMessage{},
				MessagesReceived: 0,
				MessagesExpired:  0,
				IsError:          true,
				Error:            entities.ErrInvalidQueueName.Error(),
			},
		},
		{
			name: "bad_queue_name",
			req: &pb.ReceiveQueueMessagesRequest{
				RequestID:           "some-request-id",
				ClientID:            "some-client-id",
				Channel:             "*.>",
				MaxNumberOfMessages: 0,
				WaitTimeSeconds:     0,
			},
			res: &pb.ReceiveQueueMessagesResponse{
				RequestID:        "some-request-id",
				Messages:         []*pb.QueueMessage{},
				MessagesReceived: 0,
				MessagesExpired:  0,
				IsError:          true,
				Error:            entities.ErrInvalidQueueName.Error(),
			},
		},
		{
			name: "bad_max_number1",
			req: &pb.ReceiveQueueMessagesRequest{
				RequestID:           "some-request-id",
				ClientID:            "some-client-id",
				Channel:             "some-queue",
				MaxNumberOfMessages: -1,
				WaitTimeSeconds:     0,
			},
			res: &pb.ReceiveQueueMessagesResponse{
				RequestID:        "some-request-id",
				Messages:         []*pb.QueueMessage{},
				MessagesReceived: 0,
				MessagesExpired:  0,
				IsError:          true,
				Error:            entities.ErrInvalidMaxMessages.Error(),
			},
		},
		{
			name: "bad_max_number2",
			req: &pb.ReceiveQueueMessagesRequest{
				RequestID:           "some-request-id",
				ClientID:            "some-client-id",
				Channel:             "some-queue",
				MaxNumberOfMessages: 1000000,
				WaitTimeSeconds:     0,
			},
			res: &pb.ReceiveQueueMessagesResponse{
				RequestID:        "some-request-id",
				Messages:         []*pb.QueueMessage{},
				MessagesReceived: 0,
				MessagesExpired:  0,
				IsError:          true,
				Error:            entities.ErrInvalidMaxMessages.Error(),
			},
		},
		{
			name: "bad_wait_times_seconds1",
			req: &pb.ReceiveQueueMessagesRequest{
				RequestID:           "some-request-id",
				ClientID:            "some-client-id",
				Channel:             "some-queue",
				MaxNumberOfMessages: 10,
				WaitTimeSeconds:     -1,
			},
			res: &pb.ReceiveQueueMessagesResponse{
				RequestID:        "some-request-id",
				Messages:         []*pb.QueueMessage{},
				MessagesReceived: 0,
				MessagesExpired:  0,
				IsError:          true,
				Error:            entities.ErrInvalidWaitTimeout.Error(),
			},
		},
		{
			name: "bad_wait_times_seconds2",
			req: &pb.ReceiveQueueMessagesRequest{
				RequestID:           "some-request-id",
				ClientID:            "some-client-id",
				Channel:             "some-queue",
				MaxNumberOfMessages: 10,
				WaitTimeSeconds:     10000000,
			},
			res: &pb.ReceiveQueueMessagesResponse{
				RequestID:        "some-request-id",
				Messages:         []*pb.QueueMessage{},
				MessagesReceived: 0,
				MessagesExpired:  0,
				IsError:          true,
				Error:            entities.ErrInvalidWaitTimeout.Error(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			o := getClientOptions(t, a)
			c, err := NewQueueClient(o, a.Queue)
			require.NoError(t, err)
			defer func() {
				_ = c.Disconnect()
			}()

			res, err := c.ReceiveQueueMessages(ctx, tt.req)
			require.NoError(t, err)
			assert.EqualValues(t, tt.res, res)
		})
	}
}
func TestQueueClient_AckAllQueueMessages(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	defer func() {
		_ = c.Disconnect()
	}()

	time.Sleep(1 * time.Second)
	messagesList := &pb.QueueMessagesBatchRequest{
		BatchID:  nuid.Next(),
		Messages: []*pb.QueueMessage{},
	}

	for i := 0; i < 10; i++ {
		messagesList.Messages = append(messagesList.Messages, &pb.QueueMessage{
			MessageID: nuid.Next(),
			ClientID:  "some-client-id",
			Channel:   "some-queue-channel",
			Metadata:  "",
			Body:      []byte(fmt.Sprintf("msg #%d", i)),
		})
	}
	sendResponse, err := c.SendQueueMessagesBatch(ctx, messagesList)
	require.NoError(t, err)
	require.False(t, sendResponse.HaveErrors)

	c2, err := NewQueueClient(getClientOptions(t, a), a.Queue)
	require.NoError(t, err)
	defer func() {
		_ = c2.Disconnect()
	}()
	response, err := c2.AckAllQueueMessages(ctx, &pb.AckAllQueueMessagesRequest{
		RequestID:            "",
		ClientID:             "some-client",
		Channel:              "some-queue-channel",
		WaitTimeSeconds:      1,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	require.NoError(t, err)
	assert.False(t, response.IsError)
	assert.Empty(t, response.Error)
	require.Equal(t, uint64(10), response.AffectedMessages)

	response, err = c2.AckAllQueueMessages(ctx, &pb.AckAllQueueMessagesRequest{
		RequestID:            "",
		ClientID:             "some-client",
		Channel:              "some-queue-channel",
		WaitTimeSeconds:      1,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	require.NoError(t, err)
	assert.False(t, response.IsError)
	assert.Empty(t, response.Error)
	require.Equal(t, uint64(0), response.AffectedMessages)

}
func TestQueueClient_AckAllQueueMessagesErrors(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	time.Sleep(1 * time.Second)

	tests := []struct {
		name string
		req  *pb.AckAllQueueMessagesRequest
		res  *pb.AckAllQueueMessagesResponse
	}{
		{
			name: "blank_request",
			req: &pb.AckAllQueueMessagesRequest{
				RequestID:       "some-request-id",
				ClientID:        "",
				Channel:         "",
				WaitTimeSeconds: 0,
			},
			res: &pb.AckAllQueueMessagesResponse{
				RequestID:        "some-request-id",
				AffectedMessages: 0,
				IsError:          true,
				Error:            entities.ErrInvalidClientID.Error(),
			},
		},
		{
			name: "bad_channel1",
			req: &pb.AckAllQueueMessagesRequest{
				RequestID:       "some-request-id",
				ClientID:        "some-client_it",
				Channel:         "",
				WaitTimeSeconds: 0,
			},
			res: &pb.AckAllQueueMessagesResponse{
				RequestID:        "some-request-id",
				AffectedMessages: 0,
				IsError:          true,
				Error:            entities.ErrInvalidQueueName.Error(),
			},
		},
		{
			name: "bad_channel2",
			req: &pb.AckAllQueueMessagesRequest{
				RequestID:       "some-request-id",
				ClientID:        "some-client_it",
				Channel:         "*",
				WaitTimeSeconds: 0,
			},
			res: &pb.AckAllQueueMessagesResponse{
				RequestID:        "some-request-id",
				AffectedMessages: 0,
				IsError:          true,
				Error:            entities.ErrInvalidQueueName.Error(),
			},
		},
		{
			name: "bad_channel3",
			req: &pb.AckAllQueueMessagesRequest{
				RequestID:       "some-request-id",
				ClientID:        "some-client_it",
				Channel:         ">",
				WaitTimeSeconds: 0,
			},
			res: &pb.AckAllQueueMessagesResponse{
				RequestID:        "some-request-id",
				AffectedMessages: 0,
				IsError:          true,
				Error:            entities.ErrInvalidQueueName.Error(),
			},
		},
		{
			name: "bad_wait_time1",
			req: &pb.AckAllQueueMessagesRequest{
				RequestID:       "some-request-id",
				ClientID:        "some-client_it",
				Channel:         "some-channel",
				WaitTimeSeconds: -1,
			},
			res: &pb.AckAllQueueMessagesResponse{
				RequestID:        "some-request-id",
				AffectedMessages: 0,
				IsError:          true,
				Error:            entities.ErrInvalidWaitTimeout.Error(),
			},
		},
		{
			name: "bad_wait_time2",
			req: &pb.AckAllQueueMessagesRequest{
				RequestID:       "some-request-id",
				ClientID:        "some-client_it",
				Channel:         "some-channel",
				WaitTimeSeconds: 10000000,
			},
			res: &pb.AckAllQueueMessagesResponse{
				RequestID:        "some-request-id",
				AffectedMessages: 0,
				IsError:          true,
				Error:            entities.ErrInvalidWaitTimeout.Error(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			o := getClientOptions(t, a)
			c, err := NewQueueClient(o, a.Queue)
			require.NoError(t, err)
			defer func() {
				_ = c.Disconnect()
			}()

			res, err := c.AckAllQueueMessages(ctx, tt.req)
			require.NoError(t, err)
			assert.EqualValues(t, tt.res, res)
		})
	}
}
func TestQueueClient_ReceiveQueueMessagesWithPeak(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	defer func() {
		_ = c.Disconnect()
	}()

	time.Sleep(1 * time.Second)
	messagesList := &pb.QueueMessagesBatchRequest{
		BatchID:  nuid.Next(),
		Messages: []*pb.QueueMessage{},
	}

	for i := 0; i < 10; i++ {
		messagesList.Messages = append(messagesList.Messages, &pb.QueueMessage{
			MessageID: nuid.Next(),
			ClientID:  "some-client-id",
			Channel:   "some-queue-channel",
			Metadata:  "",
			Body:      []byte(fmt.Sprintf("msg #%d", i)),
		})
	}
	sendResponse, err := c.SendQueueMessagesBatch(ctx, messagesList)
	require.NoError(t, err)
	require.False(t, sendResponse.HaveErrors)
	time.Sleep(time.Second)
	c2, err := NewQueueClient(getClientOptions(t, a), a.Queue)
	require.NoError(t, err)
	defer func() {
		_ = c2.Disconnect()
	}()

	response2, err := c2.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:            "some-request",
		ClientID:             "some-client-id",
		Channel:              "some-queue-channel",
		MaxNumberOfMessages:  1,
		WaitTimeSeconds:      1,
		IsPeak:               true,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	require.NoError(t, err)
	assert.False(t, response2.IsError)
	assert.Empty(t, response2.Error)
	require.Equal(t, 1, len(response2.Messages))
	require.Equal(t, []byte(fmt.Sprintf("msg #%d", 0)), response2.Messages[0].Body)

	response2, err = c2.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:            "some-request",
		ClientID:             "some-client-id",
		Channel:              "some-queue-channel",
		MaxNumberOfMessages:  1,
		WaitTimeSeconds:      1,
		IsPeak:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	require.NoError(t, err)
	assert.False(t, response2.IsError)
	assert.Empty(t, response2.Error)
	require.Equal(t, 1, len(response2.Messages))
	require.Equal(t, []byte(fmt.Sprintf("msg #%d", 0)), response2.Messages[0].Body)

	response2, err = c2.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:            "some-request",
		ClientID:             "some-client-id",
		Channel:              "some-queue-channel",
		MaxNumberOfMessages:  10,
		WaitTimeSeconds:      1,
		IsPeak:               true,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	require.NoError(t, err)
	assert.False(t, response2.IsError)
	assert.Empty(t, response2.Error)
	require.Equal(t, 9, len(response2.Messages))
	require.Equal(t, []byte(fmt.Sprintf("msg #%d", 1)), response2.Messages[0].Body)
}
func TestQueueClient_MonitorQueueMessages(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	defer func() {
		_ = c.Disconnect()
	}()

	time.Sleep(1 * time.Second)
	messagesList := &pb.QueueMessagesBatchRequest{
		BatchID:  nuid.Next(),
		Messages: []*pb.QueueMessage{},
	}

	counter := atomic.NewInt64(0)

	go func() {
		c2, err := NewQueueClient(getClientOptions(t, a), a.Queue)
		require.NoError(t, err)
		defer func() {
			_ = c2.Disconnect()
		}()
		msgCh := make(chan *pb.QueueMessage, 1)
		doneCh := make(chan bool, 1)
		errCh := make(chan error, 1)
		go c2.MonitorQueueMessages(ctx, "some-queue-channel", msgCh, errCh, doneCh)
		require.NoError(t, err)
		for {
			select {
			case <-msgCh:
				counter.Inc()
			case err := <-errCh:
				require.NoError(t, err)
			case <-doneCh:
				return
			case <-ctx.Done():

				return
			}
		}
	}()
	time.Sleep(time.Second)
	for i := 0; i < 10; i++ {
		messagesList.Messages = append(messagesList.Messages, &pb.QueueMessage{
			MessageID: nuid.Next(),
			ClientID:  "some-client-id",
			Channel:   "some-queue-channel",
			Metadata:  "",
			Body:      []byte(fmt.Sprintf("msg #%d", i)),
		})
	}
	sendResponse, err := c.SendQueueMessagesBatch(ctx, messagesList)
	require.NoError(t, err)
	require.False(t, sendResponse.HaveErrors)
	time.Sleep(1 * time.Second)

	cancel()
	require.Equal(t, int64(10), counter.Load())

}
func TestQueueClient_StreamQueueMessage_ChainedQueueWithPublishAndAck(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	doneQueue := "ack_done"
	startQueue := "ack_q1"
	tests := []struct {
		name       string
		queue      int
		msgCount   int
		msgSize    int
		visibility int32
		wait       int32
		receivers  int
	}{
		{
			name:       "50 messages 2 queues",
			queue:      2,
			msgCount:   50,
			msgSize:    100,
			visibility: 3,
			wait:       5,
			receivers:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
			require.NoError(t, err)
			err = sender.sendMessagesWithPolicy(ctx, tt.msgCount, startQueue, tt.msgSize, 0, 0, 1, "")
			require.NoError(t, err)

			wg := sync.WaitGroup{}
			wg.Add(tt.queue * tt.receivers)
			for i := 1; i <= tt.queue; i++ {
				time.Sleep(10 * time.Millisecond)
				rxQueue := ""
				txQueue := ""
				if i == tt.queue {
					rxQueue = fmt.Sprintf("ack_q%d", i)
					txQueue = doneQueue
				} else {
					rxQueue = fmt.Sprintf("ack_q%d", i)
					txQueue = fmt.Sprintf("ack_q%d", i+1)
				}

				for j := 0; j < tt.receivers; j++ {
					time.Sleep(1 * time.Millisecond)
					go func(rxQueue, txQueue string) {
						defer wg.Done()
						var err error
						var recevier *testQueueClient
						msgReceived := 0
						for {
							if recevier != nil {
								recevier.disconnect()
							}
							recevier, err = newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
							require.NoError(t, err, rxQueue, msgReceived)
							ctx, cancel := context.WithCancel(ctx)
							msg, expired, err := recevier.streamReceiveMessage(ctx, rxQueue, tt.visibility, tt.wait)
							require.NoError(t, err, rxQueue, msgReceived)
							if expired {
								cancel()
								recevier.disconnect()
								return
							}
							require.NotNil(t, msg)
							msgReceived++
							msg.Channel = txQueue
							msgSeq := msg.Attributes.Sequence
							res := recevier.c.SendQueueMessage(ctx, msg)
							require.False(t, res.IsError, rxQueue, msgReceived)
							err = recevier.streamAckMessage(ctx, msgSeq)
							require.NoError(t, err, rxQueue, msgReceived)
							err = recevier.streamGetDone(ctx)
							require.NoError(t, err, rxQueue, msgReceived)
							cancel()
						}
					}(rxQueue, txQueue)
				}

			}
			wg.Wait()
			if tt.receivers > 1 {
				err = sender.verifyQueueItems(ctx, int32(tt.msgCount), doneQueue, false)
			} else {
				err = sender.verifyQueueItems(ctx, int32(tt.msgCount), doneQueue, true)
			}
			require.NoError(t, err)
			sender.disconnect()
		})
	}

}
func TestQueueClient_StreamQueueMessage_ChainedQueueWithExtendVisibilityPublishAndAck(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	doneQueue := "visibility_done"
	startQueue := "visibility_q1"
	tests := []struct {
		name       string
		queue      int
		msgCount   int
		msgSize    int
		visibility int32
		wait       int32
		receivers  int
	}{
		{
			name:       "50 messages 2 queues",
			queue:      2,
			msgCount:   50,
			msgSize:    100,
			visibility: 3,
			wait:       5,
			receivers:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
			require.NoError(t, err)
			err = sender.sendMessagesWithPolicy(ctx, tt.msgCount, startQueue, tt.msgSize, 0, 0, 1, "")
			require.NoError(t, err)

			wg := sync.WaitGroup{}
			wg.Add(tt.queue * tt.receivers)
			for i := 1; i <= tt.queue; i++ {
				time.Sleep(10 * time.Millisecond)
				rxQueue := ""
				txQueue := ""
				if i == tt.queue {
					rxQueue = fmt.Sprintf("visibility_q%d", i)
					txQueue = doneQueue
				} else {
					rxQueue = fmt.Sprintf("visibility_q%d", i)
					txQueue = fmt.Sprintf("visibility_q%d", i+1)
				}

				for j := 0; j < tt.receivers; j++ {
					time.Sleep(1 * time.Millisecond)
					go func(rxQueue, txQueue string) {
						defer wg.Done()
						var err error
						var recevier *testQueueClient
						msgReceived := 0
						for {
							if recevier != nil {
								recevier.disconnect()
							}
							recevier, err = newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
							require.NoError(t, err, rxQueue, msgReceived)
							ctx, cancel := context.WithCancel(ctx)
							msg, expired, err := recevier.streamReceiveMessage(ctx, rxQueue, tt.visibility, tt.wait)
							require.NoError(t, err, rxQueue, msgReceived)
							if expired {
								cancel()
								recevier.disconnect()
								return
							}
							require.NotNil(t, msg)
							msgReceived++
							err = recevier.streamModifyVisibilityMessage(ctx, 5)
							require.NoError(t, err, rxQueue, msgReceived)
							msg.Channel = txQueue
							msgSeq := msg.Attributes.Sequence
							res := recevier.c.SendQueueMessage(ctx, msg)
							require.False(t, res.IsError, rxQueue, msgReceived)
							err = recevier.streamAckMessage(ctx, msgSeq)
							require.NoError(t, err, rxQueue, msgReceived)
							err = recevier.streamGetDone(ctx)
							require.NoError(t, err, rxQueue, msgReceived)
							cancel()
						}
					}(rxQueue, txQueue)
				}

			}
			wg.Wait()
			if tt.receivers > 1 {
				err = sender.verifyQueueItems(ctx, int32(tt.msgCount), doneQueue, false)
			} else {
				err = sender.verifyQueueItems(ctx, int32(tt.msgCount), doneQueue, true)
			}
			require.NoError(t, err)
			sender.disconnect()
		})
	}

}
func TestQueueClient_StreamQueueMessage_ChainedQueueWithResend(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	doneQueue := "resend_done"
	startQueue := "resend_q1"
	tests := []struct {
		name       string
		queue      int
		msgCount   int
		msgSize    int
		visibility int32
		wait       int32
		receivers  int
	}{
		{
			name:       "50 messages 2 queues",
			queue:      2,
			msgCount:   50,
			msgSize:    100,
			visibility: 3,
			wait:       5,
			receivers:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
			require.NoError(t, err)
			err = sender.sendMessagesWithPolicy(ctx, tt.msgCount, startQueue, tt.msgSize, 0, 0, 1, "")
			require.NoError(t, err)

			wg := sync.WaitGroup{}
			wg.Add(tt.queue * tt.receivers)
			for i := 1; i <= tt.queue; i++ {
				time.Sleep(10 * time.Millisecond)
				rxQueue := ""
				txQueue := ""
				if i == tt.queue {
					rxQueue = fmt.Sprintf("resend_q%d", i)
					txQueue = doneQueue
				} else {
					rxQueue = fmt.Sprintf("resend_q%d", i)
					txQueue = fmt.Sprintf("resend_q%d", i+1)
				}

				for j := 0; j < tt.receivers; j++ {
					time.Sleep(1 * time.Millisecond)
					go func(rxQueue, txQueue string) {
						defer wg.Done()
						var err error
						var recevier *testQueueClient
						msgReceived := 0
						for {
							if recevier != nil {
								recevier.disconnect()
							}
							recevier, err = newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
							require.NoError(t, err, rxQueue, msgReceived)
							ctx, cancel := context.WithCancel(ctx)
							msg, expired, err := recevier.streamReceiveMessage(ctx, rxQueue, tt.visibility, tt.wait)
							require.NoError(t, err, rxQueue, msgReceived)
							if expired {
								cancel()
								recevier.disconnect()
								return
							}
							require.NotNil(t, msg)
							msgReceived++
							err = recevier.streamModifyResendMessage(ctx, msg.Attributes.Sequence, txQueue)
							require.NoError(t, err, rxQueue, msgReceived)
							err = recevier.streamGetDone(ctx)
							require.NoError(t, err, rxQueue, msgReceived)
							cancel()
						}
					}(rxQueue, txQueue)
				}

			}
			wg.Wait()
			if tt.receivers > 1 {
				err = sender.verifyQueueItems(ctx, int32(tt.msgCount), doneQueue, false)
			} else {
				err = sender.verifyQueueItems(ctx, int32(tt.msgCount), doneQueue, true)
			}
			require.NoError(t, err)
			sender.disconnect()
		})
	}

}
func TestQueueClient_StreamQueueMessage_ChainedQueueWithModifyMessage(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	doneQueue := "modify_done"
	startQueue := "modify_q1"
	tests := []struct {
		name       string
		queue      int
		msgCount   int
		msgSize    int
		visibility int32
		wait       int32
		receivers  int
	}{
		{
			name:       "50 messages 2 queues",
			queue:      2,
			msgCount:   50,
			msgSize:    100,
			visibility: 3,
			wait:       5,
			receivers:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
			require.NoError(t, err)
			err = sender.sendMessagesWithPolicy(ctx, tt.msgCount, startQueue, tt.msgSize, 0, 0, 1, "")
			require.NoError(t, err)

			wg := sync.WaitGroup{}
			wg.Add(tt.queue * tt.receivers)
			for i := 1; i <= tt.queue; i++ {
				time.Sleep(10 * time.Millisecond)
				rxQueue := ""
				txQueue := ""
				if i == tt.queue {
					rxQueue = fmt.Sprintf("modify_q%d", i)
					txQueue = doneQueue
				} else {
					rxQueue = fmt.Sprintf("modify_q%d", i)
					txQueue = fmt.Sprintf("modify_q%d", i+1)
				}

				for j := 0; j < tt.receivers; j++ {
					time.Sleep(1 * time.Millisecond)
					go func(rxQueue, txQueue string) {
						defer wg.Done()
						var err error
						var recevier *testQueueClient
						msgReceived := 0
						for {
							if recevier != nil {
								recevier.disconnect()
							}
							recevier, err = newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
							require.NoError(t, err, rxQueue, msgReceived)
							ctx, cancel := context.WithCancel(ctx)
							msg, expired, err := recevier.streamReceiveMessage(ctx, rxQueue, tt.visibility, tt.wait)
							require.NoError(t, err, rxQueue, msgReceived)
							if expired {
								cancel()
								recevier.disconnect()
								return
							}
							require.NotNil(t, msg)
							msgReceived++
							msg.Channel = txQueue
							err = recevier.streamModifyMessage(ctx, msg)
							require.NoError(t, err, rxQueue, msgReceived)
							err = recevier.streamGetDone(ctx)
							require.NoError(t, err, rxQueue, msgReceived)
							cancel()
						}
					}(rxQueue, txQueue)
				}

			}
			wg.Wait()
			if tt.receivers > 1 {
				err = sender.verifyQueueItems(ctx, int32(tt.msgCount), doneQueue, false)
			} else {
				err = sender.verifyQueueItems(ctx, int32(tt.msgCount), doneQueue, true)
			}
			require.NoError(t, err)
			sender.disconnect()
		})
	}

}
func TestQueueClient_StreamQueueMessage_ChainedQueueWithDelay(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	doneQueue := "delay_done"
	startQueue := "delay_q1"
	tests := []struct {
		name       string
		queue      int
		msgCount   int
		msgSize    int
		visibility int32
		wait       int32
		receivers  int
		delay      int32
	}{
		{
			name:       "50 messages 2 queues, 1 sec delay",
			queue:      2,
			msgCount:   50,
			msgSize:    100,
			visibility: 3,
			wait:       5,
			receivers:  1,
			delay:      1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)

			require.NoError(t, err)
			sender.c.SetDelayMessagesProcessor(ctx)
			err = sender.sendMessagesWithPolicy(ctx, tt.msgCount, startQueue, tt.msgSize, 0, tt.delay, 1, "")

			require.NoError(t, err)

			wg := sync.WaitGroup{}
			wg.Add(tt.queue * tt.receivers)
			for i := 1; i <= tt.queue; i++ {
				time.Sleep(10 * time.Millisecond)
				rxQueue := ""
				txQueue := ""
				if i == tt.queue {
					rxQueue = fmt.Sprintf("delay_q%d", i)
					txQueue = doneQueue
				} else {
					rxQueue = fmt.Sprintf("delay_q%d", i)
					txQueue = fmt.Sprintf("delay_q%d", i+1)
				}

				for j := 0; j < tt.receivers; j++ {
					time.Sleep(1 * time.Millisecond)
					go func(rxQueue, txQueue string) {
						defer wg.Done()
						var err error
						var recevier *testQueueClient
						msgReceived := 0
						for {
							if recevier != nil {
								recevier.disconnect()
							}
							recevier, err = newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
							require.NoError(t, err, rxQueue, msgReceived)
							ctx, cancel := context.WithCancel(ctx)
							msg, expired, err := recevier.streamReceiveMessage(ctx, rxQueue, tt.visibility, tt.wait)
							require.NoError(t, err, rxQueue, msgReceived)
							if expired {
								cancel()
								recevier.disconnect()
								return
							}
							require.NotNil(t, msg)
							msgReceived++
							msg.Channel = txQueue
							msgSeq := msg.Attributes.Sequence
							res := recevier.c.SendQueueMessage(ctx, msg)
							require.False(t, res.IsError, rxQueue, msgReceived)
							err = recevier.streamAckMessage(ctx, msgSeq)
							require.NoError(t, err, rxQueue, msgReceived)
							err = recevier.streamGetDone(ctx)
							require.NoError(t, err, rxQueue, msgReceived)
							cancel()
						}
					}(rxQueue, txQueue)
				}

			}
			wg.Wait()
			if tt.receivers > 1 {
				err = sender.verifyQueueItems(ctx, int32(tt.msgCount), doneQueue, false)
			} else {
				err = sender.verifyQueueItems(ctx, int32(tt.msgCount), doneQueue, true)
			}
			require.NoError(t, err)
			sender.disconnect()
		})
	}

}
func TestQueueClient_StreamQueueMessage_ChainedQueueWithDeadLetter(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	doneQueue := "dead_letter_done"
	startQueue := "dead_letter_q1"
	tests := []struct {
		name       string
		queue      int
		msgCount   int
		msgSize    int
		visibility int32
		wait       int32
		receivers  int
		maxCnt     int32
	}{
		{
			name:       "50 messages 1 queue 1 max count",
			queue:      1,
			msgCount:   50,
			msgSize:    100,
			visibility: 3,
			wait:       5,
			receivers:  1,
			maxCnt:     1,
		},
		{
			name:       "50 messages 1 queue 10 max count",
			queue:      1,
			msgCount:   50,
			msgSize:    100,
			visibility: 3,
			wait:       5,
			receivers:  1,
			maxCnt:     10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)

			require.NoError(t, err)
			sender.c.SetDelayMessagesProcessor(ctx)
			err = sender.sendMessagesWithPolicy(ctx, tt.msgCount, startQueue, tt.msgSize, 0, 0, tt.maxCnt, doneQueue)

			require.NoError(t, err)

			wg := sync.WaitGroup{}
			wg.Add(tt.queue * tt.receivers)
			for i := 1; i <= tt.queue; i++ {
				time.Sleep(10 * time.Millisecond)
				rxQueue := ""
				txQueue := ""
				if i == tt.queue {
					rxQueue = fmt.Sprintf("dead_letter_q%d", i)
					txQueue = doneQueue
				} else {
					rxQueue = fmt.Sprintf("dead_letter_q%d", i)
					txQueue = fmt.Sprintf("dead_letter_q%d", i+1)
				}

				for j := 0; j < tt.receivers; j++ {
					time.Sleep(1 * time.Millisecond)
					go func(rxQueue, txQueue string) {
						defer wg.Done()
						var err error
						var recevier *testQueueClient
						msgReceived := 0
						for {
							if recevier != nil {
								recevier.disconnect()
							}
							recevier, err = newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
							require.NoError(t, err, rxQueue, msgReceived)
							ctx, cancel := context.WithCancel(ctx)
							msg, expired, err := recevier.streamReceiveMessage(ctx, rxQueue, tt.visibility, tt.wait)
							require.NoError(t, err, rxQueue, msgReceived)
							if expired {
								cancel()
								recevier.disconnect()
								return
							}
							require.NotNil(t, msg)
							msgReceived++
							err = recevier.streamRejectMessage(ctx, msg.Attributes.Sequence)
							require.NoError(t, err, rxQueue, msgReceived)
							cancel()
						}
					}(rxQueue, txQueue)
				}

			}
			wg.Wait()
			if tt.receivers > 1 {
				err = sender.verifyQueueItems(ctx, int32(tt.msgCount), doneQueue, false)
			} else {
				err = sender.verifyQueueItems(ctx, int32(tt.msgCount), doneQueue, true)
			}
			require.NoError(t, err)
			sender.disconnect()
		})
	}

}
