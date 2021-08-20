package array

import (
	"context"
	"fmt"
	"github.com/fortytw2/leaktest"
	"github.com/kubemq-io/kubemq-community/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func prepareQueueMessagesBatch(channel, clientId string, size int) *pb.QueueMessagesBatchRequest {
	b := &pb.QueueMessagesBatchRequest{
		BatchID: uuid.New(),
	}
	for i := 0; i < size; i++ {
		b.Messages = append(b.Messages, &pb.QueueMessage{
			MessageID:  uuid.New(),
			ClientID:   clientId,
			Channel:    channel,
			Metadata:   "",
			Body:       []byte("some-body"),
			Tags:       nil,
			Attributes: nil,
			Policy:     nil,
		})
	}
	return b
}
func preparePollGetRequest(channel, clientId string, maxItems int32, waitTimout int32, autoAck bool) *pb.QueuesDownstreamRequest {
	return &pb.QueuesDownstreamRequest{
		RequestID:        uuid.New(),
		ClientID:         clientId,
		RequestTypeData:  pb.QueuesDownstreamRequestType_Get,
		Channel:          channel,
		MaxItems:         maxItems,
		WaitTimeout:      waitTimout,
		AutoAck:          autoAck,
		SequenceRange:    nil,
		RefTransactionId: "",
		Metadata:         nil,
	}
}
func preparePollNackAllRequest(transactionId string, clientId string) *pb.QueuesDownstreamRequest {
	return &pb.QueuesDownstreamRequest{
		RequestID:            uuid.New(),
		ClientID:             clientId,
		RequestTypeData:      pb.QueuesDownstreamRequestType_NAckAll,
		Channel:              "",
		MaxItems:             0,
		WaitTimeout:          0,
		AutoAck:              false,
		ReQueueChannel:       "",
		SequenceRange:        nil,
		RefTransactionId:     transactionId,
		Metadata:             nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
}
func preparePollNackRangeRequest(transactionId string, clientId string, seq []int64) *pb.QueuesDownstreamRequest {
	return &pb.QueuesDownstreamRequest{
		RequestID:            uuid.New(),
		ClientID:             clientId,
		RequestTypeData:      pb.QueuesDownstreamRequestType_NAckRange,
		Channel:              "",
		MaxItems:             0,
		WaitTimeout:          0,
		AutoAck:              false,
		ReQueueChannel:       "",
		SequenceRange:        seq,
		RefTransactionId:     transactionId,
		Metadata:             nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
}
func preparePollReQueueAllRequest(transactionId string, clientId string, newChannel string) *pb.QueuesDownstreamRequest {
	return &pb.QueuesDownstreamRequest{
		RequestID:            uuid.New(),
		ClientID:             clientId,
		RequestTypeData:      pb.QueuesDownstreamRequestType_ReQueueAll,
		Channel:              "",
		MaxItems:             0,
		WaitTimeout:          0,
		AutoAck:              false,
		ReQueueChannel:       newChannel,
		SequenceRange:        nil,
		RefTransactionId:     transactionId,
		Metadata:             nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
}
func preparePollActiveOffsets(transactionId string, clientId string) *pb.QueuesDownstreamRequest {
	return &pb.QueuesDownstreamRequest{
		RequestID:            uuid.New(),
		ClientID:             clientId,
		RequestTypeData:      pb.QueuesDownstreamRequestType_ActiveOffsets,
		Channel:              "",
		MaxItems:             0,
		WaitTimeout:          0,
		AutoAck:              false,
		ReQueueChannel:       "",
		SequenceRange:        nil,
		RefTransactionId:     transactionId,
		Metadata:             nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
}
func preparePollReQueueAllRange(transactionId string, clientId string, newChannel string, seq []int64) *pb.QueuesDownstreamRequest {
	return &pb.QueuesDownstreamRequest{
		RequestID:            uuid.New(),
		ClientID:             clientId,
		RequestTypeData:      pb.QueuesDownstreamRequestType_ReQueueRange,
		Channel:              "",
		MaxItems:             0,
		WaitTimeout:          0,
		AutoAck:              false,
		ReQueueChannel:       newChannel,
		SequenceRange:        seq,
		RefTransactionId:     transactionId,
		Metadata:             nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
}
func preparePollAckAllRequest(transactionId string, clientId string) *pb.QueuesDownstreamRequest {
	return &pb.QueuesDownstreamRequest{
		RequestID:        uuid.New(),
		ClientID:         clientId,
		RequestTypeData:  pb.QueuesDownstreamRequestType_AckAll,
		RefTransactionId: transactionId,
		Metadata:         nil,
	}
}
func preparePollAckRangeRequest(transactionId string, clientId string, ackRange []int64) *pb.QueuesDownstreamRequest {
	return &pb.QueuesDownstreamRequest{
		RequestID:        uuid.New(),
		ClientID:         clientId,
		RequestTypeData:  pb.QueuesDownstreamRequestType_AckRange,
		SequenceRange:    ackRange,
		RefTransactionId: transactionId,
	}
}
func preparePollCloseRangeRequest(transactionId string, clientId string) *pb.QueuesDownstreamRequest {
	return &pb.QueuesDownstreamRequest{
		RequestID:        uuid.New(),
		ClientID:         clientId,
		RequestTypeData:  pb.QueuesDownstreamRequestType_CloseByClient,
		RefTransactionId: transactionId,
	}
}
func waitForResponseValue(value int, responseCh chan *pb.QueuesDownstreamResponse) (string, error) {
	select {
	case resp := <-responseCh:
		if resp.TransactionComplete {
			return "", fmt.Errorf("transaction completed")
		}
		if len(resp.Messages) != value {
			return "", fmt.Errorf("value %d is not equal to num of messages %d", value, len(resp.Messages))
		}
		if resp.IsError {
			return "", fmt.Errorf(resp.Error)
		}
		return resp.TransactionId, nil
	case <-time.After(5 * time.Second):
		return "", fmt.Errorf("response timeout")
	}
}
func waitForResponseObject(responseCh chan *pb.QueuesDownstreamResponse) (*pb.QueuesDownstreamResponse, error) {
	select {
	case resp := <-responseCh:
		return resp, nil
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("response timeout")
	}
}

func TestArray_QueuePoll_AutoAck_OneRequest(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, true)
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)

	doneCh <- true

}
func TestArray_QueuePoll_AutoAck_TwoRequests_In_Sequence(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, true)
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	request = preparePollGetRequest(channel, clientId, 10, 2000, true)
	requestsCh <- request
	_, err = waitForResponseValue(0, responseCh)
	require.NoError(t, err)
	doneCh <- true
}

func TestArray_QueuePoll_ManualAck_OneRequest_AckAll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	transactionId, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	requestsCh <- preparePollAckAllRequest(transactionId, clientId)

	request2 := preparePollGetRequest(channel, clientId, 10, 1000, false)
	requestsCh <- request2
	_, err = waitForResponseValue(0, responseCh)
	require.NoError(t, err)

	doneCh <- true

}
func TestArray_QueuePoll_ManualAck_OneRequest_AckRange(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	refTransaction, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	for i := 1; i <= 10; i++ {
		requestsCh <- preparePollAckRangeRequest(refTransaction, clientId, []int64{int64(i)})
	}

	request2 := preparePollGetRequest(channel, clientId, 10, 1000, false)
	requestsCh <- request2
	_, err = waitForResponseValue(0, responseCh)
	require.NoError(t, err)

	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_Close_AckAll_OneRequest(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, int32(sendSize), 2000, false)
	requestsCh <- request
	refTransaction, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	requestsCh <- preparePollCloseRangeRequest(refTransaction, clientId)

	doneCh <- true

}
func TestArray_QueuePoll_ManualAck_Done_AckAll_OneRequest(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	doneCh <- true

	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request2 := preparePollGetRequest(channel, clientId, 10, 2000, true)
	requestsCh <- request2
	_, err = waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)

	doneCh <- true

}

func TestArray_QueuePoll_BadRequest_BadRequestTypeError(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)

	requestsCh <- &pb.QueuesDownstreamRequest{
		RequestID:        "",
		ClientID:         "",
		RequestTypeData:  0,
		Channel:          "",
		MaxItems:         0,
		WaitTimeout:      0,
		AutoAck:          false,
		SequenceRange:    nil,
		RefTransactionId: "",
		Metadata:         nil,
	}
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)

	doneCh <- true

}
func TestArray_QueuePoll_BadRequest_NoChannelError(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)

	requestsCh <- &pb.QueuesDownstreamRequest{
		RequestID:        "",
		ClientID:         "",
		RequestTypeData:  1,
		Channel:          "",
		MaxItems:         0,
		WaitTimeout:      0,
		AutoAck:          false,
		SequenceRange:    nil,
		RefTransactionId: "",
		Metadata:         nil,
	}
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)

	doneCh <- true

}

func TestArray_QueuePoll_ManualAck_AckAll_InvalidRefId(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	requestsCh <- preparePollAckAllRequest("badref", clientId)
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)
	doneCh <- true

}
func TestArray_QueuePoll_ManualAck_AckRange_InvalidRefId(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	requestsCh <- preparePollAckRangeRequest("badref", clientId, []int64{1})
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)
	doneCh <- true

}
func TestArray_QueuePoll_ManualAck_NackAll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, int32(sendSize), 2000, false)
	requestsCh <- request
	refTransaction, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	request = preparePollNackAllRequest(refTransaction, clientId)
	requestsCh <- request

	request2 := preparePollGetRequest(channel, clientId, int32(sendSize), 1000, true)
	requestsCh <- request2
	_, err = waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)

	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_NackAll_Bad_Ref(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	requestsCh <- preparePollNackAllRequest("badref", clientId)
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)

	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_NackRange_OneByOne(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 1000
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 10)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 10)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, int32(sendSize), 2000, false)
	requestsCh <- request
	response, err := waitForResponseObject(responseCh)
	require.NoError(t, err)
	for _, msg := range response.Messages {
		request = preparePollNackRangeRequest(response.TransactionId, clientId, []int64{int64(msg.Attributes.Sequence)})
		requestsCh <- request
	}
	request2 := preparePollGetRequest(channel, clientId, int32(sendSize), 1000, false)
	requestsCh <- request2
	_, err = waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)

	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_NackRange_Bad_Ref(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	requestsCh <- preparePollNackRangeRequest("badref", clientId, []int64{1})
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)
	doneCh <- true
}

func TestArray_QueuePoll_ManualAck_ReQueueAll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	refTransaction, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)

	request = preparePollReQueueAllRequest(refTransaction, clientId, "new_channel")
	requestsCh <- request

	request2 := preparePollGetRequest("new_channel", clientId, 10, 10000, false)
	requestsCh <- request2
	_, err = waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)

	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_ReQueueBadRef(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)

	request = preparePollReQueueAllRequest("badref", clientId, "new_channel")
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)
	doneCh <- true
}

func TestArray_QueuePoll_ManualAck_ReQueueRange(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	refTransaction, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)

	request = preparePollReQueueAllRange(refTransaction, clientId, "new_channel_1", []int64{1, 2, 3, 4, 5})
	requestsCh <- request

	request = preparePollReQueueAllRange(refTransaction, clientId, "new_channel_2", []int64{6, 7, 8, 9, 10})
	requestsCh <- request

	request2 := preparePollGetRequest("new_channel_1", clientId, 5, 1000, true)
	requestsCh <- request2
	_, err = waitForResponseValue(5, responseCh)
	require.NoError(t, err)

	request3 := preparePollGetRequest("new_channel_2", clientId, 5, 1000, true)
	requestsCh <- request3
	_, err = waitForResponseValue(5, responseCh)
	require.NoError(t, err)

	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_ReQueueRange_BadRef(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)
	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)

	request = preparePollReQueueAllRange("bad-ref", clientId, "new_channel_1", []int64{1, 2, 3, 4, 5})
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)
	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_ActiveOffsets(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	refTransaction, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	request = preparePollActiveOffsets(refTransaction, clientId)
	requestsCh <- request
	response, err := waitForResponseObject(responseCh)
	require.NoError(t, err)
	require.EqualValues(t, sendSize, len(response.ActiveOffsets))
	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_AckRangeError(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	refTransaction, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	request = preparePollAckRangeRequest(refTransaction, clientId, []int64{-1})
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)

	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_AckRangeError_2(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	refTransaction, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	request = preparePollAckRangeRequest(refTransaction, clientId, []int64{})
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)

	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_ReQueue_Bad_Channel_Error(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	refTransaction, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	request = preparePollReQueueAllRequest(refTransaction, clientId, "")
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)

	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_ReQueueRange_Bad_Channel_Error(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	refTransaction, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	request = preparePollReQueueAllRange(refTransaction, clientId, "", []int64{1})
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)

	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_ReQueueRange_EmptyList_Error(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	refTransaction, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	request = preparePollReQueueAllRange(refTransaction, clientId, "", []int64{})
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)

	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_ReQueueRange_Invalid_Seq_Error(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	refTransaction, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	request = preparePollReQueueAllRange(refTransaction, clientId, "", []int64{-1})
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)

	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_NAckRange_Invalid_Seq_Error(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	refTransaction, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	request = preparePollNackRangeRequest(refTransaction, clientId, []int64{-1})
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)

	doneCh <- true
}
func TestArray_QueuePoll_ManualAck_NAckRange_Empty_Seq_List_Error(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	responseCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)

	batch := prepareQueueMessagesBatch(channel, clientId, sendSize)
	resp, err := na.SendQueueMessagesBatch(ctx, batch)

	require.NoError(t, err)
	require.False(t, resp.HaveErrors)
	_, err = na.queuesDownstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := preparePollGetRequest(channel, clientId, 10, 2000, false)
	requestsCh <- request
	refTransaction, err := waitForResponseValue(sendSize, responseCh)
	require.NoError(t, err)
	request = preparePollNackRangeRequest(refTransaction, clientId, []int64{})
	requestsCh <- request
	_, err = waitForResponseValue(sendSize, responseCh)
	require.Error(t, err)

	doneCh <- true
}
