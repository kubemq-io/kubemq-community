package array

import (
	"context"
	"github.com/fortytw2/leaktest"
	"github.com/kubemq-io/kubemq-community/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/stretchr/testify/require"
	"testing"
)

func prepareQueueUpstreamRequest(channel, clientId string, size int) *pb.QueuesUpstreamRequest {
	b := &pb.QueuesUpstreamRequest{
		RequestID: uuid.New(),
		Messages:  nil,
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
func Test_ArrayQueuesUpstreamHandler(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesUpstreamRequest, 1)
	responseCh := make(chan *pb.QueuesUpstreamResponse, 1)
	doneCh := make(chan bool, 1)
	err := na.QueuesUpstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := prepareQueueUpstreamRequest(channel, clientId, sendSize)
	requestsCh <- request
	resp := <-responseCh
	require.EqualValues(t, request.RequestID, resp.RefRequestID)
	require.False(t, resp.IsError)
	require.EqualValues(t, sendSize, len(resp.Results))
	doneCh <- true
}
func Test_ArrayQueuesUpstreamHandler_BadChannel_Error(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	//channel := uuid.New()
	clientId := uuid.New()
	sendSize := 10
	requestsCh := make(chan *pb.QueuesUpstreamRequest, 1)
	responseCh := make(chan *pb.QueuesUpstreamResponse, 1)
	doneCh := make(chan bool, 1)
	err := na.QueuesUpstream(ctx, requestsCh, responseCh, doneCh)
	require.NoError(t, err)
	request := prepareQueueUpstreamRequest("", clientId, sendSize)
	requestsCh <- request
	resp := <-responseCh
	require.EqualValues(t, request.RequestID, resp.RefRequestID)
	require.False(t, resp.IsError)
	for _, result := range resp.Results {
		require.True(t, result.IsError)
	}
	doneCh <- true
}
