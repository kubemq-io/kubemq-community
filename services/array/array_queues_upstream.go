package array

import (
	"context"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	pb "github.com/kubemq-io/protobuf/go"
)

type arrayQueuesUpstreamHandler struct {
	sendQueueMessagesBatchFunc func(ctx context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error)
	responsesCh                chan *pb.QueuesUpstreamResponse
	requestsCh                 chan *pb.QueuesUpstreamRequest
	done                       chan bool
}

func newArrayQueuesUpstreamHandler() *arrayQueuesUpstreamHandler {
	a := &arrayQueuesUpstreamHandler{}
	return a
}
func (a *arrayQueuesUpstreamHandler) SetSendQueueMessagesBatchFunc(sendQueueMessagesBatchFunc func(ctx context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error)) *arrayQueuesUpstreamHandler {
	a.sendQueueMessagesBatchFunc = sendQueueMessagesBatchFunc
	return a
}

func (a *arrayQueuesUpstreamHandler) SetResponsesCh(responsesCh chan *pb.QueuesUpstreamResponse) *arrayQueuesUpstreamHandler {
	a.responsesCh = responsesCh
	return a
}

func (a *arrayQueuesUpstreamHandler) SetRequestsCh(requestsCh chan *pb.QueuesUpstreamRequest) *arrayQueuesUpstreamHandler {
	a.requestsCh = requestsCh
	return a
}

func (a *arrayQueuesUpstreamHandler) SetDone(done chan bool) *arrayQueuesUpstreamHandler {
	a.done = done
	return a
}
func (a *arrayQueuesUpstreamHandler) requestTask(ctx context.Context, request *pb.QueuesUpstreamRequest) {
	resp, err := a.sendQueueMessagesBatchFunc(ctx, &pb.QueueMessagesBatchRequest{
		BatchID:  request.RequestID,
		Messages: request.Messages,
	})
	if err != nil {
		a.responsesCh <- &pb.QueuesUpstreamResponse{
			RefRequestID: request.RequestID,
			Results:      nil,
			IsError:      true,
			Error:        err.Error(),
		}
	} else {
		a.responsesCh <- &pb.QueuesUpstreamResponse{
			RefRequestID: request.RequestID,
			Results:      resp.Results,
			IsError:      false,
			Error:        "",
		}
	}
}
func (a *arrayQueuesUpstreamHandler) start(ctx context.Context) {
	for {
		select {
		case newRequest := <-a.requestsCh:
			go a.requestTask(ctx, newRequest)
		case <-a.done:
			return
		case <-ctx.Done():
			return
		}
	}
}
func (a *Array) QueuesUpstream(ctx context.Context, requests chan *pb.QueuesUpstreamRequest, response chan *pb.QueuesUpstreamResponse, done chan bool) error {
	_, err := a.queuesUpstream(ctx, requests, response, done)
	return err
}
func (a *Array) queuesUpstream(ctx context.Context, requests chan *pb.QueuesUpstreamRequest, response chan *pb.QueuesUpstreamResponse, done chan bool) (*arrayQueuesUpstreamHandler, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	handler := newArrayQueuesUpstreamHandler().
		SetSendQueueMessagesBatchFunc(a.SendQueueMessagesBatch).
		SetRequestsCh(requests).
		SetResponsesCh(response).
		SetDone(done)
	go handler.start(ctx)
	return handler, nil
}
