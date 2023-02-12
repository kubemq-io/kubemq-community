package array

import (
	"context"
	"github.com/kubemq-io/kubemq-community/pkg/client"
	"github.com/kubemq-io/kubemq-community/pkg/cmap"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"github.com/kubemq-io/kubemq-community/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
)

type queueDownstreamTransaction struct {
	transactionId   string
	request         *client.QueueDownstreamRequest
	response        *client.QueueDownstreamResponse
	responseChannel chan *pb.QueuesDownstreamResponse
	queueClient     *client.QueueClient
	queueClientId   string
}

func (q *queueDownstreamTransaction) SetResponse(response *client.QueueDownstreamResponse) *queueDownstreamTransaction {
	q.response = response
	return q
}
func newQueueDownstreamTransaction() *queueDownstreamTransaction {
	p := &queueDownstreamTransaction{
		transactionId:   uuid.New(),
		request:         nil,
		responseChannel: nil,
		queueClient:     nil,
	}
	return p
}
func (q *queueDownstreamTransaction) SetQueueClientId(queueClientId string) *queueDownstreamTransaction {
	q.queueClientId = queueClientId
	return q
}

func (q *queueDownstreamTransaction) SetRequest(request *client.QueueDownstreamRequest) *queueDownstreamTransaction {
	q.request = request
	return q
}

func (q *queueDownstreamTransaction) SetResponseChannel(responseChannel chan *pb.QueuesDownstreamResponse) *queueDownstreamTransaction {
	q.responseChannel = responseChannel
	return q
}

func (q *queueDownstreamTransaction) SetQueueClient(queueClient *client.QueueClient) *queueDownstreamTransaction {
	q.queueClient = queueClient
	return q
}
func (q *queueDownstreamTransaction) poll(ctx context.Context) (*client.QueueDownstreamResponse, error) {
	return q.queueClient.Poll(ctx, q.request)
}

type arrayQueuesDownstreamHandler struct {
	transactions         cmap.ConcurrentMap
	getClientHandler     func() (*client.QueueClient, string, error)
	releaseClientHandler func(id string)
	authMiddlewareFunc   func(qc *client.QueueClient, req *pb.QueuesDownstreamRequest) error
	responsesCh          chan *pb.QueuesDownstreamResponse
	requestsCh           chan *pb.QueuesDownstreamRequest
	done                 chan bool
	ctx                  context.Context
	ctxCancel            context.CancelFunc
}

func (a *arrayQueuesDownstreamHandler) SetAuthMiddlewareFunc(authMiddlewareFunc func(qc *client.QueueClient, req *pb.QueuesDownstreamRequest) error) *arrayQueuesDownstreamHandler {
	a.authMiddlewareFunc = authMiddlewareFunc
	return a
}

func newArrayQueuesDownstreamHandler() *arrayQueuesDownstreamHandler {

	a := &arrayQueuesDownstreamHandler{
		transactions: cmap.New(),
	}
	a.ctx, a.ctxCancel = context.WithCancel(context.Background())
	return a
}

//func (a *arrayQueuesDownstreamHandler) onCompleteHandler(response *client.QueueDownstreamResponse) {
//	fmt.Println("pop")
//	_, ok := a.transactions.Pop(response.TransactionId())
//	if ok {
//		a.responsesCh <- &pb.QueuesDownstreamResponse{
//			TransactionId:       response.TransactionId(),
//			RefRequestId:        response.RefRequestId,
//			RequestTypeData:     pb.QueuesDownstreamRequestType_TransactionStatus,
//			Messages:            nil,
//			ActiveOffsets:       nil,
//			IsError:             false,
//			Error:               "",
//			TransactionComplete: true,
//		}
//	}
//}

func (a *arrayQueuesDownstreamHandler) SetDone(done chan bool) *arrayQueuesDownstreamHandler {
	a.done = done
	return a
}

func (a *arrayQueuesDownstreamHandler) SetGetClientHandler(getClientHandler func() (*client.QueueClient, string, error)) *arrayQueuesDownstreamHandler {
	a.getClientHandler = getClientHandler
	return a
}

func (a *arrayQueuesDownstreamHandler) SetReleaseClientHandler(releaseClientHandler func(id string)) *arrayQueuesDownstreamHandler {
	a.releaseClientHandler = releaseClientHandler
	return a
}

func (a *arrayQueuesDownstreamHandler) SetResponsesCh(responsesCh chan *pb.QueuesDownstreamResponse) *arrayQueuesDownstreamHandler {
	a.responsesCh = responsesCh
	return a
}

func (a *arrayQueuesDownstreamHandler) SetRequestsCh(requestsCh chan *pb.QueuesDownstreamRequest) *arrayQueuesDownstreamHandler {
	a.requestsCh = requestsCh
	return a
}
func (a *arrayQueuesDownstreamHandler) createNewTransaction(ctx context.Context, request *pb.QueuesDownstreamRequest) {
	t := newQueueDownstreamTransaction()
	queueClient, id, err := a.getClientHandler()
	if err != nil {
		a.responsesCh <- &pb.QueuesDownstreamResponse{
			TransactionId:   t.transactionId,
			RefRequestId:    request.RequestID,
			RequestTypeData: pb.QueuesDownstreamRequestType_Get,
			Messages:        nil,
			IsError:         true,
			Error:           err.Error(),
		}
		return
	}

	queueClient.SetQueuesDownstreamMiddlewareFunc(a.authMiddlewareFunc)
	t.SetQueueClient(queueClient)
	t.SetQueueClientId(id)
	t.SetResponseChannel(a.responsesCh)
	clientRequest := client.NewPollRequest(request, t.transactionId)
	t.SetRequest(clientRequest)
	resp, err := t.poll(ctx)
	if err != nil {
		a.responsesCh <- &pb.QueuesDownstreamResponse{
			TransactionId:   t.transactionId,
			RefRequestId:    request.RequestID,
			RequestTypeData: pb.QueuesDownstreamRequestType_Get,
			Messages:        nil,
			IsError:         true,
			Error:           err.Error(),
		}
		return
	}
	t.SetResponse(resp)
	a.transactions.Set(t.transactionId, t)
	a.responsesCh <- resp.QueuesDownstreamResponse
}
func (a *arrayQueuesDownstreamHandler) ackAllTransaction(t *queueDownstreamTransaction, requestId string) {

	err := t.response.AckAll()
	if err != nil {
		a.responsesCh <- &pb.QueuesDownstreamResponse{
			TransactionId:   t.transactionId,
			RefRequestId:    requestId,
			RequestTypeData: pb.QueuesDownstreamRequestType_AckAll,
			Messages:        nil,
			IsError:         true,
			Error:           err.Error(),
		}
	}

}
func (a *arrayQueuesDownstreamHandler) nackAllTransaction(t *queueDownstreamTransaction, requestId string) {
	err := t.response.NackAll()
	if err != nil {
		a.responsesCh <- &pb.QueuesDownstreamResponse{
			TransactionId:   t.transactionId,
			RefRequestId:    requestId,
			RequestTypeData: pb.QueuesDownstreamRequestType_NAckAll,
			Messages:        nil,
			IsError:         true,
			Error:           err.Error(),
		}

	}
}
func (a *arrayQueuesDownstreamHandler) ackRangeTransaction(t *queueDownstreamTransaction, requestId string, seq []int64) {
	err := t.response.AckRange(seq)
	if err != nil {
		a.responsesCh <- &pb.QueuesDownstreamResponse{
			TransactionId:   t.transactionId,
			RefRequestId:    requestId,
			RequestTypeData: pb.QueuesDownstreamRequestType_AckRange,
			Messages:        nil,
			IsError:         true,
			Error:           err.Error(),
		}
	}
}
func (a *arrayQueuesDownstreamHandler) nackRangeTransaction(t *queueDownstreamTransaction, requestId string, seq []int64) {
	err := t.response.NackRange(seq)
	if err != nil {
		a.responsesCh <- &pb.QueuesDownstreamResponse{
			TransactionId:   t.transactionId,
			RefRequestId:    requestId,
			RequestTypeData: pb.QueuesDownstreamRequestType_NAckRange,
			Messages:        nil,
			IsError:         true,
			Error:           err.Error(),
		}
	}
}
func (a *arrayQueuesDownstreamHandler) reQueueAllTransaction(t *queueDownstreamTransaction, requestId string, channel string) {

	err := t.response.ReQueueAll(channel)
	if err != nil {
		a.responsesCh <- &pb.QueuesDownstreamResponse{
			TransactionId:   t.transactionId,
			RefRequestId:    requestId,
			RequestTypeData: pb.QueuesDownstreamRequestType_ReQueueAll,
			Messages:        nil,
			IsError:         true,
			Error:           err.Error(),
		}

	}

}
func (a *arrayQueuesDownstreamHandler) reQueueRangeTransaction(t *queueDownstreamTransaction, requestId string, channel string, seq []int64) {
	err := t.response.ReQueueRange(seq, channel)
	if err != nil {
		a.responsesCh <- &pb.QueuesDownstreamResponse{
			TransactionId:   t.transactionId,
			RefRequestId:    requestId,
			RequestTypeData: pb.QueuesDownstreamRequestType_ReQueueRange,
			Messages:        nil,
			IsError:         true,
			Error:           err.Error(),
		}

	}
}
func (a *arrayQueuesDownstreamHandler) getActiveOffsetsTransaction(t *queueDownstreamTransaction, requestId string) {
	offsets, err := t.response.ActiveOffsets()
	if err != nil {
		a.responsesCh <- &pb.QueuesDownstreamResponse{
			TransactionId:   t.transactionId,
			RefRequestId:    requestId,
			RequestTypeData: pb.QueuesDownstreamRequestType_ActiveOffsets,
			Messages:        nil,
			IsError:         true,
			Error:           err.Error(),
		}
	} else {
		a.responsesCh <- &pb.QueuesDownstreamResponse{
			TransactionId:   t.transactionId,
			RefRequestId:    requestId,
			RequestTypeData: pb.QueuesDownstreamRequestType_ActiveOffsets,
			Messages:        nil,
			ActiveOffsets:   offsets,
			IsError:         false,
			Error:           "",
		}
	}
}
func (a *arrayQueuesDownstreamHandler) closeTransaction(t *queueDownstreamTransaction, requestId string) {
	t.response.Close()
}

func (a *arrayQueuesDownstreamHandler) requestTask(ctx context.Context, request *pb.QueuesDownstreamRequest) {
	if request == nil {
		return
	}
	if request.RequestTypeData == pb.QueuesDownstreamRequestType_Get {
		a.createNewTransaction(ctx, request)
	} else {
		val, ok := a.transactions.Get(request.RefTransactionId)
		if !ok {
			a.responsesCh <- &pb.QueuesDownstreamResponse{
				TransactionId:   request.RefTransactionId,
				RefRequestId:    request.RequestID,
				RequestTypeData: request.RequestTypeData,
				Messages:        nil,
				IsError:         true,
				Error:           "transaction is completed or transaction id not found",
			}
			return
		}
		t := val.(*queueDownstreamTransaction)
		switch request.RequestTypeData {
		case pb.QueuesDownstreamRequestType_AckAll:
			a.ackAllTransaction(t, request.RequestID)

		case pb.QueuesDownstreamRequestType_AckRange:
			a.ackRangeTransaction(t, request.RequestID, request.SequenceRange)
		case pb.QueuesDownstreamRequestType_NAckAll:
			a.nackAllTransaction(t, request.RequestID)
		case pb.QueuesDownstreamRequestType_NAckRange:
			a.nackRangeTransaction(t, request.RequestID, request.SequenceRange)
		case pb.QueuesDownstreamRequestType_ReQueueAll:
			a.reQueueAllTransaction(t, request.RequestID, request.ReQueueChannel)
		case pb.QueuesDownstreamRequestType_ReQueueRange:
			a.reQueueRangeTransaction(t, request.RequestID, request.ReQueueChannel, request.SequenceRange)
		case pb.QueuesDownstreamRequestType_CloseByClient:
			a.closeTransaction(t, request.RequestID)
		case pb.QueuesDownstreamRequestType_ActiveOffsets:
			a.getActiveOffsetsTransaction(t, request.RequestID)
		default:
			a.responsesCh <- &pb.QueuesDownstreamResponse{
				TransactionId:   request.RefTransactionId,
				RefRequestId:    request.RequestID,
				RequestTypeData: pb.QueuesDownstreamRequestType_PollRequestTypeUnknown,
				Messages:        nil,
				IsError:         true,
				Error:           "request unknown",
			}
		}
	}

}
func (a *arrayQueuesDownstreamHandler) start() {
	for {
		select {

		case newRequest := <-a.requestsCh:
			a.requestTask(a.ctx, newRequest)
		case <-a.done:
			for _, val := range a.transactions.Items() {
				t := val.(*queueDownstreamTransaction)
				t.response.Close()
				a.releaseClientHandler(t.queueClientId)
			}
			a.ctxCancel()
			return
		}

	}
}

func (a *Array) QueuesDownstream(ctx context.Context, requests chan *pb.QueuesDownstreamRequest, response chan *pb.QueuesDownstreamResponse, done chan bool) error {
	_, err := a.queuesDownstream(ctx, requests, response, done)
	return err
}
func (a *Array) queuesDownstream(ctx context.Context, requests chan *pb.QueuesDownstreamRequest, response chan *pb.QueuesDownstreamResponse, done chan bool) (*arrayQueuesDownstreamHandler, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}

	handler := newArrayQueuesDownstreamHandler().
		SetGetClientHandler(a.NewQueueDownstreamClientFromPool).
		SetReleaseClientHandler(a.ReleaseQueueDownstreamClientFromPool).
		SetRequestsCh(requests).
		SetResponsesCh(response).
		SetDone(done)
	go handler.start()
	return handler, nil
}
