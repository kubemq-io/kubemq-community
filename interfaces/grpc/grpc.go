package grpc

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	pb "github.com/kubemq-io/protobuf/go"
	"go.uber.org/atomic"
	"io"
	"time"

	"github.com/kubemq-io/kubemq-community/services"

	"google.golang.org/grpc"

	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	options       *options
	services      *services.SystemServices
	logger        *logging.Logger
	grpcServer    *grpc.Server
	acceptTraffic *atomic.Bool
	times         []*atomic.Float64
	sampleCounter *atomic.Uint64
	samples       int
}

func NewServer(svc *services.SystemServices, appConfig *config.Config) (s *Server, err error) {
	opts := &options{
		port:     appConfig.Grpc.Port,
		maxSize:  appConfig.Grpc.BodyLimit,
		security: appConfig.Security,
		bufSize:  appConfig.Grpc.SubBuffSize,
	}
	s = &Server{
		options:       opts,
		services:      svc,
		logger:        logging.GetLogFactory().NewLogger("grpc"),
		acceptTraffic: atomic.NewBool(false),
		samples:       1000,
		sampleCounter: atomic.NewUint64(0),
	}
	for i := 0; i < s.samples; i++ {
		s.times = append(s.times, atomic.NewFloat64(0))
	}
	gRpcServer, err := configureServer(svc, s.logger, opts, s.acceptTraffic)
	s.grpcServer = gRpcServer
	if err != nil {
		s.logger.Errorw(" error grpc server", "error", err)
		return nil, err
	}

	pb.RegisterKubemqServer(gRpcServer, s)
	err = runServer(gRpcServer, opts.port)
	if err != nil {
		s.logger.Errorw(" error grpc server", "error", err)
		return nil, err
	}
	switch opts.security.Mode() {
	case config.SecurityModeNone:
		s.logger.Warnf("started insecure grpc server at port %d", opts.port)
	case config.SecurityModeTLS:
		s.logger.Warnf("started TLS secured grpc server at port %d", opts.port)
	case config.SecurityModeMTLS:
		s.logger.Warnf("started mTLS secured grpc server at port %d", opts.port)
	}
	if svc.Broker != nil {
		s.acceptTraffic.Store(svc.Broker.IsReady())
		s.UpdateBrokerStatus(s.acceptTraffic.Load())
		svc.Broker.RegisterToNotifyState("grpc", s.UpdateBrokerStatus)
	}
	//	go s.watch()
	return s, err
}

//	func (s *Server) watch() {
//		for {
//			select {
//			case <-time.After(5 * time.Second):
//				fmt.Printf("Counter1: %d, Counter2: %d\n", client.Counter1.Load(), client.Counter2.Load())
//			}
//		}
//	}
func (s *Server) QueuesInfo(ctx context.Context, request *pb.QueuesInfoRequest) (*pb.QueuesInfoResponse, error) {
	results, err := s.services.Broker.GetQueues(ctx)
	if err != nil {
		return nil, err
	}
	filtered := results.Filter(request.QueueName)
	info := &pb.QueuesInfo{
		TotalQueue: int32(filtered.TotalQueues),
		Sent:       filtered.Sent,
		Delivered:  filtered.Delivered,
		Waiting:    filtered.Waiting,
		Queues:     nil,
	}
	for _, queue := range filtered.Queues {
		info.Queues = append(info.Queues, &pb.QueueInfo{
			Name:          queue.Name,
			Messages:      queue.Messages,
			Bytes:         queue.Bytes,
			FirstSequence: queue.FirstSequence,
			LastSequence:  queue.LastSequence,
			Sent:          queue.Sent,
			Delivered:     queue.Delivered,
			Waiting:       queue.Waiting,
			Subscribers:   int64(queue.Subscribers),
		})
	}
	return &pb.QueuesInfoResponse{
		RefRequestID: request.RequestID,
		Info:         info,
	}, nil
}

func (s *Server) Close() {
	s.acceptTraffic.Store(false)
	if s.services.Broker != nil {
		s.services.Broker.UnRegisterToNotifyState("grpc")
	}
	s.grpcServer.Stop()
	s.logger.Warnw("grpc server shutdown completed")
}
func (s *Server) UpdateBrokerStatus(state bool) {
	s.acceptTraffic.Store(state)
	if state {
		s.logger.Warn("grpc interface is accepting traffic")
	} else {
		s.logger.Warn("grpc interface is not accepting traffic, waiting for broker")
	}
}
func (s *Server) SendEvent(ctx context.Context, event *pb.Event) (*pb.Result, error) {

	var err error
	var result *pb.Result
	if event.Store {
		result, err = s.services.Array.SendEventsStore(ctx, event)
	} else {
		result, err = s.services.Array.SendEvents(ctx, event)
	}
	if err != nil {
		s.logger.Errorw("error on send event", "error", err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	return result, nil
}
func (s *Server) SendEventsStream(stream pb.Kubemq_SendEventsStreamServer) error {
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.logger.Errorw("error on receive event from stream", "error", err)
			return status.Error(codes.Internal, err.Error())
		}
		var result *pb.Result
		var sendErr error
		if event.Store {
			result, sendErr := s.services.Array.SendEventsStore(stream.Context(), event)
			if sendErr != nil {
				result = &pb.Result{
					EventID: event.EventID,
					Sent:    false,
					Error:   sendErr.Error(),
				}
			}
			err := stream.Send(result)
			if err != nil {
				s.logger.Errorw("error on send result", "error", err)
				return status.Error(codes.Internal, err.Error())
			}
		} else {
			result, sendErr = s.services.Array.SendEvents(stream.Context(), event)
			// sending result only if there is an error
			if sendErr != nil {
				result = &pb.Result{
					EventID: event.EventID,
					Sent:    false,
					Error:   sendErr.Error(),
				}
				err := stream.Send(result)
				if err != nil {
					s.logger.Errorw("error on send result", "error", err)
					return status.Error(codes.Internal, err.Error())
				}
			}
		}

	}
	return nil
}

func (s *Server) SubscribeToEvents(subRequest *pb.Subscribe, stream pb.Kubemq_SubscribeToEventsServer) error {
	var pattern string
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	errCh := make(chan error, 10)
	eventReceiveCh := make(chan *pb.EventReceive, s.options.bufSize)
	var subId string
	var subErr error
	switch subRequest.SubscribeTypeData {
	case pb.Subscribe_Events:
		subId, subErr = s.services.Array.SubscribeEvents(ctx, subRequest, eventReceiveCh, errCh)
		pattern = "events"
	case pb.Subscribe_EventsStore:
		subId, subErr = s.services.Array.SubscribeEventsStore(ctx, subRequest, eventReceiveCh, errCh)
		pattern = "events_store"
	default:
		s.logger.Errorw("error on subscribe to events", "error", subErr)
		return status.Error(codes.Internal, entities.ErrInvalidSubscribeType.Error())
	}

	if subErr != nil {
		s.logger.Errorw("error on subscribe to events", "error", subErr)
		return status.Error(codes.Internal, subErr.Error())
	}
	metrics.ReportClient(pattern, "receive", subRequest.Channel, 1)

	defer func() {
		err := s.services.Array.DeleteClient(subId)
		metrics.ReportClient(pattern, "receive", subRequest.Channel, -1)
		if err != nil {
			s.logger.Errorw("error on disconnecting array client", "error", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case eventReceive := <-eventReceiveCh:
			err := stream.Send(eventReceive)
			if err != nil {
				s.logger.Errorw("error on sending events on stream", "error", err)
				return nil
			}
			metrics.ReportEventReceive(eventReceive, subRequest)
		case <-errCh:
			return nil
		}
	}
}

func (s *Server) SendRequest(ctx context.Context, request *pb.Request) (*pb.Response, error) {
	var response *pb.Response
	var err error

	switch request.RequestTypeData {
	case pb.Request_Command:
		response, err = s.services.Array.SendCommand(ctx, request)
	case pb.Request_Query:
		response, err = s.services.Array.SendQuery(ctx, request)

	default:
		s.logger.Errorw("error on send request", "error", entities.ErrInvalidRequestType)
		return nil, status.Error(codes.Internal, entities.ErrInvalidRequestType.Error())
	}
	metrics.ReportRequest(request, response, err)
	if err != nil {
		s.logger.Debugw("error on response from send request", "error", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	return response, nil
}

func (s *Server) SendResponse(ctx context.Context, response *pb.Response) (*pb.Empty, error) {
	err := s.services.Array.SendResponse(ctx, response)
	metrics.ReportResponse(response, err)
	if err != nil {
		s.logger.Debugw("error on send response", "error", err)
		return &pb.Empty{}, status.Error(codes.Internal, err.Error())
	}
	return &pb.Empty{}, nil
}

func (s *Server) SubscribeToRequests(subRequest *pb.Subscribe, stream pb.Kubemq_SubscribeToRequestsServer) error {
	var pattern string
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	errCh := make(chan error, 10)
	requestsCh := make(chan *pb.Request, s.options.bufSize)
	var subId string
	var err error
	switch subRequest.SubscribeTypeData {
	case pb.Subscribe_Commands:
		subId, err = s.services.Array.SubscribeToCommands(ctx, subRequest, requestsCh, errCh)
		pattern = "commands"
	case pb.Subscribe_Queries:
		subId, err = s.services.Array.SubscribeToQueries(ctx, subRequest, requestsCh, errCh)
		pattern = "queries"
	default:
		s.logger.Debugw("error on subscribe to request", "error", err)
		return status.Error(codes.Internal, entities.ErrInvalidSubscribeType.Error())
	}

	if err != nil {
		s.logger.Debugw("error on sending events on stream", "error", err)
		return status.Error(codes.Internal, err.Error())
	}
	metrics.ReportClient(pattern, "receive", subRequest.Channel, 1)

	defer func() {
		err := s.services.Array.DeleteClient(subId)
		if err != nil {
			s.logger.Debugw("error on subscribe to requests", "error", err)
		}
		metrics.ReportClient(pattern, "receive", subRequest.Channel, -1)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case request := <-requestsCh:
			err := stream.Send(request)
			if err != nil {
				s.logger.Debugw("error on sending message to wire", "error", err)
				return err
			}
			metrics.ReportRequest(request, nil, nil)
		case err := <-errCh:
			s.logger.Debugw("error received from transport server", "error", err)
			return nil
		}
	}

}

func (s *Server) SendQueueMessage(ctx context.Context, msg *pb.QueueMessage) (*pb.SendQueueMessageResult, error) {
	var result *pb.SendQueueMessageResult
	var err error
	result, err = s.services.Array.SendQueueMessage(ctx, msg)
	if err != nil {
		s.logger.Errorw("error on send queue message", "error", err)
		return nil, err
	}
	return result, nil

}

func (s *Server) SendQueueMessagesBatch(ctx context.Context, batchRequest *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error) {
	if len(batchRequest.Messages) == 0 {
		return nil, fmt.Errorf("empty messages batch")
	}
	reportChannel := batchRequest.Messages[0].Channel
	metrics.ReportClient("queues", "send", reportChannel, 1)
	defer metrics.ReportClient("queues", "send", reportChannel, -1)
	var batchResponse *pb.QueueMessagesBatchResponse
	var err error
	batchResponse, err = s.services.Array.SendQueueMessagesBatch(ctx, batchRequest)
	if err != nil {
		s.logger.Errorw("error on send batch of queue messages", "error", err)
		return nil, err
	}
	return batchResponse, nil
}

func (s *Server) ReceiveQueueMessages(ctx context.Context, request *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error) {
	metrics.ReportClient("queues", "receive", request.Channel, 1)
	defer metrics.ReportClient("queues", "receive", request.Channel, -1)
	var response *pb.ReceiveQueueMessagesResponse
	var err error
	response, err = s.services.Array.ReceiveQueueMessages(ctx, request)
	if err != nil {
		return nil, err
	}
	metrics.ReportReceiveQueueMessages(request, response)
	return response, nil
}

func (s *Server) StreamQueueMessage(stream pb.Kubemq_StreamQueueMessageServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer func() {
		cancel()
	}()
	messagesRequestsCh := make(chan *pb.StreamQueueMessagesRequest, 1)
	messagesResponsesCh := make(chan *pb.StreamQueueMessagesResponse, 1)
	doneCh := make(chan bool, 1)
	subID, err := s.services.Array.StreamQueueMessage(ctx, messagesRequestsCh, messagesResponsesCh, doneCh)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	defer func() {
		s.services.Array.ReleaseQueueDownstreamClientFromPool(subID)
	}()
	go func() {
		for {
			messagesRequest, err := stream.Recv()
			if err == io.EOF {
				return
			}
			select {
			case messagesRequestsCh <- messagesRequest:

			case <-ctx.Done():
				return
			}
		}
	}()
	doneFlag := false
	for {
		select {
		case <-ctx.Done():
			return nil
		case messagesResponse := <-messagesResponsesCh:
			err := stream.Send(messagesResponse)
			if err != nil {
				s.logger.Errorw("error on send stream queue response", "error", err)
				return nil
			}
			if messagesResponse.StreamRequestTypeData == pb.StreamRequestType_ReceiveMessage {
				metrics.ReportReceiveStreamQueueMessage(messagesResponse.Message)
			}
		case <-doneCh:
			doneFlag = true

		case <-time.After(10 * time.Millisecond):
			if doneFlag {
				return nil
			}
		}
	}
}
func (s *Server) QueuesUpstream(stream pb.Kubemq_QueuesUpstreamServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer func() {
		cancel()
	}()
	upstreamRequestsCh := make(chan *pb.QueuesUpstreamRequest, 10)
	upstreamResponsesCh := make(chan *pb.QueuesUpstreamResponse, 10)
	doneCh := make(chan bool, 1)
	err := s.services.Array.QueuesUpstream(ctx, upstreamRequestsCh, upstreamResponsesCh, doneCh)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer func() {
		doneCh <- true
	}()
	go func() {
		for {
			request, err := stream.Recv()
			if err == io.EOF {
				return
			}
			select {
			case upstreamRequestsCh <- request:
				if request != nil && len(request.Messages) > 0 {
					go func() {
						metrics.ReportQueueUpstreamRequest(request)
					}()
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case response := <-upstreamResponsesCh:
			err := stream.Send(response)
			if err != nil {
				s.logger.Errorw("error on send queue upstream response", "error", err)
				return nil
			}
		}
	}
}

func (s *Server) QueuesDownstream(stream pb.Kubemq_QueuesDownstreamServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer func() {
		cancel()
	}()
	downstreamRequestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	downstreamResponsesCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)
	err := s.services.Array.QueuesDownstream(ctx, downstreamRequestsCh, downstreamResponsesCh, doneCh)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer func() {
		doneCh <- true
	}()
	go func() {
		for {
			request, err := stream.Recv()
			if err == io.EOF {
				return
			}
			select {
			case downstreamRequestsCh <- request:

			case <-ctx.Done():
				return
			}

		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case response := <-downstreamResponsesCh:
			if response.RequestTypeData == pb.QueuesDownstreamRequestType_Get && len(response.Messages) > 0 {
				go func() {
					for _, msg := range response.Messages {
						metrics.ReportReceiveStreamQueueMessage(msg)
					}
				}()
			}
			err := stream.Send(response)
			if err != nil {
				s.logger.Errorw("error on send queue downstream responses", "error", err)

				return nil
			}

		}
	}
}

func (s *Server) Ping(ctx context.Context, req *pb.Empty) (*pb.PingResult, error) {
	ss := config.GetServerState()
	return &pb.PingResult{
		Host:                ss.Host,
		Version:             ss.Version,
		ServerStartTime:     ss.ServerStartTime,
		ServerUpTimeSeconds: ss.ServerUpTimeSeconds,
	}, nil
}

func (s *Server) AckAllQueueMessages(ctx context.Context, request *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error) {
	metrics.ReportClient("queues", "receive", request.Channel, 1)
	defer metrics.ReportClient("queues", "receive", request.Channel, -1)
	response, err := s.services.Array.AckAllQueueMessages(ctx, request)
	if err != nil {
		return nil, err
	}
	return response, nil
}
