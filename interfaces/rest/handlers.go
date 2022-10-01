package rest

import (
	"context"
	"encoding/json"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubemq-io/kubemq-community/config"

	"github.com/pkg/errors"

	pb "github.com/kubemq-io/protobuf/go"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
)

var (
	upgrader = websocket.Upgrader{
		WriteBufferSize: 10 * 1024,
		ReadBufferSize:  10 * 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func ToPlainJson(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}
func FromPlainJson(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (s *Server) handlerSendMessage(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	event := &pb.Event{}
	err := c.Bind(event)
	if err != nil {
		return res.SetError(err).SetHttpCode(400).Send()
	}
	var result *pb.Result

	if event.Store {
		result, err = s.services.Array.SendEventsStore(ctx, event)
	} else {
		result, err = s.services.Array.SendEvents(ctx, event)
	}
	metrics.ReportEvent(event, result)
	if err != nil {
		return res.SetError(err).SetHttpCode(400).Send()
	}
	return res.SetResponseBody(result).Send()
}

func (s *Server) handlerSendRequest(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	request := &pb.Request{}
	err := c.Bind(request)
	if err != nil {
		return res.SetError(err).SetHttpCode(400).Send()
	}
	var response *pb.Response

	switch request.RequestTypeData {
	case pb.Request_Command:
		response, err = s.services.Array.SendCommand(ctx, request)

	case pb.Request_Query:
		response, err = s.services.Array.SendQuery(ctx, request)

	default:
		return res.SetError(entities.ErrInvalidRequestType).SetHttpCode(400).Send()
	}
	metrics.ReportRequest(request, response, err)
	if err != nil {

		return res.SetError(err).SetHttpCode(400).Send()
	}

	return res.SetResponseBody(response).Send()
}

func (s *Server) handlerSendResponse(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()

	httpResponse := NewResponse(c)
	response := &pb.Response{}
	err := c.Bind(response)
	if err != nil {
		return httpResponse.SetError(err).SetHttpCode(400).Send()
	}

	err = s.services.Array.SendResponse(ctx, response)
	metrics.ReportResponse(response, err)
	if err != nil {
		return httpResponse.SetError(err).SetHttpCode(400).Send()
	}
	return httpResponse.Send()
}

func (s *Server) handlerSubscribeToEvents(c echo.Context, appConfig *config.Config) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	var pattern string
	res := NewResponse(c)
	subRequest := paramsToSubRequest(c)
	eventReceiveCh := make(chan *pb.EventReceive, appConfig.Rest.SubBuffSize)
	errCh := make(chan error, 10)
	var err error
	var subId string
	switch subRequest.SubscribeTypeData {
	case pb.Subscribe_Events:
		subId, err = s.services.Array.SubscribeEvents(ctx, subRequest, eventReceiveCh, errCh)
		pattern = "events"
	case pb.Subscribe_EventsStore:
		subId, err = s.services.Array.SubscribeEventsStore(ctx, subRequest, eventReceiveCh, errCh)
		pattern = "events_store"
	default:
		return res.SetError(entities.ErrInvalidSubscribeType).SetHttpCode(400).Send()
	}
	if err != nil {
		return res.SetError(err).SetHttpCode(400).Send()
	}
	metrics.ReportClient(pattern, "receive", subRequest.Channel, 1)

	defer func() {
		err := s.services.Array.DeleteClient(subId)
		if err != nil {
			s.logger.Debug(err)
		}
		metrics.ReportClient(pattern, "receive", subRequest.Channel, -1)
	}()

	conn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {

		return res.SetError(err).SetHttpCode(500).Send()
	}
	defer func() {
		_ = conn.Close()
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_, _, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			goto drain
		case eventReceive := <-eventReceiveCh:
			data, err := ToPlainJson(eventReceive)
			if err != nil {
				s.logger.Error(entities.ErrMarshalError)
				continue
			}
			errOnSend := conn.WriteMessage(1, data)
			if err != nil {
				s.logger.Debugw("error on writing to web socket", "error", errors.Wrapf(entities.ErrOnWriteToWebSocket, errOnSend.Error()))
				goto drain
			}
			metrics.ReportEventReceive(eventReceive, subRequest)
		case err := <-errCh:
			var closeErr error
			if err != nil {
				closeErr = errors.Wrapf(entities.ErrConnectionClosed, "reason %s", err.Error())
			} else {
				goto drain
			}
			s.logger.Errorw("error receiving from transport server", "error", closeErr)
			resErr := &Response{}
			errOnSend := conn.WriteMessage(1, resErr.SetError(closeErr).Marshal())
			if errOnSend != nil {
				s.logger.Debugw("error on writing to web socket", "error", errors.Wrapf(entities.ErrOnWriteToWebSocket, errOnSend.Error()))
			}
			goto drain

		}
	}
drain:

	return nil
}

func (s *Server) handlerSubscribeToRequests(c echo.Context, appConfig *config.Config) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	subRequest := paramsToSubRequest(c)
	requestsCh := make(chan *pb.Request, appConfig.Rest.SubBuffSize)
	errCh := make(chan error, 10)
	var err error
	var subId string
	var pattern string
	switch subRequest.SubscribeTypeData {
	case pb.Subscribe_Commands:
		subId, err = s.services.Array.SubscribeToCommands(ctx, subRequest, requestsCh, errCh)
		pattern = "commands"
	case pb.Subscribe_Queries:
		subId, err = s.services.Array.SubscribeToQueries(ctx, subRequest, requestsCh, errCh)
		pattern = "queries"
	default:
		return res.SetError(entities.ErrInvalidSubscribeType).SetHttpCode(400).Send()
	}
	if err != nil {
		return res.SetError(err).SetHttpCode(400).Send()
	}
	metrics.ReportClient(pattern, "receive", subRequest.Channel, 1)
	metrics.ReportClient(pattern, "receive", subRequest.Channel, -1)
	defer func() {
		err := s.services.Array.DeleteClient(subId)
		if err != nil {
			s.logger.Errorw("error on deleting array client", "error", err)
		}
		metrics.ReportClient(pattern, "receive", subRequest.Channel, -1)
	}()

	conn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {

		return res.SetError(err).SetHttpCode(500).Send()
	}
	defer func() {
		_ = conn.Close()
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:

			}
			_, _, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			goto drain

		case request := <-requestsCh:
			reqData, err := ToPlainJson(request)
			if err != nil {
				s.logger.Error(entities.ErrMarshalError)
				continue
			}
			errOnSend := conn.WriteMessage(1, reqData)
			if err != nil {
				s.logger.Debugw("error on writing to web socket", "error", errors.Wrapf(entities.ErrOnWriteToWebSocket, errOnSend.Error()))
				goto drain
			}
			metrics.ReportRequest(request, nil, nil)
		case err := <-errCh:
			var closeErr error
			if err != nil {
				closeErr = errors.Wrapf(entities.ErrConnectionClosed, "reason: %s", err.Error())
			} else {
				goto drain
			}
			s.logger.Errorw("error received from transport server", "error", closeErr)
			resErr := &Response{}
			errOnSend := conn.WriteMessage(1, resErr.SetError(closeErr).Marshal())
			if errOnSend != nil {
				s.logger.Debugw("error on writing to web socket", "error", errors.Wrapf(entities.ErrOnWriteToWebSocket, errOnSend.Error()))
			}
			goto drain
		}

	}

drain:
	return nil
}

func (s *Server) handlerSendMessageStream(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	conn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return res.SetError(err).SetHttpCode(500).Send()
	}
	defer func() {
		_ = conn.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			//s.logger.Error(errors.Wrapf(entities.ErrOnReadToWebSocket, err.Error()))
			return nil
		}
		if msgType == 1 {
			event := &pb.Event{}
			err = FromPlainJson(data, event)
			if err != nil {
				s.logger.Error(entities.ErrMarshalError)
				continue
			}
			var result *pb.Result
			if event.Store {
				result, err = s.services.Array.SendEventsStore(ctx, event)
			} else {
				result, err = s.services.Array.SendEvents(ctx, event)
			}
			if err != nil {
				result = &pb.Result{
					EventID: "",
					Sent:    false,
					Error:   err.Error(),
				}
			}

			data, err := ToPlainJson(result)
			if err != nil {
				s.logger.Error(entities.ErrMarshalError)
				continue
			}

			errOnSend := conn.WriteMessage(1, data)
			if err != nil {
				s.logger.Debugw("error on writing to web socket", "error", errors.Wrapf(entities.ErrOnWriteToWebSocket, errOnSend.Error()))
				return nil
			}
			metrics.ReportEvent(event, result)
		} else {
			s.logger.Error(entities.ErrInvalidWebSocketMessageType)
			continue
		}
	}

}

func paramsToSubRequest(c echo.Context) *pb.Subscribe {
	subReq := &pb.Subscribe{
		SubscribeTypeData:    0,
		ClientID:             "",
		Channel:              "",
		Group:                "",
		EventsStoreTypeData:  0,
		EventsStoreTypeValue: 0,
	}
	subReq.ClientID = c.QueryParam("client_id")
	subReq.Channel = c.QueryParam("channel")
	subReq.Group = c.QueryParam("group")
	switch c.QueryParam("subscribe_type") {
	case "events":
		subReq.SubscribeTypeData = pb.Subscribe_Events
	case "events_store":
		subReq.SubscribeTypeData = pb.Subscribe_EventsStore
	case "commands":
		subReq.SubscribeTypeData = pb.Subscribe_Commands
	case "queries":
		subReq.SubscribeTypeData = pb.Subscribe_Queries
	default:

	}
	qst, _ := strconv.Atoi(c.QueryParam("events_store_type_data"))
	subReq.EventsStoreTypeData = intToSubscribeQueueType(qst)
	subReq.EventsStoreTypeValue, _ = strconv.ParseInt(c.QueryParam("events_store_type_value"), 10, 64)

	return subReq
}

func (s *Server) handlerSendQueueMessage(c echo.Context) error {

	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	msg := &pb.QueueMessage{}
	err := c.Bind(msg)
	if err != nil {

		return res.SetError(err).SetHttpCode(400).Send()
	}
	var result *pb.SendQueueMessageResult
	result, err = s.services.Array.SendQueueMessage(ctx, msg)
	metrics.ReportSendQueueMessage(msg, result)
	if err != nil {
		return res.SetError(err).SetHttpCode(400).Send()
	}
	return res.SetResponseBody(result).Send()
}

func (s *Server) handlerSendBatchQueueMessages(c echo.Context) error {

	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	batchRequest := &pb.QueueMessagesBatchRequest{}
	err := c.Bind(batchRequest)
	if err != nil {
		return res.SetError(err).SetHttpCode(400).Send()
	}
	var batchResponse *pb.QueueMessagesBatchResponse
	batchResponse, err = s.services.Array.SendQueueMessagesBatch(ctx, batchRequest)
	metrics.ReportSendQueueMessageBatch(batchRequest, batchResponse)
	if err != nil {
		return res.SetError(err).SetHttpCode(400).Send()
	}

	return res.SetResponseBody(batchResponse).Send()
}

func (s *Server) handlerReceiveQueueMessages(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	request := &pb.ReceiveQueueMessagesRequest{}
	err := c.Bind(request)
	if err != nil {
		return res.SetError(err).SetHttpCode(400).Send()
	}
	var response *pb.ReceiveQueueMessagesResponse
	response, err = s.services.Array.ReceiveQueueMessages(ctx, request)
	metrics.ReportReceiveQueueMessages(request, response)
	if err != nil {
		return res.SetError(err).SetHttpCode(400).Send()
	}

	return res.SetResponseBody(response).Send()

}

func (s *Server) handlerStreamQueueMessages(c echo.Context) error {
	//var channelForMetrics string
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	conn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return res.SetError(err).SetHttpCode(500).Send()
	}
	defer func() {
		conn.Close()
	}()

	reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
	messagesResponsesCh := make(chan *pb.StreamQueueMessagesResponse, 1)
	doneCh := make(chan bool, 1)
	subID, err := s.services.Array.StreamQueueMessage(ctx, reqCh, messagesResponsesCh, doneCh)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer func() {
		s.services.Array.ReleaseQueueDownstreamClientFromPool(subID)

	}()

	go func() {
		for {
			msgType, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if msgType == 1 {
				messagesRequest := &pb.StreamQueueMessagesRequest{}
				err = FromPlainJson(data, messagesRequest)
				if err != nil {
					s.logger.Error(entities.ErrMarshalError)
					continue
				}
				//if messagesRequest != nil && messagesRequest.StreamRequestTypeData == pb.StreamRequestType_ReceiveMessage && channelForMetrics == "" {
				//	metrics.ReportClient("queue", "receive", messagesRequest.Channel, 1)
				//	channelForMetrics = messagesRequest.Channel
				//}
				select {
				case reqCh <- messagesRequest:
				case <-ctx.Done():
					return
				}

			} else {
				s.logger.Error(entities.ErrInvalidWebSocketMessageType)
				continue
			}
		}
	}()
	doneFlag := false
	for {
		select {
		case <-ctx.Done():
			return nil
		case messagesResponse := <-messagesResponsesCh:
			data, err := ToPlainJson(messagesResponse)
			if err != nil {
				s.logger.Error(entities.ErrMarshalError)
				continue
			}

			errOnSend := conn.WriteMessage(1, data)
			if errOnSend != nil {
				s.logger.Errorw("error on send stream queue response", "error", err)
				return nil
			}
			if messagesResponse.StreamRequestTypeData == pb.StreamRequestType_ReceiveMessage {
				metrics.ReportReceiveStreamQueueMessage(messagesResponse.Message)
			}

		case <-doneCh:
			doneFlag = true
		case <-time.After(1 * time.Millisecond):
			if doneFlag {
				return nil
			}
		}
	}

}
func (s *Server) handlerQueueInfo(c echo.Context) error {
	//var channelForMetrics string
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	request := &pb.QueuesInfoRequest{}
	err := c.Bind(request)
	if err != nil {
		return res.SetError(err).SetHttpCode(400).Send()
	}

	results, err := s.services.Broker.GetQueues(ctx)
	if err != nil {
		return err
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
	resp := &pb.QueuesInfoResponse{
		RefRequestID: request.RequestID,
		Info:         info,
	}
	return res.SetResponseBody(resp).Send()
}
func (s *Server) handlerPing(c echo.Context) error {

	res := NewResponse(c)
	ss := config.GetServerState()
	result := &pb.PingResult{
		Host:                ss.Host,
		Version:             ss.Version,
		ServerStartTime:     ss.ServerStartTime,
		ServerUpTimeSeconds: ss.ServerUpTimeSeconds,
	}
	return res.SetResponseBody(result).Send()
}

func (s *Server) handlerAckAllQueueMessages(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	req := &pb.AckAllQueueMessagesRequest{}
	err := c.Bind(req)
	if err != nil {

		return res.SetError(err).SetHttpCode(400).Send()
	}

	result, err := s.services.Array.AckAllQueueMessages(ctx, req)
	if err != nil {

		return res.SetError(err).SetHttpCode(400).Send()
	}

	return res.SetResponseBody(result).Send()
}

func intToSubscribeQueueType(i int) pb.Subscribe_EventsStoreType {
	switch i {
	case 0:
		return pb.Subscribe_EventsStoreTypeUndefined
	case 1:
		return pb.Subscribe_StartNewOnly
	case 2:
		return pb.Subscribe_StartFromFirst
	case 3:
		return pb.Subscribe_StartFromLast
	case 4:
		return pb.Subscribe_StartAtSequence
	case 5:
		return pb.Subscribe_StartAtTime
	case 6:
		return pb.Subscribe_StartAtTimeDelta
	}
	return pb.Subscribe_EventsStoreTypeUndefined
}
