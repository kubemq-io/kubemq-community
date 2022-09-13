package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/api"
	actions_pkg "github.com/kubemq-io/kubemq-community/pkg/api/actions"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/services/api/actions"
	"github.com/kubemq-io/kubemq-community/services/broker"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	"github.com/labstack/echo/v4"
	"net/http"
	"sync"
	"time"
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

const saveInterval = time.Second * 5

type service struct {
	sync.Mutex
	appConfig               *config.Config
	broker                  *broker.Service
	metricsExporter         *metrics.Exporter
	lastSnapshot            *api.Snapshot
	db                      *api.DB
	logger                  *logging.Logger
	lastLoadedEntitiesGroup *api.EntitiesGroup
	actionClient            *actions.Client
}

func newService(appConfig *config.Config, broker *broker.Service, exp *metrics.Exporter) *service {
	s := &service{
		appConfig:       appConfig,
		broker:          broker,
		metricsExporter: exp,
		db:              api.NewDB(),
	}
	return s
}

func (s *service) init(ctx context.Context, logger *logging.Logger) error {
	s.logger = logger
	if err := s.db.Init(s.appConfig.Store.StorePath); err != nil {
		return fmt.Errorf("error initializing api db: %s", err.Error())
	}
	var err error
	s.lastLoadedEntitiesGroup, err = s.db.GetLastEntities()
	if err != nil {
		s.logger.Errorf("error getting last entities data from local db: %s", err.Error())
		s.lastLoadedEntitiesGroup = api.NewEntitiesGroup()
	}
	s.actionClient = actions.NewClient()
	err = s.actionClient.Init(ctx, s.appConfig.Grpc.Port)
	if err != nil {
		return fmt.Errorf("error initializing actions client: %s", err.Error())
	}
	go s.run(ctx)
	return nil
}
func (s *service) stop() error {
	return s.db.Close()
}
func (s *service) run(ctx context.Context) {
	s.logger.Infof("starting api snapshot service")
	go func() {
		ticker := time.NewTicker(saveInterval)
		for {
			select {
			case <-ticker.C:
				s.saveEntitiesGroup(ctx)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()

}
func (s *service) saveEntitiesGroup(ctx context.Context) {
	currentSnapshot, err := s.getCurrentSnapshot(ctx)
	if err != nil {
		s.logger.Errorf("error getting snapshot: %s", err.Error())
		return
	}
	if err := s.db.SaveEntitiesGroup(currentSnapshot.Entities); err != nil {
		s.logger.Errorf("error saving entities group: %s", err.Error())
		return
	}
	if err := s.db.SaveLastEntitiesGroup(currentSnapshot.Entities); err != nil {
		s.logger.Errorf("error saving last entities group: %s", err.Error())
		return
	}

}

func (s *service) getCurrentSnapshot(ctx context.Context) (*api.Snapshot, error) {
	s.Lock()
	defer s.Unlock()

	ss, err := s.metricsExporter.Snapshot()
	if err != nil {
		return nil, err
	}
	ss.Entities = s.lastLoadedEntitiesGroup.Clone().Merge(ss.Entities)

	// clean queue entities waiting messages count
	queueGroup, ok := ss.Entities.Families["queues"]
	if ok {
		for _, entity := range queueGroup.Entities {
			entity.Out.Waiting = 0
		}
	}

	q, err := s.broker.GetQueues(ctx)
	if err != nil {
		return nil, err
	}
	for _, queue := range q.Queues {
		en, ok := ss.Entities.GetEntity("queues", queue.Name)
		if ok {
			en.Out.Waiting = queue.Waiting
		}
	}
	if s.lastSnapshot == nil {
		ss.System.SetCPUUtilization(0, 0)
	} else {
		ss.System.SetCPUUtilization(s.lastSnapshot.System.Uptime, s.lastSnapshot.System.TotalCPUSeconds)
	}
	s.lastSnapshot = ss
	return ss, nil
}

func (s *service) getSnapshot(c echo.Context) error {
	res := NewResponse(c)
	if s.lastSnapshot == nil {
		_, err := s.getCurrentSnapshot(c.Request().Context())
		if err != nil {
			return res.SetError(err).Send()
		}
	}
	s.Lock()
	groupDTO := api.NewSnapshotDTO(s.lastSnapshot.System, s.lastSnapshot.Entities)
	s.Unlock()
	return res.SetResponseBody(groupDTO).Send()
}

func (s *service) handleRequests(c echo.Context) error {
	res := NewResponse(c)
	req := actions_pkg.NewRequest()
	if err := c.Bind(req); err != nil {
		return res.SetError(err).Send()
	}
	switch req.Type {
	case "create_channel":
		actionRequest := actions_pkg.NewCreateChannelRequest()
		if err := actionRequest.ParseRequest(req); err != nil {
			return res.SetError(err).Send()
		}
		if err := s.actionClient.CreateChannel(c.Request().Context(), actionRequest); err != nil {
			return res.SetError(err).Send()
		}
		return res.Send()
	case "send_queue_message":
		actionRequest := actions_pkg.NewSendQueueMessageRequest()
		if err := actionRequest.ParseRequest(req); err != nil {
			return res.SetError(err).Send()
		}
		if actionRes, err := s.actionClient.SendQueueMessage(c.Request().Context(), actionRequest); err != nil {
			return res.SetError(err).Send()
		} else {
			return res.SetResponseBody(actionRes).Send()
		}
	case "receive_queue_messages":
		actionRequest := actions_pkg.NewReceiveQueueMessagesRequest()
		if err := actionRequest.ParseRequest(req); err != nil {
			return res.SetError(err).Send()
		}
		if actionRes, err := s.actionClient.ReceiveQueueMessages(c.Request().Context(), actionRequest); err != nil {
			return res.SetError(err).Send()
		} else {
			return res.SetResponseBody(actionRes).Send()
		}
	case "purge_queue_channel":
		actionRequest := actions_pkg.NewPurgeQueueChannelRequest()
		if err := actionRequest.ParseRequest(req); err != nil {
			return res.SetError(err).Send()
		}
		if actionRes, err := s.actionClient.PurgeQueueChannel(c.Request().Context(), actionRequest); err != nil {
			return res.SetError(err).Send()
		} else {
			return res.SetResponseBody(actionRes).Send()
		}
	case "send_pubsub_message":
		actionRequest := actions_pkg.NewSendPubSubMessageRequest()
		if err := actionRequest.ParseRequest(req); err != nil {
			return res.SetError(err).Send()
		}
		if err := s.actionClient.SendPubSubMessage(c.Request().Context(), actionRequest); err != nil {
			return res.SetError(err).Send()
		} else {
			return res.Send()
		}
	case "send_cqrs_message_request":
		actionRequest := actions_pkg.NewSendCQRSMessageRequest()
		if err := actionRequest.ParseRequest(req); err != nil {
			return res.SetError(err).Send()
		}
		if actionRes, err := s.actionClient.SendCQRSMessageRequest(c.Request().Context(), actionRequest); err != nil {
			return res.SetError(err).Send()
		} else {
			return res.SetResponseBody(actionRes).Send()
		}

	case "send_cqrs_message_response":
		actionRequest := actions_pkg.NewSendCQRSMessageResponse()
		if err := actionRequest.ParseRequest(req); err != nil {
			return res.SetError(err).Send()
		}
		if err := s.actionClient.SendCQRSMessageResponse(c.Request().Context(), actionRequest); err != nil {
			return res.SetError(err).Send()
		} else {
			return res.Send()
		}
	default:
		return res.SetError(fmt.Errorf("unknown action type: %s", req.Type)).Send()
	}
}
func (s *service) handlerConnectionStatus(c echo.Context) error {
	res := NewResponse(c)
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	conn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		s.logger.Errorf("error upgrading connection: %s", err.Error())
		return res.SetError(err).SetHttpCode(500).Send()
	}
	defer func() {
		_ = conn.Close()
	}()
	ticker := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return nil
		case <-ticker.C:
			if s.lastSnapshot == nil {
				continue
			}
			snapshot := api.NewSnapshotDTO(s.lastSnapshot.System, s.lastSnapshot.Entities)
			if err := conn.WriteJSON(snapshot); err != nil {
				return nil
			}
		}
	}
}
func (s *service) handlerSubscribeToPubSub(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	subType := c.QueryParam("subscribe_type")
	subClientId := c.QueryParam("client_id")
	subChannel := c.QueryParam("channel")
	subGroup := c.QueryParam("group")
	subEventStoreType := c.QueryParam("events_store_type_data")
	subEventStoreValue := c.QueryParam("events_store_type_value")
	eventReceiveCh := make(chan *actions_pkg.SubscribePubSubMessage, 10)
	errCh := make(chan error, 10)

	switch subType {
	case "events":
		err := s.actionClient.SubscribeToEvents(ctx, subChannel, subGroup, eventReceiveCh, errCh)
		if err != nil {
			s.logger.Errorf("error subscribing to events: %s", err.Error())
			return res.SetError(err).Send()
		}
	case "events_store":
		err := s.actionClient.SubscribeToEventsStore(ctx, subChannel, subGroup, subClientId, subEventStoreType, subEventStoreValue, eventReceiveCh, errCh)
		if err != nil {
			s.logger.Errorf("error subscribing to events store: %s", err.Error())
			return res.SetError(err).Send()
		}
	default:
		s.logger.Errorf("unknown subscribe type: %s", subType)
		return res.SetError(fmt.Errorf("unknown subscribe type: %s", subType)).Send()
	}
	if subChannel == "" {
		s.logger.Errorf("subscribe type %s requires channel", subType)
		return res.SetError(fmt.Errorf("channel is empty")).Send()
	}

	conn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		s.logger.Errorf("error upgrading connection: %s", err.Error())
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
				s.logger.Errorf("error converting event to plain json: %s", err.Error())
				continue
			}
			errOnSend := conn.WriteMessage(1, data)
			if err != nil {
				s.logger.Debugf("error on writing to web socket, error: %s", errOnSend.Error())
				goto drain
			}
		case err := <-errCh:
			var closeErr error
			if err != nil {
				closeErr = fmt.Errorf("connection closed, reason: %s", err.Error())
			} else {
				goto drain
			}
			s.logger.Error(closeErr)
			errOnSend := conn.WriteMessage(1, []byte(closeErr.Error()))
			if errOnSend != nil {
				s.logger.Debugf("error on writing to web socket, error: %s", errOnSend.Error())
			}
			goto drain
		}
	}
drain:

	return nil
}

func (s *service) handlerStreamQueueMessages(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()

	conn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		s.logger.Errorf("error upgrading connection: %s", err.Error())
		return fmt.Errorf("error upgrading connection: %s", err.Error())
	}
	defer func() {
		_ = conn.Close()
	}()
	errCh := make(chan error, 10)
	requests := make(chan *actions_pkg.StreamQueueMessagesRequest, 1)
	responses := make(chan *actions_pkg.StreamQueueMessagesResponse, 1)
	go s.actionClient.StreamQueueMessages(ctx, requests, responses)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_, data, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			request := actions_pkg.NewStreamQueueMessagesRequest()
			if err := request.ParseRequest(data); err != nil {
				s.logger.Errorf("error parsing request: %s", err.Error())
				return
			}
			requests <- request
		}
	}()

	for {
		select {
		case <-ctx.Done():
			goto drain

		case response := <-responses:
			fmt.Println(response.Marshal())
			errOnSend := conn.WriteJSON(response)
			if err != nil {
				s.logger.Debugf("error on writing to web socket, error: %s", errOnSend.Error())
				goto drain
			}
		case err := <-errCh:
			var closeErr error
			if err != nil {
				closeErr = fmt.Errorf("connection closed, reason: %s", err.Error())
			} else {
				goto drain
			}
			s.logger.Error(closeErr)
			errOnSend := conn.WriteMessage(1, []byte(closeErr.Error()))
			if errOnSend != nil {
				s.logger.Debugf("error on writing to web socket, error: %s", errOnSend.Error())
			}
			goto drain
		}

	}
drain:
	return nil
}

func (s *service) handlerSubscribeToCQRS(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	subType := c.QueryParam("subscribe_type")
	subChannel := c.QueryParam("channel")
	subGroup := c.QueryParam("group")
	requestsReceiveCh := make(chan *actions_pkg.SubscribeCQRSRequestMessage, 10)
	errCh := make(chan error, 10)

	switch subType {
	case "commands":
		err := s.actionClient.SubscribeToCommands(ctx, subChannel, subGroup, requestsReceiveCh, errCh)
		if err != nil {
			s.logger.Errorf("error subscribing to commands: %s", err.Error())
			return res.SetError(err).Send()
		}
	case "queries":
		err := s.actionClient.SubscribeToQueries(ctx, subChannel, subGroup, requestsReceiveCh, errCh)
		if err != nil {
			s.logger.Errorf("error subscribing to queris: %s", err.Error())
			return res.SetError(err).Send()
		}
	default:
		s.logger.Errorf("unknown subscribe type: %s", subType)
		return res.SetError(fmt.Errorf("unknown subscribe type: %s", subType)).Send()
	}
	if subChannel == "" {
		s.logger.Errorf("subscribe type %s requires channel", subType)
		return res.SetError(fmt.Errorf("channel is empty")).Send()
	}

	conn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		s.logger.Errorf("error upgrading connection: %s", err.Error())
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
		case request := <-requestsReceiveCh:
			data, err := ToPlainJson(request)
			if err != nil {
				s.logger.Errorf("error converting request to plain json: %s", err.Error())
				continue
			}
			errOnSend := conn.WriteMessage(1, data)
			if err != nil {
				s.logger.Debugf("error on writing to web socket, error: %s", errOnSend.Error())
				goto drain
			}
		case err := <-errCh:
			var closeErr error
			if err != nil {
				closeErr = fmt.Errorf("connection closed, reason: %s", err.Error())
			} else {
				goto drain
			}
			s.logger.Error(closeErr)
			errOnSend := conn.WriteMessage(1, []byte(closeErr.Error()))
			if errOnSend != nil {
				s.logger.Debugf("error on writing to web socket, error: %s", errOnSend.Error())
			}
			goto drain
		}
	}
drain:

	return nil
}
