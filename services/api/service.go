package api

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/api"
	actions_pkg "github.com/kubemq-io/kubemq-community/pkg/api/actions"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/services/api/actions"
	"github.com/kubemq-io/kubemq-community/services/broker"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	"github.com/labstack/echo/v4"
	"sync"
	"time"
)

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
	groupDTO := api.NewSnapshotDTO(s.lastSnapshot.System, s.lastSnapshot.Entities)
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
	default:
		return res.SetError(fmt.Errorf("unknown action type: %s", req.Type)).Send()
	}
}
