package api

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/api"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/services/broker"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	"github.com/labstack/echo/v4"
	"sync"
	"time"
)

const saveInterval = time.Second * 5

type service struct {
	sync.Mutex
	appConfig       *config.Config
	broker          *broker.Service
	metricsExporter *metrics.Exporter
	lastSnapshot    *api.Snapshot
	db              *api.DB
	logger          *logging.Logger
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
	if err := s.db.Init(); err != nil {
		return fmt.Errorf("error initializing api db: %s", err.Error())
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
				s.saveSnapshot(ctx)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()

}

func (s *service) saveSnapshot(ctx context.Context) {
	ss, err := s.snapshot(ctx)
	if err != nil {
		s.logger.Errorf("error getting snapshot: %s", err.Error())
		return
	}
	if err := s.db.SaveSnapshot(ss); err != nil {
		s.logger.Errorf("error saving snapshot: %s", err.Error())
		return
	}
	//entities := ss.Entities.List()
	//if len(entities) > 0 {
	//	if err := s.db.AddEntities(entities); err != nil {
	//		s.logger.Errorf("error saving entities snapshot: %s", err.Error())
	//	} else {
	//		s.logger.Infof("saved %d entities snapshot", len(entities))
	//	}
	//}

}

func (s *service) snapshot(ctx context.Context) (*api.Snapshot, error) {
	s.Lock()
	defer s.Unlock()

	ss, err := s.metricsExporter.Snapshot()
	if err != nil {
		return nil, err
	}
	q, err := s.broker.GetQueues(ctx)
	if err != nil {
		return nil, err
	}
	group, ok := ss.Status.Entities["queues"]
	if ok {
		group.Out.Waiting = q.Waiting
	}
	for _, queue := range q.Queues {
		en, ok := ss.Entities.GetEntity("queues", queue.Name)
		if ok {
			en.Out.Waiting = queue.Waiting
		}
	}
	if s.lastSnapshot == nil {
		ss.Status.System.SetCPUUtilization(0, 0)
	} else {
		ss.Status.System.SetCPUUtilization(s.lastSnapshot.Status.System.Uptime, s.lastSnapshot.Status.System.TotalCPUSeconds)
	}

	s.lastSnapshot = ss
	return ss, nil
}
func (s *service) getSnapshot(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	ss, err := s.snapshot(ctx)
	if err != nil {
		return res.SetError(err).Send()
	}
	return res.SetResponseBody(ss).Send()
}
func (s *service) getInfo(c echo.Context) error {

	res := NewResponse(c)
	info := api.NewInfo().
		SetHost(s.appConfig.Host).
		SetVersion(s.appConfig.GetVersion()).
		SetIsHealthy(s.broker.IsHealthy()).
		SetIsReady(s.broker.IsReady())

	return res.SetResponseBody(info).Send()
}
func (s *service) getStatus(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)

	ss, err := s.snapshot(ctx)
	if err != nil {
		return res.SetError(err).Send()
	}
	return res.SetResponseBody(ss.Status).Send()
}
func (s *service) getEntities(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	ss, err := s.snapshot(ctx)
	if err != nil {
		return res.SetError(err).Send()
	}
	group := c.QueryParam("group")
	channel := c.QueryParam("channel")

	if group == "" {
		return res.SetResponseBody(ss.Entities).Send()
	}
	grEntities, ok := ss.Entities.GetFamily(group)
	if !ok {
		return res.SetHttpCode(400).SetError(fmt.Errorf("no such group: %s", group)).Send()
	}

	if channel == "" {
		return res.SetResponseBody(grEntities).Send()
	}
	chEntity, ok := grEntities[channel]
	if !ok {
		return res.SetHttpCode(400).SetError(fmt.Errorf("no such channel: %s in group: %s", channel, group)).Send()
	}
	return res.SetResponseBody(chEntity).Send()
}
