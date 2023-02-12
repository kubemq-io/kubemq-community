package services

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	"github.com/kubemq-io/kubemq-community/services/report"
	"go.uber.org/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/services/api"
	"github.com/kubemq-io/kubemq-community/services/array"
	"github.com/kubemq-io/kubemq-community/services/broker"
)

type SystemServices struct {
	Stopped              chan struct{}
	AppConfig            *config.Config
	Broker               *broker.Service
	Array                *array.Array
	Api                  *api.Server
	logger               *logging.Logger
	ctx                  context.Context
	cancelFunc           context.CancelFunc
	readyToAcceptTraffic *atomic.Bool
	reportService        *report.Service
}

func Start(ctx context.Context, appConfig *config.Config) (*SystemServices, error) {
	s := &SystemServices{
		AppConfig:            appConfig,
		Stopped:              make(chan struct{}, 1),
		readyToAcceptTraffic: atomic.NewBool(false),
		reportService:        report.NewService(),
	}
	s.ctx, s.cancelFunc = context.WithCancel(ctx)
	var err error
	s.logger = logging.CreateLoggerFactory(s.ctx, appConfig.Host, appConfig.Log).NewLogger("services")
	metrics.InitExporter(s.ctx)

	s.Broker = broker.New(appConfig)
	s.Api, err = api.CreateApiServer(s.ctx, s.Broker, appConfig)
	if err != nil {
		return nil, errors.Wrapf(entities.ErrOnLoadingService, "service: %s, error: %s", "api service", err.Error())
	}

	s.Broker, err = s.Broker.Start(s.ctx)
	if err != nil {
		return nil, errors.Wrapf(entities.ErrOnLoadingService, "service: %s, error: %s", "broker service", err.Error())
	}
	failCounter := 0
	for {
		select {
		case <-time.After(time.Second):
			if s.Broker.IsReady() {
				goto start
			} else {
				failCounter++
				if failCounter == 10 {
					s.logger.Warn("broker service is not ready")
					failCounter = 0
				}
			}

		case <-ctx.Done():
			return s, fmt.Errorf("broker service is not ready")
		}
	}
start:
	s.Array, err = array.Start(s.ctx, appConfig)
	if err != nil {
		return nil, errors.Wrapf(entities.ErrOnLoadingService, "service: %s, error: %s", "array service", err.Error())
	}
	if err := s.Api.InitApiService(ctx, s.Array); err != nil {
		s.logger.Errorf("error on loading api service: %s", err.Error())
	}
	if err := s.reportService.Init(s.ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (ss *SystemServices) Close() {
	ss.logger.Warnw("shutting down broker services")
	ss.Array.Close()
	<-ss.Array.Stopped
	ss.Broker.Close()
	<-ss.Broker.Stopped
	ss.Stopped <- struct{}{}
	ss.cancelFunc()
	ss.Api.Close()
	ss.logger.Warnw("broker services stopped")
}
