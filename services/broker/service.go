package broker

import (
	"context"
	"fmt"
	"go.uber.org/atomic"
	"time"

	"github.com/kubemq-io/kubemq-community/pkg/http"
	"github.com/kubemq-io/kubemq-community/services/metrics"

	"github.com/kubemq-io/kubemq-community/pkg/logging"

	"github.com/kubemq-io/kubemq-community/config"

	natsd "github.com/kubemq-io/broker/server/gnatsd/server"
	snats "github.com/kubemq-io/broker/server/stan/server"
)

const (
	readyTimeout       = 90 * time.Second
	reportTimeInterval = 30 * time.Second
)

type State int8

const (
	Standalone State = iota
)

type Service struct {
	NatsOptions             *natsd.Options
	SnatsOptions            *snats.Options
	isHealthy               *atomic.Bool
	isReady                 *atomic.Bool
	Nats                    *natsd.Server
	Snats                   *snats.StanServer
	Stopped                 chan struct{}
	logger                  *logging.Logger
	appConfig               *config.Config
	cancelFunc              context.CancelFunc
	disableMetricsReporting bool
	stateNotifiers          *HealthNotifier
}

func New(appConfig *config.Config) *Service {
	s := &Service{
		Nats:      nil,
		Snats:     nil,
		Stopped:   make(chan struct{}, 1),
		appConfig: appConfig,

		isReady:        atomic.NewBool(false),
		isHealthy:      atomic.NewBool(false),
		stateNotifiers: NewHealthNotifier(),
	}
	return s
}
func (s *Service) Start(ctx context.Context) (*Service, error) {
	ctx, cancel := context.WithCancel(ctx)
	s.cancelFunc = cancel
	s.logger = logging.GetLogFactory().NewLogger("broker")
	err := s.startAsBroker(ctx, s.appConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Service) DisableMetricsReporting() {
	s.disableMetricsReporting = true
}
func (s *Service) Close() {
	s.cancelFunc()
	if s.Snats != nil {
		s.Snats.Shutdown()
	}
	s.Nats.Shutdown()
	s.logger.Warnw("broker shutdown process completed")
	s.Stopped <- struct{}{}

}

func (s *Service) GetQueues(ctx context.Context) (*Queues, error) {
	ch := &channels{}
	err := http.Get(ctx, fmt.Sprintf("http://localhost:%d/streaming/channelsz?limit=100000&subs=1", s.appConfig.Broker.MonitoringPort), ch)
	if err != nil {
		return nil, err
	}
	return ch.toQueues("_QUEUES_."), nil
}

func (s *Service) GetEventsStores(ctx context.Context) (*Queues, error) {
	ch := &channels{}
	err := http.Get(ctx, fmt.Sprintf("http://localhost:%d/streaming/channelsz?limit=100000&subs=1", s.appConfig.Broker.MonitoringPort), ch)
	if err != nil {
		return nil, err
	}
	return ch.toQueues("_EVENTS_STORE_."), nil
}

func (s *Service) reportPendingMetrics(ctx context.Context) {
	queuesList, err := s.GetQueues(ctx)
	if err != nil {
		s.logger.Errorf("error on getting queues info for metrics reporting: %s", err.Error())
		return
	}
	for _, queue := range queuesList.Queues {
		for _, client := range queue.clients {
			metrics.ReportPending("queues", client.ClientId, queue.Name, float64(client.Pending))
		}
	}

	eventsList, err := s.GetEventsStores(ctx)
	if err != nil {
		s.logger.Errorf("error on getting events_store info for metrics reporting: %s", err.Error())
		return
	}
	for _, event := range eventsList.Queues {
		for _, client := range event.clients {
			metrics.ReportPending("events_store", client.ClientId, event.Name, float64(client.Pending))
		}
	}

}

func (s *Service) runReportWorker(ctx context.Context) {

	for {
		select {
		case <-time.After(reportTimeInterval):
			if !s.disableMetricsReporting {
				s.reportPendingMetrics(ctx)
			} else {
				return
			}
		case <-ctx.Done():
			return
		}

	}
}

func (s *Service) IsHealthy() bool {
	return s.isHealthy.Load()
}
func (s *Service) IsReady() bool {
	return s.isReady.Load()
}

func (s *Service) HealthState() *HealthState {
	return &HealthState{
		IsHealthy: s.isHealthy.Load(),
		IsReady:   s.isReady.Load(),
	}
}

func (s *Service) RegisterToNotifyState(name string, sub func(state bool)) {
	s.stateNotifiers.Register(name, sub)
}
func (s *Service) UnRegisterToNotifyState(name string) {
	s.stateNotifiers.UnRegister(name)
}

type HealthState struct {
	IsHealthy bool `json:"is_healthy"`
	IsReady   bool `json:"is_ready"`
}
