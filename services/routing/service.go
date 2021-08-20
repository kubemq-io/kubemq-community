package routing

import (
	"context"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/routing"
	pb "github.com/kubemq-io/protobuf/go"
	"time"
)

var singletonRoutingController *Service

type Options struct {
	InMemory      string
	FilePath      string
	WebServiceUrl string
}
type Service struct {
	router *routing.Controller
}

func SetSingleton(s *Service) {
	singletonRoutingController = s
}

func GetSingleton() *Service {
	return singletonRoutingController
}
func CreateRoutingService(ctx context.Context, appConfig *config.Config) (*Service, error) {
	var opts *Options
	var err error
	var reloadInterval time.Duration
	if appConfig.Routing != nil {
		opts = &Options{
			InMemory:      appConfig.Routing.Data,
			FilePath:      appConfig.Routing.FilePath,
			WebServiceUrl: appConfig.Routing.Url,
		}
		reloadInterval = time.Duration(appConfig.Routing.AutoReload) * time.Second
	}
	router, err := routing.NewController(ctx, getProvider(opts), reloadInterval)
	if err != nil {
		return nil, err
	}
	return &Service{
		router: router,
	}, nil
}

func (s *Service) Close() {
	if s.router != nil {
		s.router.Close()
	}
}
func getProvider(opts *Options) routing.Provider {
	if opts == nil {
		return nil
	}
	if opts.InMemory != "" {
		return routing.NewMemoryProvider([]byte(opts.InMemory))
	}
	if opts.FilePath != "" {
		return routing.NewFileProvider(opts.FilePath)
	}
	if opts.WebServiceUrl != "" {
		return routing.NewWebServiceProvider(opts.WebServiceUrl)
	}

	return nil

}

func (s *Service) CreateRouteMessages(channel string, msg interface{}) (*RouteMessages, error) {
	rm := &RouteMessages{
		Route:         s.router.GetRoutes(channel),
		Events:        nil,
		EventsStore:   nil,
		QueueMessages: nil,
	}
	var err error
	switch v := msg.(type) {
	case *pb.Event:
		if !v.Store {
			rm, err = rm.processEventsRouting(v)
		} else {
			rm, err = rm.processEventsStoreRouting(v)
		}
	case *pb.QueueMessage:
		rm, err = rm.processQueueMessageRouting(v)
	}
	if err != nil {
		return nil, err
	}
	return rm, nil
}
