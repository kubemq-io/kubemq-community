package interfaces

import (
	"github.com/kubemq-io/kubemq-community/services"

	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/interfaces/grpc"
	"github.com/kubemq-io/kubemq-community/interfaces/rest"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/services/array"
)

type Options struct {
	AppConfig  *config.Config
	Array      *array.Array
	EnableGRPC bool
	EnableRest bool
}

type Interfaces struct {
	Grpc      *grpc.Server
	Rest      *rest.Server
	appConfig *config.Config
	logger    *logging.Logger
	quit      chan bool
}

func StartInterfaces(svc *services.SystemServices, appConfig *config.Config) (*Interfaces, error) {

	i := &Interfaces{
		Grpc:      nil,
		Rest:      nil,
		appConfig: appConfig,
		logger:    logging.GetLogFactory().NewLogger("connectors-manager"),
		quit:      make(chan bool, 2),
	}

	var err error
	i.Grpc, err = grpc.NewServer(svc, appConfig)
	if err != nil {
		i.logger.Errorw("error loading grpc server", "error", err)
		return nil, err
	}

	i.Rest, err = rest.NewServer(svc)
	if err != nil {
		i.logger.Errorw("error loading rest server", "error", err)
		return nil, err
	}

	return i, nil
}

func (i *Interfaces) Close() {
	i.Grpc.Close()
	i.Rest.Close()
	i.quit <- true
	i.logger.Warnw("connectors shutdown completed")
}
