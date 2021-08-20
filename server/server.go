package server

import (
	"context"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/interfaces"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/services"
	"runtime"
)

type Server struct {
	version    string
	appConfig  *config.Config
	logger     *logging.Logger
	ctx        context.Context
	cancelFunc context.CancelFunc
	services   *services.SystemServices
	connectors *interfaces.Interfaces
}

func New() *Server {
	return &Server{}
}
func (s *Server) SetVersion(version string) *Server {
	s.version = version
	return s
}

func (s *Server) Run() error {
	s.ctx, s.cancelFunc = context.WithCancel(context.Background())
	s.appConfig = config.GetAppConfig("./","./config")
	err := s.appConfig.Validate()
	if err != nil {
		return err
	}
	s.appConfig.SetVersion(s.version)
	logging.CreateLoggerFactory(s.ctx, s.appConfig.Host, s.appConfig.Log)

	s.logger = logging.GetLogFactory().NewLogger("server")
	s.logger.Infof("starting kubemq community edition version: %s, cores: %d", s.version, runtime.NumCPU())
	s.services, err = services.Start(s.ctx, s.appConfig)
	if err != nil {
		return err
	}
	s.connectors, err = interfaces.StartInterfaces(s.services, s.appConfig)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) Close() {
	s.logger.Warnw("kubemq server shutdown started")
	if s.connectors != nil {
		s.connectors.Close()
	}

	s.services.Close()
	<-s.services.Stopped
	s.logger.Warnw("kubemq server shutdown completed")
	logging.Close()
}

