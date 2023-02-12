package rest

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/atomic"

	"github.com/kubemq-io/kubemq-community/services"

	"github.com/kubemq-io/kubemq-community/pkg/logging"

	"net/http"
	"time"

	"github.com/kubemq-io/kubemq-community/config"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type Server struct {
	services      *services.SystemServices
	appConfig     *config.Config
	echoWebServer *echo.Echo
	logger        *logging.Logger
	isTLS         bool
	acceptTraffic *atomic.Bool
}

func (s *Server) Close() {
	s.acceptTraffic.Store(false)
	if s.services.Broker != nil {
		s.services.Broker.UnRegisterToNotifyState("rest")
	}
	_ = s.echoWebServer.Shutdown(context.Background())
}

func getCORSConfig(appConfig *config.Config) middleware.CORSConfig {
	conf := middleware.CORSConfig{
		Skipper:          nil,
		AllowOrigins:     appConfig.Rest.Cors.AllowOrigins,
		AllowMethods:     appConfig.Rest.Cors.AllowMethods,
		AllowHeaders:     appConfig.Rest.Cors.AllowHeaders,
		AllowCredentials: appConfig.Rest.Cors.AllowCredentials,
		ExposeHeaders:    appConfig.Rest.Cors.ExposeHeaders,
		MaxAge:           appConfig.Rest.Cors.MaxAge,
	}
	return conf
}
func NewServer(svc *services.SystemServices, appConfigs ...*config.Config) (*Server, error) {
	var appConfig *config.Config
	if len(appConfigs) == 0 {
		appConfig = config.GetAppConfig()
	} else {
		appConfig = appConfigs[0]
	}

	s := &Server{
		services:      svc,
		appConfig:     appConfig,
		echoWebServer: nil,
		logger:        logging.GetLogFactory().NewLogger("rest-interface"),
		isTLS:         false,
		acceptTraffic: atomic.NewBool(false),
	}
	kubeversion = fmt.Sprintf("KubeMQ %s", appConfig.GetVersion())

	e := echo.New()
	s.echoWebServer = e
	e.Logger = logging.GetLogFactory().NewEchoLogger("rest-api")
	e.HideBanner = true
	e.Use(middleware.Recover())
	e.Use(Traffic(s.acceptTraffic))
	e.HTTPErrorHandler = s.customHTTPErrorHandler
	e.Use(middleware.CORSWithConfig(getCORSConfig(appConfig)))
	e.Use(Logging(s.logger))

	if svc.Broker != nil {
		s.acceptTraffic.Store(svc.Broker.IsReady())
		s.UpdateBrokerStatus(s.acceptTraffic.Load())
		svc.Broker.RegisterToNotifyState("rest", s.UpdateBrokerStatus)
	}
	if appConfig.Rest.BodyLimit != "" {
		e.Use(middleware.BodyLimit(appConfig.Rest.BodyLimit))
	}

	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "hi")
	})

	// route to websocket of subscribe to commands
	e.GET("/subscribe/events", func(c echo.Context) error {

		return s.handlerSubscribeToEvents(c, appConfig)
	})
	// route to websocket of subscribe to request
	e.GET("/subscribe/requests", func(c echo.Context) error {

		return s.handlerSubscribeToRequests(c, appConfig)
	})

	// route to websocket of sending stream of messages
	e.GET("/send/stream", func(c echo.Context) error {

		return s.handlerSendMessageStream(c)

	})
	// route to post message
	e.POST("/send/request", func(c echo.Context) error {
		return s.handlerSendRequest(c)
	})
	// route to post message
	e.POST("/send/event", func(c echo.Context) error {
		return s.handlerSendMessage(c)
	})
	// route to post message
	e.POST("/queue/send", func(c echo.Context) error {
		return s.handlerSendQueueMessage(c)
	})

	// route to post message
	e.POST("/queue/send_batch", func(c echo.Context) error {
		return s.handlerSendBatchQueueMessages(c)
	})

	// route to get message
	e.POST("/queue/receive", func(c echo.Context) error {
		return s.handlerReceiveQueueMessages(c)
	})

	// route to get message
	e.GET("/queue/stream", func(c echo.Context) error {
		return s.handlerStreamQueueMessages(c)
	})
	// route to get queues info
	e.GET("/queue/info", func(c echo.Context) error {
		return s.handlerQueueInfo(c)
	})
	// route to get message
	e.GET("/ping", func(c echo.Context) error {
		return s.handlerPing(c)
	})
	// route to post request
	e.POST("/queue/ack_all", func(c echo.Context) error {
		return s.handlerAckAllQueueMessages(c)
	})

	// route to post response
	e.POST("/send/response", func(c echo.Context) error {
		return s.handlerSendResponse(c)
	})

	e.Server.ReadTimeout = time.Duration(appConfig.Rest.ReadTimeout) * time.Second
	e.Server.WriteTimeout = time.Duration(appConfig.Rest.WriteTimeout) * time.Second
	err := s.Run()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Server) Run() error {
	conf := s.appConfig.Rest
	switch s.appConfig.Security.Mode() {
	case config.SecurityModeNone:
		s.logger.Infof("started insecure rest server at port %d", conf.Port)
		go func() {
			_ = s.echoWebServer.Start(fmt.Sprintf(":%d", conf.Port))
		}()

		return nil
	case config.SecurityModeTLS, config.SecurityModeMTLS:
		certBlock, err := s.appConfig.Security.Cert.Get()
		if err != nil {

			return errors.Errorf("secured rest server not started, invalid cert data : %s", err.Error())
		}
		keyBlock, err := s.appConfig.Security.Key.Get()
		if err != nil {
			return errors.Errorf("secured rest server not started,invalid key data: %s", err.Error())
		}
		s.logger.Warnw("started secured rest server at port %d", conf.Port)
		go func() {
			err = s.echoWebServer.StartTLS(fmt.Sprintf(":%d", conf.Port), certBlock, keyBlock)
			if err != nil {
				s.logger.Errorf(err.Error())
			}
		}()

		return nil
	}
	return nil
}

func Logging(l *logging.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) (err error) {
			l.Debugw("start rest call", "method", c.Request().RequestURI, "sender", c.Request().RemoteAddr)
			err = next(c)

			if err != nil {
				l.Debugw("rpc rest ends with error", "method", c.Request().RequestURI, "sender", c.Request().RemoteAddr, "error", err.Error())
			} else {
				l.Debugw("rpc rest ended successfully", "method", c.Request().RequestURI, "sender", c.Request().RemoteAddr)
			}
			return err
		}
	}
}

type ErrorMessage struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

func (s *Server) customHTTPErrorHandler(err error, c echo.Context) {

	if he, ok := err.(*echo.HTTPError); ok {
		code := he.Code
		_ = c.JSON(code, &ErrorMessage{
			ErrorCode: he.Code,
			Message:   he.Message.(string),
		})
		s.logger.Errorw(he.Error(), "method", c.Request().RequestURI, "sender", c.Request().RemoteAddr)
	}
}
func (s *Server) UpdateBrokerStatus(state bool) {
	s.acceptTraffic.Store(state)
	if state {
		s.logger.Warn("rest interface is accepting traffic")
	} else {
		s.logger.Warn("rest interface is not accepting traffic, waiting for broker")
	}
}
