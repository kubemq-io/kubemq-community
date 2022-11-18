package api

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"github.com/kubemq-io/kubemq-community/services/array"
	"github.com/kubemq-io/kubemq-community/services/broker"
	"strings"

	"github.com/kubemq-io/kubemq-community/services/metrics"

	"net/http"

	"time"

	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/pkg/monitor"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

var StaticAssets embed.FS

type Server struct {
	echoWebServer   *echo.Echo
	logger          *logging.Logger
	context         context.Context
	cancelFunc      context.CancelFunc
	healthFuncs     []func() json.RawMessage
	broker          *broker.Service
	metricsExporter *metrics.Exporter
	apiService      *service
}

var appConfig *config.Config

func customHTTPErrorHandler(err error, c echo.Context) {
	uri := c.Request().RequestURI
	if strings.HasPrefix(uri, "/dash") {
		c.Redirect(301, "/dashboard")
		return
	}
	if strings.HasPrefix(uri, "/que") {
		c.Redirect(301, "/queues")
		return
	}
	if strings.HasPrefix(uri, "/pub") {
		c.Redirect(301, "/pubsub")
		return
	}
	if strings.HasPrefix(uri, "/cq") {
		c.Redirect(301, "/cqrs")
		return
	}

	c.String(404, err.Error())
}
func CreateApiServer(ctx context.Context, broker *broker.Service, appConfigs ...*config.Config) (*Server, error) {
	if len(appConfigs) == 0 {
		appConfig = config.GetAppConfig()
	} else {
		appConfig = appConfigs[0]
	}
	s := &Server{
		logger:          logging.GetLogFactory().NewLogger("api"),
		broker:          broker,
		metricsExporter: metrics.GetExporter(),
	}
	s.apiService = newService(appConfig, broker, s.metricsExporter)
	s.context, s.cancelFunc = context.WithCancel(ctx)
	e := echo.New()
	e.HideBanner = true
	e.Logger = logging.GetLogFactory().NewEchoLogger("server-api")
	e.Use(middleware.Recover())
	e.Use(loggingMiddleware(s.logger))
	e.Use(middleware.CORS())
	e.HTTPErrorHandler = customHTTPErrorHandler
	fs := echo.MustSubFS(StaticAssets, "assets")
	e.StaticFS("/", fs)
	e.GET("/health", func(c echo.Context) error {

		if s.broker.IsHealthy() {
			return c.String(http.StatusOK, "healthy")
		} else {
			return c.String(500, "not healthy")
		}

	})

	e.GET("/ready", func(c echo.Context) error {
		if s.broker.IsReady() {
			return c.JSON(http.StatusOK, s.broker.HealthState())
		} else {
			return c.JSON(500, s.broker.HealthState())
		}
	})
	e.GET("/config", func(c echo.Context) error {
		return c.JSONPretty(http.StatusOK, appConfig, "\t")
	})
	e.GET("/metrics", echo.WrapHandler(s.metricsExporter.PrometheusHandler()))
	statsGroup := e.Group("/v1/stats")
	statsGroup.Use(middleware.GzipWithConfig(middleware.GzipConfig{
		Level: 5,
	}))

	statsGroup.GET("/queues", func(c echo.Context) error {
		return s.getQueueHandler(c)
	})
	statsGroup.GET("/events_stores", func(c echo.Context) error {
		return s.getEventsStoresHandler(c)
	})
	statsGroup.GET("/channels", func(c echo.Context) error {
		return s.getChannelsHandler(c)
	})
	statsGroup.GET("/clients", func(c echo.Context) error {
		return s.getClientsHandler(c)
	})
	statsGroup.GET("/attach", func(c echo.Context) error {
		c.Set("appconf", appConfig)
		_ = monitor.MonitorHandlerFunc(s.context, c)
		return nil
	})
	apiGroup := e.Group("/api")
	apiGroup.GET("/snapshot", func(c echo.Context) error {
		return s.apiService.getSnapshot(c)
	})
	apiGroup.POST("/request", func(c echo.Context) error {
		return s.apiService.handleRequests(c)
	})
	apiGroup.GET("/config", func(c echo.Context) error {
		return c.JSONPretty(http.StatusOK, appConfig, "\t")
	})
	apiGroup.GET("/monitor", func(c echo.Context) error {
		c.Set("appconf", appConfig)
		_ = monitor.MonitorHandlerFunc(s.context, c)
		return nil
	})
	apiGroup.GET("/subscribe/pubsub", func(c echo.Context) error {
		return s.apiService.handlerSubscribeToPubSub(c)
	})
	apiGroup.GET("/subscribe/cqrs", func(c echo.Context) error {
		return s.apiService.handlerSubscribeToCQRS(c)
	})
	apiGroup.GET("/stream_queue_messages", func(c echo.Context) error {
		return s.apiService.handlerStreamQueueMessages(c)
	})
	apiGroup.GET("/connection", func(c echo.Context) error {
		return s.apiService.handlerConnectionStatus(c)
	})
	e.Server.ReadTimeout = time.Duration(180) * time.Second
	e.Server.WriteTimeout = time.Duration(180) * time.Second
	s.echoWebServer = e

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.echoWebServer.Start(fmt.Sprintf("0.0.0.0:%d", appConfig.Api.Port))
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return nil, err
		}
		e.Logger.Infof("api server started at port %d", appConfig.Api.Port)
		return s, nil
	case <-time.After(1 * time.Second):
		return s, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("error strarting api server, %w", ctx.Err())
	}

}
func (s *Server) InitApiService(ctx context.Context, array *array.Array) error {
	return s.apiService.init(ctx, s.logger, array)
}
func (s *Server) AddHealthFunc(fn func() json.RawMessage) {
	s.healthFuncs = append(s.healthFuncs, fn)
}

func (s *Server) Close() {
	_ = s.echoWebServer.Shutdown(context.Background())
	_ = s.apiService.stop()
	s.cancelFunc()
}
func loggingMiddleware(l *logging.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) (err error) {
			l.Debugw("start api call", "method", c.Request().RequestURI, "sender", c.Request().RemoteAddr)
			c.Set("logger", l)
			err = next(c)
			val := c.Get("result")
			res, ok := val.(*Response)
			if ok {
				if res.Error {
					l.Debugw("api call ended with error", "method", c.Request().RequestURI, "sender", c.Request().RemoteAddr, "error", res.ErrorString)
				} else {
					l.Debugw("api call ended successfully", "method", c.Request().RequestURI, "sender", c.Request().RemoteAddr, "result", res.Data)
				}

			}
			return err
		}
	}
}
