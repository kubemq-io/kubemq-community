package monitor

import (
	"context"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"strconv"

	"bytes"
	"encoding/json"

	"fmt"
	"os"
	"strings"

	"net/http"

	"github.com/gorilla/websocket"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/labstack/echo/v4"
	"go.uber.org/atomic"
)

func getHostname() string {
	hostname, _ := os.Hostname()
	if strings.Contains("0123456789", hostname[:1]) {
		hostname = "kubemq-" + hostname
	}
	return hostname
}

var hostname = getHostname()

const (
	startStr   = "Broker connected."
	stopStr    = "Broker disconnected."
	unknownStr = "Unknown command"
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

func jsonPrettyPrint(in string) string {
	var out bytes.Buffer
	err := json.Indent(&out, []byte(in), "", "\t")
	if err != nil {
		return in
	}
	return out.String()
}

func websocketHandler(ctx context.Context, c echo.Context, txChan chan string, errCh chan error) error {
	logger := logging.GetLogFactory().NewLogger("monitor_web_socket")
	conn, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		logger.Errorw("error on upgrade to websocket", "error", err.Error())
		return err
	}
	defer conn.Close()
	toSend := atomic.NewBool(false)
	ctrlChan := make(chan string, 10)
	go func() {
		for {
			msgType, data, err := conn.ReadMessage()
			if err != nil {
				logger.Errorw("error on read from websocket", "error", err.Error())
				errCh <- err
				return
			}

			if msgType == 1 {
				rx := string(data)
				logger.Debugw("websocket read", "command", rx)
				switch rx {
				case "start":
					toSend.Swap(true)
					ctrlChan <- fmt.Sprintf("%s %s", hostname, startStr)
				case "stop":
					ctrlChan <- stopStr
					toSend.Swap(false)
				default:
					ctrlChan <- unknownStr
				}
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-txChan:
			if toSend.Load() {
				err := conn.WriteMessage(1, []byte(msg))
				if err != nil {
					logger.Errorw("error on write to websocket", "error", err.Error())
					return err
				}
			}
		case msg := <-ctrlChan:
			err := conn.WriteMessage(1, []byte(msg))
			if err != nil {
				logger.Errorw("error on write to websocket", "error", err.Error())
				return err
			}
		}
	}
}

func MonitorHandlerFunc(ctx context.Context, c echo.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
	}()
	logger := logging.GetLogFactory().NewLogger("monitor_handler")
	monitorRequest, err := paramsToMonitorRequest(c)
	if err != nil {
		logger.Error(err)
		return err
	}
	transportCh := make(chan *Transport, 100)
	errCh := make(chan error, 10)
	switch monitorRequest.Kind {
	case entities.KindTypeEvent, entities.KindTypeEventStore:
		go NewEventsMonitor(ctx, transportCh, monitorRequest, errCh)
	case entities.KindTypeCommand, entities.KindTypeQuery:
		go NewRequestResponseMonitor(ctx, transportCh, monitorRequest, errCh)
	case entities.KindTypeQueue:
		go NewQueueMessagesMonitor(ctx, transportCh, monitorRequest, errCh)
	default:
		return entities.ErrInvalidKind
	}
	txChan := make(chan string, 100)
	logger.Debugw("starting wab socket session", "channel", monitorRequest.Channel, "kind", monitorRequest.Kind, "max_size", monitorRequest.MaxBodySize)
	go func() {
		_ = websocketHandler(ctx, c, txChan, errCh)
	}()
	for {
		select {
		case err := <-errCh:
			logger.Warnw("wab socket session ended with error", "error", err.Error(), "channel", monitorRequest.Channel, "kind", monitorRequest.Kind, "max_size", monitorRequest.MaxBodySize)
			cancel()
			return err
		case tr := <-transportCh:
			if tr.Error == nil {
				buffer, err := TransformToDtoString(tr)
				if err != nil {
					logger.Errorw("error on transform to string", "error", err.Error())
				} else {
					txChan <- buffer
				}
			} else {
				txChan <- tr.String()
			}
			tr.Finish()

		case <-c.Request().Context().Done():
			logger.Debugw("wab socket session ended", "channel", monitorRequest.Channel, "kind", monitorRequest.Kind, "max_size", monitorRequest.MaxBodySize)
			cancel()
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

func paramsToMonitorRequest(c echo.Context) (*MonitorRequest, error) {
	mr := &MonitorRequest{}
	mr.Channel = c.QueryParam("channel")
	switch c.QueryParam("kind") {
	case "events":
		mr.Kind = entities.KindTypeEvent
	case "events_store":
		mr.Kind = entities.KindTypeEventStore
	case "commands":
		mr.Kind = entities.KindTypeCommand
	case "queries":
		mr.Kind = entities.KindTypeQuery
	case "queue":
		mr.Kind = entities.KindTypeQueue
	default:

	}
	mr.MaxBodySize, _ = strconv.Atoi(c.QueryParam("max_size"))
	return mr, mr.validate()
}
