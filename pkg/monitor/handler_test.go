package monitor

import (
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"os"
	"testing"

	"github.com/kubemq-io/kubemq-community/services/broker"

	"github.com/fortytw2/leaktest"

	"net/http"

	"fmt"

	"context"

	"encoding/json"
	"github.com/gorilla/websocket"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/client"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/stretchr/testify/require"
)

func setupRestApi(ctx context.Context, t *testing.T) (*client.Client, *config.Config, *echo.Echo, *broker.Service) {
	c, appConfig, ns := setup(ctx, t)
	e := echo.New()
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())
	e.GET("/ws", func(c echo.Context) error {
		c.Set("appconf", appConfig)
		err := MonitorHandlerFunc(context.Background(), c)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error())
		}
		return c.String(http.StatusOK, "ok")
	})
	go func() {
		_ = e.Start(fmt.Sprintf(":%d", appConfig.Api.Port))
	}()

	return c, appConfig, e, ns
}

func tearDownRestApi(c *client.Client, e *echo.Echo, a *config.Config, ns *broker.Service) {
	_ = c.Disconnect()
	_ = e.Shutdown(context.Background())
	ns.Close()
	<-ns.Stopped
	err := os.RemoveAll(a.Store.StorePath)
	if err != nil {
		panic(err)
	}
}

func runWebsocketClientReaderWriter(ctx context.Context, t *testing.T, uri string, chRead chan string, chWrite chan string) {
	c, res, err := websocket.DefaultDialer.Dial(uri, nil)
	if err != nil {
		buf := make([]byte, 1024)
		n, err := res.Body.Read(buf)
		if err != nil {
			chRead <- err.Error()
		} else {
			chRead <- string(buf[:n])
		}
		return
	}
	defer c.Close()
	go func() {
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				chRead <- err.Error()

			} else {
				in := string(message)
				chRead <- in
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	for {
		select {
		case msg := <-chWrite:
			err := c.WriteMessage(1, []byte(msg))
			if err != nil {
				chRead <- err.Error()
				return
			}
		case <-ctx.Done():
			return

		}
	}

}

func TestMonitorHandlerFunc_Start_Stop(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, appConfig, e, ns := setupRestApi(ctx, t)
	defer tearDownRestApi(c, e, appConfig, ns)
	channel := "TestMonitorHandlerFunc_Start_Stop"
	urlStream := fmt.Sprintf("ws://localhost:%d/ws?channel=%s&kind=%s&client_id=some_client_id", appConfig.Api.Port, channel, "events")
	rxChan := make(chan string, 10)
	txChan := make(chan string, 10)
	go runWebsocketClientReaderWriter(ctx, t, urlStream, rxChan, txChan)
	sleep(1)
	txChan <- "start"
	require.Equal(t, fmt.Sprintf("%s %s", hostname, startStr), <-rxChan)
	txChan <- "stop"
	require.Equal(t, stopStr, <-rxChan)
	txChan <- "start"
	require.Equal(t, fmt.Sprintf("%s %s", hostname, startStr), <-rxChan)
	txChan <- "some_unknown_command"
	require.Equal(t, unknownStr, <-rxChan)

}

func TestMonitorHandlerFunc_ParamsError(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, appConfig, e, ns := setupRestApi(ctx, t)
	defer tearDownRestApi(c, e, appConfig, ns)

	t.Run("error_no_channel", func(t *testing.T) {
		urlStream := fmt.Sprintf("ws://localhost:%d/ws?kind=%s&client_id=some_client_id", appConfig.Api.Port, "events")
		rxChan := make(chan string, 10)
		txChan := make(chan string, 10)
		go runWebsocketClientReaderWriter(ctx, t, urlStream, rxChan, txChan)
		sleep(1)
		txChan <- "start"
		require.Equal(t, entities.ErrInvalidChannel.Error(), <-rxChan)
	})
	t.Run("error_invalid_kind", func(t *testing.T) {
		urlStream := fmt.Sprintf("ws://localhost:%d/ws?channel=some-channel&client_id=some_client_id", appConfig.Api.Port)
		rxChan := make(chan string, 10)
		txChan := make(chan string, 10)
		go runWebsocketClientReaderWriter(ctx, t, urlStream, rxChan, txChan)
		sleep(1)
		txChan <- "start"
		require.Equal(t, entities.ErrInvalidKind.Error(), <-rxChan)
	})

}

func TestMonitorHandlerFunc_Events(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, appConfig, e, ns := setupRestApi(ctx, t)
	defer tearDownRestApi(c, e, appConfig, ns)

	channel := "TestMonitorHandlerFunc_Events"
	msg := &pb.Event{Channel: channel, Metadata: "some-metadata", Body: []byte("some-body"), ClientID: "some-pub-client-id"}
	urlStream := fmt.Sprintf("ws://localhost:%d/ws?channel=%s&kind=%s&client_id=some_client_id", appConfig.Api.Port, channel, "events")
	rxChan := make(chan string, 10)
	txChan := make(chan string, 10)

	go runWebsocketClientReaderWriter(ctx, t, urlStream, rxChan, txChan)
	sleep(1)
	txChan <- "start"
	require.Equal(t, fmt.Sprintf("%s %s", hostname, startStr), <-rxChan)
	result, err := c.SendEvents(ctx, msg)
	require.NoError(t, err)
	require.True(t, result.Sent)
	rxMsg := &pb.EventReceive{}
	msgRcvStr := <-rxChan

	err = json.Unmarshal([]byte(msgRcvStr), rxMsg)
	require.NoError(t, err)
	require.EqualValues(t, msg.Channel, rxMsg.Channel)

}
