package monitor

import (
	"fmt"
	"github.com/kubemq-io/broker/pkg/pipe"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/nats-io/nuid"

	pb "github.com/kubemq-io/protobuf/go"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kubemq-io/kubemq-community/services/broker"

	"github.com/fortytw2/leaktest"

	"context"

	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/client"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func sleep(i int) {
	time.Sleep(time.Duration(i) * time.Second)
}
func init() {
	_ = logging.CreateLoggerFactory(context.Background(), "test", &config.LogConfig{
		Level: "debug",
	})

}

var testPort = *atomic.NewInt32(26788)

func setup(ctx context.Context, t *testing.T) (*client.Client, *config.Config, *broker.Service) {
	tmp, err := ioutil.TempDir("./", "test_monitor")
	require.NoError(t, err)
	appConfig := config.GetAppConfig("../../test/")

	appConfig.Broker.MonitoringPort = int(testPort.Inc())
	appConfig.Api.Port = int(testPort.Inc())
	appConfig.Store.StorePath = tmp
	appConfig.Broker.MemoryPipe = pipe.NewPipe(nuid.Next())
	brokerServer := broker.New(appConfig)
	brokerServer, err = brokerServer.Start(ctx)
	require.NoError(t, err)
	brokerServer.DisableMetricsReporting()
	timer := time.NewTimer(10 * time.Second)
	for {
		if brokerServer.IsReady() {
			break
		}

		select {
		case <-timer.C:
			require.True(t, brokerServer.IsReady())
		default:

		}
	}
	ncOpts := client.NewClientOptions("monitor-test-client" + nuid.Next()).
		SetMemoryPipe(appConfig.Broker.MemoryPipe)

	natsClient, err := client.NewClient(ncOpts)
	require.NoError(t, err)
	require.NotNil(t, natsClient)

	return natsClient, appConfig, brokerServer

}
func setupWithStoreClient(ctx context.Context, t *testing.T) (*client.Client, *config.Config, *broker.Service) {
	tmp, err := ioutil.TempDir("./", "test_monitor")
	require.NoError(t, err)
	appConfig := config.GetAppConfig("../../test/")
	appConfig.Broker.MemoryPipe = pipe.NewPipe(nuid.Next())
	appConfig.Broker.MonitoringPort = int(testPort.Inc())
	appConfig.Api.Port = int(testPort.Inc())
	appConfig.Store.StorePath = tmp
	brokerServer := broker.New(appConfig)
	brokerServer, err = brokerServer.Start(ctx)
	require.NoError(t, err)
	brokerServer.DisableMetricsReporting()
	timer := time.NewTimer(10 * time.Second)
	for {
		if brokerServer.IsReady() {
			break
		}

		select {
		case <-timer.C:
			require.True(t, brokerServer.IsReady())
		default:

		}
	}
	ncOpts := client.NewClientOptions(nuid.Next()).
		SetMemoryPipe(appConfig.Broker.MemoryPipe)

	nc, err := client.NewStoreClient(ncOpts)
	require.NoError(t, err)
	require.NotNil(t, nc)

	return nc, appConfig, brokerServer

}

func setupWithQueueClient(ctx context.Context, t *testing.T) (*client.QueueClient, *config.Config, *broker.Service) {
	tmp, err := ioutil.TempDir("./", "test_monitor")
	require.NoError(t, err)
	appConfig := config.GetAppConfig("../../test/")
	appConfig.Broker.MonitoringPort = int(testPort.Inc())
	appConfig.Api.Port = int(testPort.Inc())
	appConfig.Store.StorePath = tmp
	brokerServer := broker.New(appConfig)
	brokerServer, err = brokerServer.Start(ctx)
	require.NoError(t, err)
	brokerServer.DisableMetricsReporting()
	timer := time.NewTimer(10 * time.Second)
	for {
		if brokerServer.IsReady() {
			break
		}

		select {
		case <-timer.C:
			require.True(t, brokerServer.IsReady())
		default:

		}
	}
	ncOpts := client.NewClientOptions(nuid.Next()).
		SetMemoryPipe(appConfig.Broker.MemoryPipe)
	nc, err := client.NewQueueClient(ncOpts, appConfig.Queue)
	require.NoError(t, err)
	require.NotNil(t, nc)
	return nc, appConfig, brokerServer

}

func tearDown(c *client.Client, a *config.Config, ns *broker.Service) {
	_ = c.Disconnect()
	ns.Close()
	<-ns.Stopped
	err := os.RemoveAll(a.Store.StorePath)
	if err != nil {
		panic(err)
	}
}

func tearDownQueueClient(c *client.QueueClient, a *config.Config, ns *broker.Service) {
	_ = c.Disconnect()
	ns.Close()
	<-ns.Stopped
	err := os.RemoveAll(a.Store.StorePath)
	if err != nil {
		panic(err)
	}
}
func TestNewEventsMonitor(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	c, appConfig, ns := setup(ctx, t)
	defer tearDown(c, appConfig, ns)
	defer cancel()
	channel := "TestNewEventsMonitor"
	msg := &pb.Event{ClientID: "test-client-id", Channel: channel, Metadata: "some-metadata", Body: []byte("some-body")}
	errChan := make(chan error, 10)
	rxChan := make(chan *Transport, 10)

	mr := &MonitorRequest{
		Kind:        entities.KindTypeEvent,
		Channel:     channel,
		MaxBodySize: 0,
	}
	go NewEventsMonitor(ctx, rxChan, mr, errChan)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:

			}
			result, err := c.SendEvents(ctx, msg)
			require.NoError(t, err)
			require.True(t, result.Sent)
			sleep(1)

		}
	}()

	rxMsg := <-rxChan
	newMsg := &pb.EventReceive{}
	err := rxMsg.Unmarshal(newMsg)
	require.NoError(t, err)
	require.EqualValues(t, msg.Body, newMsg.Body)
}

func TestNewEventStoreMonitor(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	c, appConfig, ns := setupWithStoreClient(ctx, t)
	defer tearDown(c, appConfig, ns)
	defer cancel()
	channel := "TestNewPublishMonitor"
	msg := &pb.Event{ClientID: "test-client-id", Channel: channel, Metadata: "some-metadata", Body: []byte("some-body"), Store: true}
	errChan := make(chan error, 10)
	rxChan := make(chan *Transport, 10)
	mr := &MonitorRequest{
		Kind:        entities.KindTypeEventStore,
		Channel:     channel,
		MaxBodySize: 0,
	}
	go NewEventsMonitor(ctx, rxChan, mr, errChan)
	time.Sleep(2 * time.Second)
	result, err := c.SendEventsStore(ctx, msg)
	require.NoError(t, err)
	require.True(t, result.Sent)
	sleep(1)
	select {
	case rxMsg := <-rxChan:
		newMsg := &pb.EventReceive{}
		err = rxMsg.Unmarshal(newMsg)
		assert.NoError(t, err)
		assert.EqualValues(t, msg.Body, newMsg.Body)
	case <-time.After(5 * time.Second):
		require.True(t, false)
	}
}

func TestNewEventMonitorWithError(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	c, appConfig, ns := setup(ctx, t)
	defer tearDown(c, appConfig, ns)
	defer cancel()
	channel := "TestNewPublishMonitor"
	errChan := make(chan error, 10)
	rxChan := make(chan *Transport, 10)

	mr := &MonitorRequest{
		Kind:        entities.KindTypeBlank,
		Channel:     channel,
		MaxBodySize: 0,
	}
	go NewEventsMonitor(ctx, rxChan, mr, errChan)
	err := <-errChan
	require.Error(t, err)
}

func TestNewQueryRequestMonitor(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	c, appConfig, ns := setup(ctx, t)
	defer tearDown(c, appConfig, ns)
	defer cancel()

	t.Run("query_with_no_response", func(t *testing.T) {
		channel := "TestNewRequestMonitor"
		request := &pb.Request{
			RequestID:            "some_id",
			RequestTypeData:      pb.Request_Query,
			ClientID:             "test-client-id",
			Channel:              channel,
			Metadata:             "some-metadata",
			Body:                 []byte("some-body"),
			ReplyChannel:         "",
			Timeout:              100,
			CacheKey:             "",
			CacheTTL:             0,
			Span:                 nil,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		errChan := make(chan error, 10)
		rxChan := make(chan *Transport, 10)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		mr := &MonitorRequest{
			Kind:        entities.KindTypeQuery,
			Channel:     channel,
			MaxBodySize: 0,
		}
		go NewRequestResponseMonitor(ctx, rxChan, mr, errChan)
		sleep(1)
		go func() {
			_, err := c.SendQuery(context.Background(), request)
			require.Error(t, err)
			re := NewResponseErrorFromRequest(request, entities.ErrRequestTimeout)
			msg := re.ToMessage(fmt.Sprintf("%s.%s", responseChannelPrefix, channel))
			result, err := c.SendEvents(ctx, msg)
			require.NoError(t, err)
			require.True(t, result.Sent)

		}()

		for rxMsg := range rxChan {
			switch rxMsg.Kind {
			case "query":
				newReq := &pb.Request{}
				err := rxMsg.Unmarshal(newReq)
				require.NoError(t, err)
				require.EqualValues(t, request.RequestID, newReq.RequestID)
			case "response_error":
				newResErr := &ResponseError{}
				err := rxMsg.Unmarshal(newResErr)
				require.NoError(t, err)
				require.EqualValues(t, request.RequestID, newResErr.RequestID)
				return
			}
			rxMsg.Finish()
		}
	})
	t.Run("query_with_response", func(t *testing.T) {
		channel := "TestNewRequestMonitor"
		request := &pb.Request{
			RequestID:            "some_id",
			RequestTypeData:      pb.Request_Query,
			ClientID:             "test-client-id",
			Channel:              channel,
			Metadata:             "some-metadata",
			Body:                 []byte("some-body"),
			ReplyChannel:         "",
			Timeout:              100,
			CacheKey:             "",
			CacheTTL:             0,
			Span:                 nil,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		errChan := make(chan error, 10)
		rxChan := make(chan *Transport, 10)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		mr := &MonitorRequest{
			Kind:        entities.KindTypeQuery,
			Channel:     channel,
			MaxBodySize: 0,
		}
		go NewRequestResponseMonitor(ctx, rxChan, mr, errChan)

		sleep(1)
		go func() {
			_, err := c.SendQuery(ctx, request)
			require.Error(t, err)
			res := &pb.Response{ClientID: "test-client-id", RequestID: request.RequestID}
			msg := responseToMessage(res, fmt.Sprintf("%s.%s", responseChannelPrefix, channel))
			result, err := c.SendEvents(ctx, msg)
			require.NoError(t, err)
			require.True(t, result.Sent)
		}()
		for rxMsg := range rxChan {
			switch rxMsg.Kind {
			case "query":
				newReq := &pb.Request{}
				err := rxMsg.Unmarshal(newReq)
				require.NoError(t, err)
				require.EqualValues(t, request.RequestID, newReq.RequestID)
			case "response":
				newRes := &pb.Response{}
				err := rxMsg.Unmarshal(newRes)
				require.NoError(t, err)
				require.EqualValues(t, request.RequestID, newRes.RequestID)
				return
			}
			rxMsg.Finish()
		}
	})
}

func TestMonitor_Queue(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	c, appConfig, ns := setupWithQueueClient(ctx, t)
	defer tearDownQueueClient(c, appConfig, ns)
	defer cancel()

	channel := "TestQueueChannel"
	errChan := make(chan error, 10)
	rxChan := make(chan *Transport, 10)
	mr := &MonitorRequest{
		Kind:        entities.KindTypeQueue,
		Channel:     channel,
		MaxBodySize: 0,
	}
	go NewQueueMessagesMonitor(ctx, rxChan, mr, errChan)

	go func() {
		sleep(1)
		result := c.SendQueueMessage(ctx, &pb.QueueMessage{
			MessageID:  "some-message-id",
			ClientID:   "some-client-id",
			Channel:    channel,
			Metadata:   "",
			Body:       []byte("some-queue-message"),
			Tags:       nil,
			Attributes: nil,
			Policy:     nil,
		})
		require.False(t, result.IsError)
	}()
	sleep(2)

	rxMsg := <-rxChan
	switch rxMsg.Kind {
	case "queue":
		msg := &pb.QueueMessage{}
		err := rxMsg.Unmarshal(msg)
		require.NoError(t, err)
		require.Equal(t, "some-message-id", msg.MessageID)
		rxMsg.Finish()

	}

}
