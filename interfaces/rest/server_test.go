package rest

import (
	"encoding/json"
	"fmt"
	"github.com/kubemq-io/broker/pkg/pipe"
	test2 "github.com/kubemq-io/kubemq-community/interfaces/test"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/nats-io/nuid"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	sdk "github.com/kubemq-io/kubemq-go"

	"github.com/go-resty/resty/v2"
	"github.com/kubemq-io/kubemq-community/pkg/logging"

	"github.com/kubemq-io/kubemq-community/services"

	"github.com/kubemq-io/kubemq-community/config"

	"context"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

var testPort = atomic.NewInt32(52222)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	metrics.InitExporter(ctx)

	os.Exit(m.Run())
}
func init() {
	_ = logging.CreateLoggerFactory(context.Background(), "test", &config.LogConfig{
		Level: "debug",
	})

}
func getRestyRequest() *resty.Request {
	r := resty.New().R()
	return r
}
func getAppConfig() *config.Config {
	tmp, err := ioutil.TempDir("./", "test_rest")
	if err != nil {
		panic(err)
	}
	appConfig := config.GetCopyAppConfig("../../test/")
	appConfig.Rest.Port = int(testPort.Inc())
	appConfig.Broker.MonitoringPort = int(testPort.Inc())
	appConfig.Broker.MemoryPipe = pipe.NewPipe(nuid.Next())
	appConfig.Store.StorePath = tmp
	return appConfig
}
func setup(ctx context.Context, t *testing.T, appConfigs ...*config.Config) (int, *services.SystemServices, *Server) {
	var appConfig *config.Config
	if len(appConfigs) == 0 {
		appConfig = getAppConfig()
	} else {
		appConfig = appConfigs[0]
	}

	svc, err := services.Start(ctx, appConfig)
	if err != nil {
		panic(err)
	}
	var s *Server
	s, err = NewServer(svc, appConfig)
	if err != nil {
		panic(err)
	}
	svc.Broker.DisableMetricsReporting()
	//time.Sleep(1 * time.Second)
	return appConfig.Rest.Port, svc, s
}
func tearDown(svc *services.SystemServices, s *Server) {

	s.Close()

	svc.Close()
	<-svc.Stopped

	err := os.RemoveAll(svc.AppConfig.Store.StorePath)
	if err != nil {
		panic(err)
	}

}

func runWebsocketClientReader(ctx context.Context, t *testing.T, uri string, ch chan string) error {
	header := http.Header{}
	c, res, err := websocket.DefaultDialer.Dial(uri, header)
	if err != nil {
		buf := make([]byte, 1024)
		if res != nil {
			n, err := res.Body.Read(buf)
			if err != nil {
				ch <- err.Error()
			} else {
				ch <- string(buf[:n])
			}
		}
		return err
	}
	defer func() {
		_ = c.Close()
	}()
	go func() {
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				ch <- err.Error()
				return
			} else {
				ch <- string(message)
			}
		}

	}()
	<-ctx.Done()
	return nil

}

func runWebsocketClientWriter(ctx context.Context, t *testing.T, uri string, chRead chan string, chWrite chan string) error {
	header := http.Header{}
	c, res, err := websocket.DefaultDialer.Dial(uri, header)
	if err != nil {

		buf := make([]byte, 1024)
		n, err := res.Body.Read(buf)
		if err != nil {
			chRead <- err.Error()
		} else {
			chRead <- string(buf[:n])
		}
		return err
	}
	defer func() {
		_ = c.Close()
	}()

	go func() {
		for {
			_, message, err := c.ReadMessage()

			if err != nil {
				chRead <- err.Error()
				return
			} else {
				chRead <- string(message)
			}
		}
	}()

	for {
		select {
		case msg := <-chWrite:
			err := c.WriteMessage(1, []byte(msg))
			if err != nil {
				chWrite <- "err"
				return err
			}

		case <-ctx.Done():
			return nil
		}
	}

}

func waitForCount(fn func() int, required, timeout int) error {
	for i := 0; i < 10*timeout; i++ {
		if fn() == required {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout, wantResponses %d, got %d", required, fn())
}

func TestRestServer_Queue_SendAndReceive(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	port, svc, s := setup(ctx, t)
	defer tearDown(svc, s)
	client, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest))
	require.NoError(t, err)
	defer client.Close()
	channel := nuid.Next()
	messages := client.QMB()
	for i := 0; i < 10; i++ {
		msg := client.QM()
		msg.SetId(nuid.Next()).SetChannel(channel).SetBody([]byte(fmt.Sprintf("msg #%d", i)))
		messages.Add(msg)
	}
	res, err := messages.Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 10, len(res))

	rxRes, err := client.RQM().SetChannel(channel).SetMaxNumberOfMessages(10).SetWaitTimeSeconds(10).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 10, len(rxRes.Messages))
}

func TestRestServer_Queue_SendAndReceiveWithPeak(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	port, svc, s := setup(ctx, t)
	defer tearDown(svc, s)
	client, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId("some-client-id"), sdk.WithTransportType(sdk.TransportTypeRest))
	require.NoError(t, err)
	channel := nuid.Next()
	messages := client.QMB()
	for i := 0; i < 10; i++ {
		msg := client.QM()
		msg.SetId(nuid.Next()).SetChannel(channel).SetBody([]byte(fmt.Sprintf("msg #%d", i)))
		messages.Add(msg)
	}
	res, err := messages.Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 10, len(res))

	time.Sleep(100 * time.Millisecond)
	rxRes, err := client.RQM().SetChannel(channel).SetMaxNumberOfMessages(5).SetWaitTimeSeconds(1).SetIsPeak(true).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 5, len(rxRes.Messages))

	time.Sleep(100 * time.Millisecond)
	rxRes, err = client.RQM().SetChannel(channel).SetMaxNumberOfMessages(10).SetWaitTimeSeconds(1).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 10, len(rxRes.Messages))

}

func TestRestServer_Queue_SendAndReceiveWithAckAllAndPeak(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	port, svc, s := setup(ctx, t)
	defer tearDown(svc, s)
	client, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId("some-client-id"), sdk.WithTransportType(sdk.TransportTypeRest))
	require.NoError(t, err)
	channel := nuid.Next()
	messages := client.QMB()
	for i := 0; i < 10; i++ {
		msg := client.QM()
		msg.SetId(nuid.Next()).SetChannel(channel).SetBody([]byte(fmt.Sprintf("msg #%d", i)))
		messages.Add(msg)
	}
	res, err := messages.Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 10, len(res))

	time.Sleep(100 * time.Millisecond)
	rxRes, err := client.RQM().SetChannel(channel).SetMaxNumberOfMessages(5).SetWaitTimeSeconds(1).SetIsPeak(true).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 5, len(rxRes.Messages))

	time.Sleep(100 * time.Millisecond)
	rxRes, err = client.RQM().SetChannel(channel).SetMaxNumberOfMessages(5).SetWaitTimeSeconds(1).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 5, len(rxRes.Messages))

	time.Sleep(100 * time.Millisecond)
	rxRes, err = client.RQM().SetChannel(channel).SetMaxNumberOfMessages(5).SetWaitTimeSeconds(1).SetIsPeak(true).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 5, len(rxRes.Messages))

	time.Sleep(100 * time.Millisecond)
	ackRes, err := client.AQM().SetChannel(channel).SetWaitTimeSeconds(1).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, ackRes)
	require.Equal(t, uint64(5), ackRes.AffectedMessages)
}

func TestRestServer_Queue_SendAndStreamReceive(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	port, svc, s := setup(ctx, t)
	defer tearDown(svc, s)
	client, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest))
	require.NoError(t, err)
	defer client.Close()
	channel := nuid.Next()
	counter := atomic.NewInt64(0)
	go func() {
		client2, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest))
		require.NoError(t, err)
		defer client2.Close()
		for {
			if counter.Load() == 10 {
				return
			}
			stream := client2.SQM().SetChannel(channel)
			msg, err := stream.Next(ctx, 1, 5)
			if err != nil {
				continue
			}
			if msg != nil {
				err = msg.Ack()
				require.NoError(t, err)
				counter.Inc()

			}
			stream.Close()
		}
	}()

	go func() {
		client2, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest))
		require.NoError(t, err)
		defer client2.Close()
		for {
			if counter.Load() == 10 {
				return
			}
			stream := client2.SQM().SetChannel(channel)
			msg, err := stream.Next(ctx, 1, 5)
			if err != nil {
				continue
			}
			if msg != nil {
				err = msg.Ack()
				require.NoError(t, err)
				counter.Inc()

			}
			stream.Close()

		}
	}()
	time.Sleep(2000 * time.Millisecond)
	messages := client.QMB()
	for i := 0; i < 10; i++ {
		msg := client.QM()
		msg.SetId(nuid.Next()).SetChannel(channel).SetBody([]byte(fmt.Sprintf("msg #%d", i)))
		messages.Add(msg)
	}
	res, err := messages.Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 10, len(res))
	err = waitForCount(func() int {
		return int(counter.Load())
	}, 10, 10)
	require.NoError(t, err)

}

func TestRestServer_Queue_SendAndStreamReceiveWithReject(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	port, svc, s := setup(ctx, t)
	defer tearDown(svc, s)
	client, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest))
	require.NoError(t, err)
	defer client.Close()
	channel := nuid.Next()
	newChannel := nuid.Next()
	msg := client.QM()
	result, err := msg.SetId(nuid.Next()).
		SetChannel(channel).
		SetBody([]byte("msg reject")).
		SetPolicyMaxReceiveCount(2).
		SetPolicyMaxReceiveQueue(newChannel).Send(ctx)

	require.NoError(t, err)
	require.NotNil(t, result)
	time.Sleep(1000 * time.Millisecond)
	client2, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest))
	require.NoError(t, err)
	defer client2.Close()
	for i := 0; i < 3; i++ {
		stream := client2.SQM().SetChannel(channel)
		msg, err := stream.Next(ctx, 1, 5)
		if err != nil {
			continue
		}
		if msg != nil {
			err = msg.Reject()
			require.NoError(t, err)
		}
		stream.Close()

	}
	time.Sleep(100 * time.Millisecond)
	rxRes, err := client.NewReceiveQueueMessagesRequest().SetChannel(newChannel).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(10).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 1, len(rxRes.Messages))

}

func TestRestServer_Queue_SendAndStreamReceiveWithExtendVisibility(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	port, svc, s := setup(ctx, t)
	defer tearDown(svc, s)
	client, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest))
	require.NoError(t, err)
	defer client.Close()
	channel := nuid.Next()
	msg := client.QM()
	result, err := msg.SetId(nuid.Next()).
		SetChannel(channel).
		SetBody([]byte("msg extend")).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, result)
	time.Sleep(1000 * time.Millisecond)
	client2, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest))
	require.NoError(t, err)
	defer client2.Close()
	stream := client2.SQM().SetChannel(channel)
	msgRx, err := stream.Next(ctx, 2, 5)
	require.NoError(t, err)
	require.NotNil(t, msgRx)
	time.Sleep(1000 * time.Millisecond)
	err = msgRx.ExtendVisibility(2)
	require.NoError(t, err)
	time.Sleep(1000 * time.Millisecond)
	err = msgRx.Ack()
	require.NoError(t, err)
	time.Sleep(1000 * time.Millisecond)
	stream.Close()
	rxRes, err := client.RQM().SetChannel(channel).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(1).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 0, len(rxRes.Messages))

}

func TestRestServer_Queue_SendAndStreamReceiveWithResendToNewQueue(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	port, svc, s := setup(ctx, t)
	defer tearDown(svc, s)
	client, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest))
	require.NoError(t, err)
	defer client.Close()
	channel := nuid.Next()
	newChannel := nuid.Next()
	msg := client.QM()
	result, err := msg.SetId(nuid.Next()).
		SetChannel(channel).
		SetBody([]byte("msg resend")).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, result)
	time.Sleep(1000 * time.Millisecond)
	client2, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest))
	require.NoError(t, err)
	defer client2.Close()
	stream := client2.SQM().SetChannel(channel)
	msgRx, err := stream.Next(ctx, 2, 5)
	require.NoError(t, err)
	require.NotNil(t, msgRx)
	time.Sleep(1000 * time.Millisecond)
	err = msgRx.Resend(newChannel)
	require.NoError(t, err)
	time.Sleep(1000 * time.Millisecond)
	stream.Close()
	rxRes, err := client.RQM().SetChannel(channel).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(1).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 0, len(rxRes.Messages))
	rxRes, err = client.RQM().SetChannel(newChannel).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(1).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 1, len(rxRes.Messages))

}

func TestRestServer_Queue_SendAndStreamReceiveWithResendWithNewMessage(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	port, svc, s := setup(ctx, t)
	defer tearDown(svc, s)
	client, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest))
	require.NoError(t, err)
	defer client.Close()
	channel := nuid.Next()
	newChannel := nuid.Next()
	msg := client.QM()
	result, err := msg.SetId(nuid.Next()).
		SetChannel(channel).
		SetBody([]byte("msg resend")).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, result)
	time.Sleep(1000 * time.Millisecond)
	client2, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest))
	require.NoError(t, err)
	defer client2.Close()
	stream := client2.SQM().SetChannel(channel)
	msgRx, err := stream.Next(ctx, 2, 5)
	require.NoError(t, err)
	require.NotNil(t, msgRx)
	time.Sleep(1000 * time.Millisecond)
	err = stream.ResendWithNewMessage(client.QM().SetId(nuid.Next()).
		SetChannel(newChannel).
		SetBody([]byte("new msg")))
	require.NoError(t, err)
	time.Sleep(1000 * time.Millisecond)
	stream.Close()
	rxRes, err := client.RQM().SetChannel(channel).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(1).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 0, len(rxRes.Messages))
	rxRes, err = client.RQM().SetChannel(newChannel).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(1).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 1, len(rxRes.Messages))
}

func TestRestServer_QueueInfo_SendAndReceive(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	port, svc, s := setup(ctx, t)
	defer tearDown(svc, s)
	client, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://localhost:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest))
	require.NoError(t, err)
	defer func() {
		_ = client.Close()
	}()

	channel := nuid.Next()
	messages := client.QMB()
	for i := 0; i < 10; i++ {
		msg := client.QM()
		msg.SetId(nuid.Next()).SetChannel(channel).SetBody([]byte(fmt.Sprintf("msg #%d", i)))
		messages.Add(msg)
	}
	res, err := messages.Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 10, len(res))

	rxRes, err := client.RQM().SetChannel(channel).SetMaxNumberOfMessages(10).SetWaitTimeSeconds(10).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 10, len(rxRes.Messages))

	qi, err := client.QueuesInfo(ctx, "")
	require.NoError(t, err)
	require.EqualValues(t, int32(1), qi.TotalQueues)
	require.EqualValues(t, int64(10), qi.Sent)
	require.EqualValues(t, int64(10), qi.Delivered)
	require.EqualValues(t, 0, qi.Waiting)
}

func TestRestServer_SendEventsAndEventsStoreErrors(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	port, svc, rs := setup(ctx, t)
	defer tearDown(svc, rs)
	defer cancel()
	for _, test := range test2.SendEventsErrorFixtures {
		t.Run("send_event-"+test.Name, func(t *testing.T) {
			test.Msg.Store = false
			method := "send/event"
			url := fmt.Sprintf("http://localhost:%d/%s", port, method)
			res := &Response{}
			_, err := getRestyRequest().SetBody(test.Msg).SetError(res).SetResult(res).Post(url)
			require.NoError(t, err)
			require.True(t, res.IsError)
			require.Equal(t, test.ExpectedErrorText, res.Message)

			test.Msg.Store = true
			_, err = getRestyRequest().SetBody(test.Msg).SetError(res).SetResult(res).Post(url)
			require.NoError(t, err)
			require.True(t, res.IsError)
			require.Equal(t, test.ExpectedErrorText, res.Message)

		})
		t.Run("send_stream-"+test.Name, func(t *testing.T) {
			test.Msg.Store = false
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			methodStream := "send/stream"
			urlStream := fmt.Sprintf("ws://localhost:%d/%s", port, methodStream)
			chRead := make(chan string, 1)
			chWrite := make(chan string, 1)
			go func() {
				_ = runWebsocketClientWriter(ctx, t, urlStream, chRead, chWrite)
			}()
			time.Sleep(1 * time.Second)
			data, _ := ToPlainJson(test.Msg)
			chWrite <- string(data)
			errStr := <-chRead
			result := &pb.Result{}
			err := json.Unmarshal([]byte(errStr), result)
			require.NoError(t, err)
			require.Equal(t, test.ExpectedErrorText, result.Error)

			test.Msg.Store = true
			data, _ = ToPlainJson(test.Msg)
			chWrite <- string(data)
			errStr = <-chRead
			result = &pb.Result{}
			err = json.Unmarshal([]byte(errStr), result)
			require.NoError(t, err)
			require.Equal(t, test.ExpectedErrorText, result.Error)

		})

	}
}

func TestRestServer_SendMessagesSubscribeMessages(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	port, svc, rs := setup(ctx, t)
	defer tearDown(svc, rs)
	defer cancel()
	methodSend := "send/event"
	methodSub := "subscribe/events"

	for _, test := range test2.SendEventSubscribeMessagesFixtures {
		t.Run("send_message-"+test.Name, func(t *testing.T) {
			urlSend := fmt.Sprintf("http://localhost:%d/%s", port, methodSend)
			urlSub := fmt.Sprintf("ws://localhost:%d/%s?&client_id=%s&channel=%s&group=%s&subscribe_type=%s", port, methodSub, test.SubRequest.ClientID, test.SubRequest.Channel, test.SubRequest.Group, "events")
			resCh := make(chan string, 10)
			go func() {
				_ = runWebsocketClientReader(ctx, t, urlSub, resCh)
			}()
			time.Sleep(1 * time.Second)
			res := &Response{}
			_, err := getRestyRequest().SetBody(test.Msg).SetResult(res).SetError(res).Post(urlSend)
			require.NoError(t, err)
			require.False(t, res.IsError)
			result := &pb.Result{}
			err = res.Unmarshal(result)
			require.NoError(t, err)
			assert.EqualValues(t, test.ExpectedResult, result)
			msgRcvStr := <-resCh
			msgRcv := &pb.EventReceive{}
			err = FromPlainJson([]byte(msgRcvStr), msgRcv)
			require.NoError(t, err)
			assert.EqualValues(t, test.ExpectedMsgReceived, msgRcv)
		})
	}

}

func TestRestServer_SendMessagesSubscribePersistence(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	port, svc, rs := setup(ctx, t)
	defer tearDown(svc, rs)
	defer cancel()

	methodSend := "send/event"
	methodSub := "subscribe/events"

	for _, test := range test2.SendMessagesSubscribePersistentFixtures {
		t.Run("send_message-"+test.Name, func(t *testing.T) {
			urlSend := fmt.Sprintf("http://localhost:%d/%s", port, methodSend)
			urlSub := fmt.Sprintf("ws://localhost:%d/%s?&client_id=%s&channel=%s&group=%s&subscribe_type=%s&true&events_store_type_data=%d&events_store_type_value=%d",
				port, methodSub, test.SubRequest.ClientID, test.SubRequest.Channel, test.SubRequest.Group, "events_store", test.SubRequest.SubscribeTypeData, test.SubRequest.EventsStoreTypeValue)
			resCh := make(chan string, 10)
			go func() {
				_ = runWebsocketClientReader(ctx, t, urlSub, resCh)
			}()
			time.Sleep(1 * time.Second)
			res := &Response{}
			_, err := getRestyRequest().SetBody(test.Msg).SetResult(res).SetError(res).Post(urlSend)
			require.NoError(t, err)
			require.False(t, res.IsError)
			result := &pb.Result{}
			err = res.Unmarshal(result)
			require.NoError(t, err)
			assert.EqualValues(t, test.ExpectedResult, result)
			msgRcvStr := <-resCh
			msgRcv := &pb.EventReceive{}
			err = FromPlainJson([]byte(msgRcvStr), msgRcv)
			require.NoError(t, err)
			require.NotZero(t, msgRcv.Timestamp)
			msgRcv.Timestamp = 0
			assert.EqualValues(t, test.ExpectedMsgReceived, msgRcv)
		})
	}
}

func TestRestServer_SendMessagesStreamSubscribeMessages(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	port, svc, rs := setup(ctx, t)
	defer tearDown(svc, rs)
	defer cancel()
	methodSub := "subscribe/events"
	methodStream := "send/stream"

	for _, test := range test2.SendEventSubscribeMessagesFixtures {
		t.Run("send_stream-"+test.Name, func(t *testing.T) {

			urlSub := fmt.Sprintf("ws://localhost:%d/%s?&client_id=%s&channel=%s&group=%s&subscribe_type=%s", port, methodSub, test.SubRequest.ClientID, test.SubRequest.Channel, test.SubRequest.Group, "events")
			resCh := make(chan string, 10)
			go func() {
				_ = runWebsocketClientReader(ctx, t, urlSub, resCh)
			}()
			urlStream := fmt.Sprintf("ws://localhost:%d/%s", port, methodStream)
			chRead := make(chan string, 10)
			chWrite := make(chan string, 1)
			go func() {
				_ = runWebsocketClientWriter(ctx, t, urlStream, chRead, chWrite)
			}()
			time.Sleep(1 * time.Second)
			data, _ := ToPlainJson(test.Msg)
			chWrite <- string(data)
			resultStr := <-chRead
			result := &pb.Result{}
			err := json.Unmarshal([]byte(resultStr), result)
			require.NoError(t, err)
			assert.EqualValues(t, test.ExpectedResult, result)
			msgRcvStr := <-resCh
			msgRcv := &pb.EventReceive{}
			err = FromPlainJson([]byte(msgRcvStr), msgRcv)
			require.NoError(t, err)
			assert.EqualValues(t, test.ExpectedMsgReceived, msgRcv)
		})
	}

}

func TestRestServer_SubscribeToChannelErrors(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	port, svc, rs := setup(ctx, t)
	defer tearDown(svc, rs)
	defer cancel()
	methodSub := "subscribe/events"
	tests := []struct {
		name    string
		urlSub  string
		errText string
	}{
		{
			name:    "invalid_subscribe_type",
			urlSub:  fmt.Sprintf("ws://localhost:%d/%s?channel=test-channel", port, methodSub),
			errText: entities.ErrInvalidSubscribeType.Error(),
		},
		{
			name:    "invalid_client_id",
			urlSub:  fmt.Sprintf("ws://localhost:%d/%s?channel=test-channel&subscribe_type=%s", port, methodSub, "events"),
			errText: entities.ErrInvalidClientID.Error(),
		},
		{
			name:    "mismatch_subscribe_type",
			urlSub:  fmt.Sprintf("ws://localhost:%d/%s?client_id=test-client-id5&channel=test-channel&subscribe_type=%s&events_store_type_data=5", port, methodSub, "events"),
			errText: entities.ErrInvalidEventStoreType.Error(),
		},
		{
			name:    "invalid_client_id",
			urlSub:  fmt.Sprintf("ws://localhost:%d/%s?channel=test-channel&subscribe_type=%s", port, methodSub, "events"),
			errText: entities.ErrInvalidClientID.Error(),
		},
		{
			name:    "invalid_channel_id",
			urlSub:  fmt.Sprintf("ws://localhost:%d/%s?client_id=test-client-id1&subscribe_type=%s", port, methodSub, "events"),
			errText: entities.ErrInvalidChannel.Error(),
		},
		{
			name:    "invalid_channel_wildcards_for_persistence_sub",
			urlSub:  fmt.Sprintf("ws://localhost:%d/%s?client_id=test-client-id3&channel=some_channel_*_>&subscribe_type=%s", port, methodSub, "events_store"),
			errText: entities.ErrInvalidWildcards.Error(),
		},
		{
			name:    "sub_to_persistence_with_undefined",
			urlSub:  fmt.Sprintf("ws://localhost:%d/%s?client_id=test-client-id4&channel=test-channel&subscribe_type=%s", port, methodSub, "events_store"),
			errText: entities.ErrInvalidSubscriptionType.Error(),
		},
		{
			name:    "sub_to_persistence_with_start_at_sequence_no_value",
			urlSub:  fmt.Sprintf("ws://localhost:%d/%s?client_id=test-client-id5&channel=test-channel&events_store_type_data=4&events_store_type_value=-1&subscribe_type=%s", port, methodSub, "events_store"),
			errText: entities.ErrInvalidStartSequenceValue.Error(),
		},
		{
			name:    "sub_to_persistence_with_start_time_no_value",
			urlSub:  fmt.Sprintf("ws://localhost:%d/%s?client_id=test-client-id6&channel=test-channel&events_store_type_data=5&events_store_type_value=-1&subscribe_type=%s", port, methodSub, "events_store"),
			errText: entities.ErrInvalidStartAtTimeValue.Error(),
		},
		{
			name:    "sub_to_persistence_with_start_time_delta_no_value",
			urlSub:  fmt.Sprintf("ws://localhost:%d/%s?client_id=test-client-id7&channel=test-channel&events_store_type_data=6&events_store_type_value=-1&subscribe_type=%s", port, methodSub, "events_store"),
			errText: entities.ErrInvalidStartAtTimeDeltaValue.Error(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resCh := make(chan string, 20)
			go func() {
				_ = runWebsocketClientReader(ctx, t, test.urlSub, resCh)
			}()

			time.Sleep(1 * time.Second)
			rxMsgStr := <-resCh
			res := &Response{}
			err := FromPlainJson([]byte(rxMsgStr), res)
			require.NoError(t, err)
			require.True(t, res.IsError)
			require.Equal(t, res.Message, test.errText)
		})

	}
}

func TestRestServer_SubscribeToChannelMultiSub(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	port, svc, rs := setup(ctx, t)
	defer tearDown(svc, rs)
	defer cancel()
	coxForClients, cancelForClients := context.WithCancel(ctx)
	for i := 0; i < 1000; i++ {
		go func(i int) {
			methodSub := "subscribe/events"
			clientID := fmt.Sprintf("test-client-id_%d", i)
			urlSub := fmt.Sprintf("ws://localhost:%d/%s?&client_id=%s&channel=%s&group=%s&subscribe_type=%s", port, methodSub, clientID, "some-channel", "", "events")
			resCh := make(chan string, 1)
			go func() {
				_ = runWebsocketClientReader(coxForClients, t, urlSub, resCh)
			}()

			<-coxForClients.Done()
		}(i)
	}

	_ = waitForCount(func() int {
		return int(svc.Array.ClientsCount())
	}, 10000, 10)

	cancelForClients()
	err := waitForCount(func() int {
		return int(svc.Array.ClientsCount())
	}, 0, 5)
	require.NoError(t, err)

}

func TestRestServer_SendRequestError(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	port, svc, rs := setup(ctx, t)
	defer tearDown(svc, rs)
	defer cancel()
	for _, test := range test2.SendRequestErrorFixtures {
		t.Run(test.Name, func(t *testing.T) {
			method := "send/request"
			url := fmt.Sprintf("http://localhost:%d/%s", port, method)
			res := &Response{}
			_, err := getRestyRequest().SetBody(test.Request).SetError(res).Post(url)
			require.NoError(t, err)
			require.True(t, res.IsError)
			require.Equal(t, test.ExpectedErrText, res.Message)
		})
	}
}

func TestRestServer_SendResponseError(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	port, svc, rs := setup(ctx, t)
	defer tearDown(svc, rs)
	defer cancel()
	for _, test := range test2.SendResponseErrorFixtures {
		t.Run("send_response_"+test.Name, func(t *testing.T) {
			method := "send/response"
			url := fmt.Sprintf("http://localhost:%d/%s", port, method)
			res := &Response{}
			_, err := getRestyRequest().SetBody(test.Response).SetError(res).Post(url)
			require.NoError(t, err)
			require.True(t, res.IsError)
			require.Equal(t, test.ExpectedErrText, res.Message)
		})
	}
}

func TestRestServer_QueryResponse(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	port, svc, rs := setup(ctx, t)
	defer tearDown(svc, rs)
	defer cancel()
	methodSendRequest := "send/request"
	methodSubRequest := "subscribe/requests"
	methodSendResponse := "send/response"
	channelRequest := "send.request.1"
	urlSendRequest := fmt.Sprintf("http://localhost:%d/%s", port, methodSendRequest)
	urlSendResponse := fmt.Sprintf("http://localhost:%d/%s", port, methodSendResponse)
	urlSubRequest := fmt.Sprintf("ws://localhost:%d/%s?channel=%s&client_id=test-client-id&subscribe_type=%s", port, methodSubRequest, channelRequest, "queries")
	subCh := make(chan string, 2)
	go func() {
		_ = runWebsocketClientReader(ctx, t, urlSubRequest, subCh)
	}()
	time.Sleep(1 * time.Second)

	req := &pb.Request{
		RequestID:            nuid.Next(),
		RequestTypeData:      pb.Request_Query,
		ClientID:             "test-client-id",
		Channel:              channelRequest,
		Metadata:             "some-metadata",
		Body:                 []byte("some-request"),
		ReplyChannel:         "",
		Timeout:              5000,
		CacheKey:             "",
		CacheTTL:             0,
		Span:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	resForRequest := &Response{}
	done := make(chan bool)
	go func() {

		_, err := getRestyRequest().SetBody(req).SetResult(resForRequest).Post(urlSendRequest)
		assert.NoError(t, err)
		assert.False(t, resForRequest.IsError)
		getRes := &pb.Response{}
		err = resForRequest.Unmarshal(getRes)
		require.NoError(t, err)
		assert.Equal(t, []byte("some-response"), getRes.Body)
		assert.Equal(t, req.RequestID, getRes.RequestID)
		assert.False(t, getRes.CacheHit)
		assert.Equal(t, req.Metadata, getRes.Metadata)
		assert.NotZero(t, getRes.Timestamp)
		assert.True(t, getRes.Executed)
		done <- true
	}()

	rxReqStr := <-subCh
	rxReq := &pb.Request{}
	err := json.Unmarshal([]byte(rxReqStr), rxReq)
	require.NoError(t, err)
	require.Equal(t, rxReq.Metadata, req.Metadata)
	require.Equal(t, rxReq.Body, []byte("some-request"))
	require.NotEmpty(t, rxReq.ReplyChannel)
	require.Equal(t, rxReq.RequestID, req.RequestID)
	res := &pb.Response{
		ClientID:             "test-client-id",
		RequestID:            rxReq.RequestID,
		ReplyChannel:         rxReq.ReplyChannel,
		Metadata:             rxReq.Metadata,
		Body:                 []byte("some-response"),
		CacheHit:             false,
		Timestamp:            time.Now().UTC().Unix(),
		Executed:             true,
		Error:                "",
		Span:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	resForResponse := &Response{}
	_, err = getRestyRequest().SetBody(res).SetResult(resForResponse).Post(urlSendResponse)
	require.NoError(t, err)
	require.False(t, resForResponse.IsError)
	<-done
}

func TestRestServer_QueryResponse_WithCache(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	port, svc, rs := setup(ctx, t)
	defer tearDown(svc, rs)
	defer cancel()
	methodSendRequest := "send/request"
	methodSubRequest := "subscribe/requests"
	methodSendResponse := "send/response"
	channelRequest := "send.request.2"
	urlSendRequest := fmt.Sprintf("http://localhost:%d/%s", port, methodSendRequest)
	urlSendResponse := fmt.Sprintf("http://localhost:%d/%s", port, methodSendResponse)
	urlSubRequest := fmt.Sprintf("ws://localhost:%d/%s?channel=%s&client_id=test-client-id&subscribe_type=%s", port, methodSubRequest, channelRequest, "queries")
	subCh := make(chan string, 2)
	go func() {
		_ = runWebsocketClientReader(ctx, t, urlSubRequest, subCh)
	}()
	time.Sleep(1 * time.Second)

	req := &pb.Request{
		RequestID:            nuid.Next(),
		RequestTypeData:      pb.Request_Query,
		ClientID:             "test-client-id",
		Channel:              channelRequest,
		Metadata:             "some-metadata",
		Body:                 []byte("some-request"),
		ReplyChannel:         "",
		Timeout:              2000,
		CacheKey:             "some-request",
		CacheTTL:             10000,
		Span:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}

	done := make(chan bool)
	go func() {

		resForRequest := &Response{}
		_, err := getRestyRequest().SetBody(req).SetResult(resForRequest).Post(urlSendRequest)
		assert.NoError(t, err)
		assert.False(t, resForRequest.IsError)
		getRes := &pb.Response{}
		err = resForRequest.Unmarshal(getRes)
		require.NoError(t, err)
		assert.NoError(t, err)
		assert.Equal(t, []byte("some-response"), getRes.Body)
		assert.Equal(t, req.RequestID, getRes.RequestID)
		assert.False(t, getRes.CacheHit)
		assert.Equal(t, req.Metadata, getRes.Metadata)

		_, err = getRestyRequest().SetBody(req).SetResult(resForRequest).Post(urlSendRequest)
		assert.NoError(t, err)
		assert.False(t, resForRequest.IsError)
		getRes = &pb.Response{}
		err = resForRequest.Unmarshal(getRes)
		require.NoError(t, err)
		assert.NoError(t, err)
		assert.Equal(t, []byte("some-response"), getRes.Body)
		assert.Equal(t, req.RequestID, getRes.RequestID)
		assert.True(t, getRes.CacheHit)
		assert.Equal(t, req.Metadata, getRes.Metadata)
		assert.NotZero(t, getRes.Timestamp)
		assert.True(t, getRes.Executed)
		done <- true
	}()

	rxReqStr := <-subCh
	rxReq := &pb.Request{}
	err := json.Unmarshal([]byte(rxReqStr), rxReq)
	require.NoError(t, err)
	require.Equal(t, rxReq.Metadata, req.Metadata)
	require.Equal(t, rxReq.Body, []byte("some-request"))
	require.NotEmpty(t, rxReq.ReplyChannel)
	require.Equal(t, rxReq.RequestID, req.RequestID)
	res := &pb.Response{
		ClientID:             "test-client-id",
		RequestID:            rxReq.RequestID,
		ReplyChannel:         rxReq.ReplyChannel,
		Metadata:             rxReq.Metadata,
		Body:                 []byte("some-response"),
		CacheHit:             false,
		Timestamp:            time.Now().Unix(),
		Executed:             true,
		Error:                "",
		Span:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	resForResponse := &Response{}

	_, err = getRestyRequest().SetBody(res).SetResult(resForResponse).Post(urlSendResponse)
	require.NoError(t, err)
	require.False(t, resForResponse.IsError)
	<-done
}

func TestRestServer_CommandResponse(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	port, svc, rs := setup(ctx, t)
	defer tearDown(svc, rs)
	defer cancel()
	methodSendRequest := "send/request"
	methodSubRequest := "subscribe/requests"
	methodSendResponse := "send/response"
	channelRequest := "send.request.1"
	urlSendRequest := fmt.Sprintf("http://localhost:%d/%s", port, methodSendRequest)
	urlSendResponse := fmt.Sprintf("http://localhost:%d/%s", port, methodSendResponse)
	urlSubRequest := fmt.Sprintf("ws://localhost:%d/%s?channel=%s&client_id=test-client-id&subscribe_type=%s", port, methodSubRequest, channelRequest, "commands")
	subCh := make(chan string, 2)
	go func() {
		_ = runWebsocketClientReader(ctx, t, urlSubRequest, subCh)
	}()
	time.Sleep(1 * time.Second)

	req := &pb.Request{
		RequestID:            nuid.Next(),
		RequestTypeData:      pb.Request_Command,
		ClientID:             "test-client-id",
		Channel:              channelRequest,
		Metadata:             "some-metadata",
		Body:                 []byte("some-request"),
		ReplyChannel:         "",
		Timeout:              5000,
		CacheKey:             "",
		CacheTTL:             0,
		Span:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	resForRequest := &Response{}
	done := make(chan bool)
	go func() {

		_, err := getRestyRequest().SetBody(req).SetResult(resForRequest).Post(urlSendRequest)
		assert.NoError(t, err)
		assert.False(t, resForRequest.IsError)
		getRes := &pb.Response{}
		err = resForRequest.Unmarshal(getRes)
		require.NoError(t, err)
		assert.Empty(t, getRes.Body)
		assert.Empty(t, getRes.Metadata)
		assert.Equal(t, req.RequestID, getRes.RequestID)
		assert.NotZero(t, getRes.Timestamp)
		assert.True(t, getRes.Executed)
		done <- true
	}()

	rxReqStr := <-subCh
	rxReq := &pb.Request{}
	err := json.Unmarshal([]byte(rxReqStr), rxReq)
	require.NoError(t, err)
	require.Equal(t, rxReq.Metadata, req.Metadata)
	require.Equal(t, rxReq.Body, []byte("some-request"))
	require.NotEmpty(t, rxReq.ReplyChannel)
	require.Equal(t, rxReq.RequestID, req.RequestID)
	res := &pb.Response{
		ClientID:             "test-client-id",
		RequestID:            rxReq.RequestID,
		ReplyChannel:         rxReq.ReplyChannel,
		Metadata:             rxReq.Metadata,
		Body:                 []byte("some-response"),
		CacheHit:             false,
		Timestamp:            time.Now().Unix(),
		Executed:             true,
		Error:                "",
		Span:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	resForResponse := &Response{}

	_, err = getRestyRequest().SetBody(res).SetResult(resForResponse).Post(urlSendResponse)
	require.NoError(t, err)
	require.False(t, resForResponse.IsError)
	<-done
}
