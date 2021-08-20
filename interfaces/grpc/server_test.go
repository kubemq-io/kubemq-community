package grpc

import (
	"crypto/rand"
	"github.com/kubemq-io/broker/pkg/pipe"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"github.com/kubemq-io/kubemq-community/pkg/uuid"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	"github.com/kubemq-io/kubemq-go/queues_stream"
	"github.com/nats-io/nuid"
	"io/ioutil"
	"os"
	"testing"

	"github.com/kubemq-io/kubemq-community/pkg/logging"

	"github.com/kubemq-io/kubemq-community/services"

	"github.com/fortytw2/leaktest"
	pb "github.com/kubemq-io/protobuf/go"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"context"
	"sync"
	"time"

	"github.com/kubemq-io/kubemq-community/config"
	test2 "github.com/kubemq-io/kubemq-community/interfaces/test"

	"fmt"

	sdk "github.com/kubemq-io/kubemq-go"
	"go.uber.org/atomic"
)

var testPort = *atomic.NewInt32(55555)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	metrics.InitExporter(ctx)

	os.Exit(m.Run())
}

func init() {
	_ = logging.CreateLoggerFactory(context.Background(), "test", &config.LogConfig{
		Level: "info",
	})

}

func getAppConfig() *config.Config {
	tmp, err := ioutil.TempDir("./", "test_grpc")
	appConfig := config.GetCopyAppConfig("../../test/")
	if err != nil {
		panic(err)
	}
	appConfig.Grpc.Port = int(testPort.Inc())
	appConfig.Broker.MonitoringPort = int(testPort.Inc())
	appConfig.Broker.MemoryPipe = pipe.NewPipe(nuid.Next())
	appConfig.Store.StorePath = tmp
	return appConfig
}
func setup(ctx context.Context, t *testing.T, appConfigs ...*config.Config) (*config.Config, *Server, *services.SystemServices) {
	var appConfig *config.Config
	if len(appConfigs) == 0 {
		appConfig = getAppConfig()
	} else {
		appConfig = appConfigs[0]
	}

	svc, err := services.Start(ctx, appConfig)
	require.NoError(t, err)
	s, err := NewServer(svc, appConfig)
	require.NoError(t, err)
	require.NotNil(t, s)
	svc.Broker.DisableMetricsReporting()
	return appConfig, s, svc
}

func tearDown(s *Server, svc *services.SystemServices) {

	s.Close()
	time.Sleep(time.Second)
	svc.Close()
	<-svc.Stopped

	err := os.RemoveAll(s.services.AppConfig.Store.StorePath)
	if err != nil {
		panic(err)
	}
}

func getNewClient(ctx context.Context, t *testing.T, appConfig *config.Config) grpcClient {
	clientOptions := NewClientOptions(fmt.Sprintf(":%d", appConfig.Grpc.Port))
	conn, err := GetClientConn(ctx, clientOptions)
	require.NoError(t, err)
	client, err := grpcClient{pb.NewKubemqClient(conn), conn}, nil
	require.NoError(t, err)
	require.NotNil(t, client)
	return client
}

func waitForCount(fn func() int, required, timeout int) error {
	for i := 0; i < 10*timeout; i++ {
		if fn() == required {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout, want %d, got %d", required, fn())
}

func randStringBytes(n int) []byte {
	blk := make([]byte, n)
	_, err := rand.Read(blk)
	if err != nil {
		panic(err)
	}
	return blk
}
func createBatchMessages(channel string, amount int) []*queues_stream.QueueMessage {
	var list []*queues_stream.QueueMessage
	for i := 0; i < amount; i++ {
		list = append(list, queues_stream.NewQueueMessage().
			SetId(uuid.New()).
			SetChannel(channel).
			SetBody([]byte(fmt.Sprintf("message %d", i))))
	}
	return list
}

func TestGRPCServer_SendEvents(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	count := atomic.NewInt32(0)
	channel := "TestGRPCServer_SendMessageStream"
	msg := &pb.Event{
		EventID:              "",
		ClientID:             "test-client-id",
		Channel:              channel,
		Metadata:             "some meta data",
		Body:                 []byte("some-body"),
		Store:                false,
		Tags:                 make(map[string]string),
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	msg.Tags["tag1"] = "tag1"
	ready := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer func() {
			wg.Done()
		}()
		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Events,
			ClientID:             "test-client-id",
			Channel:              channel,
			Group:                "",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		msgCh := make(chan *pb.EventReceive, 1)
		errCh := make(chan error, 1)
		go func() {
			_ = SubscribeToEvents(ctx, appConfig, subReq, msgCh, errCh)
		}()
		ready <- struct{}{}
		rxMsg := <-msgCh
		require.EqualValues(t, rxMsg.Body, msg.Body)
		require.EqualValues(t, rxMsg.Tags, msg.Tags)
		count.Inc()
	}()

	go func() {
		defer func() {
			wg.Done()
		}()
		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Events,
			ClientID:             "test-client-id-2",
			Channel:              channel,
			Group:                "",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		msgCh := make(chan *pb.EventReceive, 1)
		errCh := make(chan error, 1)
		go func() {
			_ = SubscribeToEvents(ctx, appConfig, subReq, msgCh, errCh)
		}()

		ready <- struct{}{}
		rxMsg := <-msgCh
		require.EqualValues(t, rxMsg.Body, msg.Body)
		require.EqualValues(t, rxMsg.Tags, msg.Tags)
		count.Inc()
	}()

	<-ready
	<-ready
	time.Sleep(time.Second)
	result, err := SendEvents(ctx, appConfig, msg)
	require.NoError(t, err)
	require.True(t, result.Sent)
	wg.Wait()
	require.Equal(t, int32(2), count.Load())
}
func TestGRPCServer_SendEventsStream(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	msg := &pb.Event{
		ClientID: "test-client-id",
		Body:     []byte("some-body"),
		Channel:  "foo",
		Metadata: "some meta data",
	}
	worker1, worker2 := atomic.NewInt32(0), atomic.NewInt32(0)

	go func() {
		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Events,
			ClientID:             "test-client-id-1",
			Channel:              "foo",
			Group:                "bar",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		msgCh := make(chan *pb.EventReceive, 1)
		errCh := make(chan error, 1)
		go func() {
			_ = SubscribeToEvents(ctx, appConfig, subReq, msgCh, errCh)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case rxMsg := <-msgCh:
				require.EqualValues(t, rxMsg.Body, msg.Body)
				worker1.Inc()

			}
		}
	}()

	go func() {
		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Events,
			ClientID:             "test-client-id-2",
			Channel:              "foo",
			Group:                "bar",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		msgCh := make(chan *pb.EventReceive, 1)
		errCh := make(chan error, 1)
		go func() {
			_ = SubscribeToEvents(ctx, appConfig, subReq, msgCh, errCh)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case rxMsg := <-msgCh:
				require.EqualValues(t, rxMsg.Body, msg.Body)
				worker2.Inc()
			}
		}
	}()
	time.Sleep(1 * time.Second)
	events := make(chan *pb.Event)
	results := make(chan *pb.Result)
	go func() {
		_ = EventsStreamMessage(ctx, appConfig, events, results)
	}()
	for i := 0; i < 1000; i++ {
		events <- msg

	}
	time.Sleep(1 * time.Second)
	assert.NotZero(t, worker1.Load())
	assert.NotZero(t, worker2.Load())
	assert.Equal(t, int32(1000), worker1.Load()+worker2.Load())
}
func TestGRPCServer_SendEventsAndEventsStoreErrors(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	for _, test := range test2.SendEventsErrorFixtures {
		t.Run("send_message-"+test.Name, func(t *testing.T) {
			test.Msg.Store = false
			_, err := SendEvents(ctx, appConfig, test.Msg)
			require.Contains(t, err.Error(), test.ExpectedErrorText)
			test.Msg.Store = true
			_, err = SendEvents(ctx, appConfig, test.Msg)
			require.Contains(t, err.Error(), test.ExpectedErrorText)
		})
		t.Run("send_stream-"+test.Name, func(t *testing.T) {
			test.Msg.Store = false
			events := make(chan *pb.Event)
			results := make(chan *pb.Result)
			go func() {
				_ = EventsStreamMessage(ctx, appConfig, events, results)
			}()
			events <- test.Msg
			result := <-results
			require.False(t, result.Sent)
			require.Equal(t, test.ExpectedErrorText, result.Error)
			test.Msg.Store = true
			events <- test.Msg
			result = <-results
			require.False(t, result.Sent)
			require.Equal(t, test.ExpectedErrorText, result.Error)
		})
	}
}
func TestGRPCServer_SubscribeToEventsErrors(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	tests := []struct {
		name    string
		subReq  *pb.Subscribe
		errText string
	}{
		{
			name: "invalid_subscribe_type",
			subReq: &pb.Subscribe{
				SubscribeTypeData:    0,
				ClientID:             "",
				Channel:              "test-channel",
				Group:                "",
				EventsStoreTypeData:  0,
				EventsStoreTypeValue: 0,
				XXX_NoUnkeyedLiteral: struct{}{},
				XXX_sizecache:        0,
			},
			errText: entities.ErrInvalidSubscribeType.Error(),
		},
		{
			name: "invalid_client_id",
			subReq: &pb.Subscribe{
				SubscribeTypeData:    pb.Subscribe_Events,
				ClientID:             "",
				Channel:              "test-channel",
				Group:                "",
				EventsStoreTypeData:  0,
				EventsStoreTypeValue: 0,
				XXX_NoUnkeyedLiteral: struct{}{},
				XXX_sizecache:        0,
			},
			errText: entities.ErrInvalidClientID.Error(),
		},
		{
			name: "invalid_channel_id",
			subReq: &pb.Subscribe{
				SubscribeTypeData:    pb.Subscribe_Events,
				ClientID:             "test-client-id",
				Channel:              "",
				Group:                "",
				EventsStoreTypeData:  0,
				EventsStoreTypeValue: 0,
				XXX_NoUnkeyedLiteral: struct{}{},
				XXX_sizecache:        0,
			},
			errText: entities.ErrInvalidChannel.Error(),
		},
		{
			name: "invalid_channel_wildcards_for_events_store_sub",
			subReq: &pb.Subscribe{
				SubscribeTypeData:    pb.Subscribe_EventsStore,
				ClientID:             "test-client-id",
				Channel:              "test-channel-*->",
				Group:                "",
				EventsStoreTypeData:  0,
				EventsStoreTypeValue: 0,
				XXX_NoUnkeyedLiteral: struct{}{},
				XXX_sizecache:        0,
			},
			errText: entities.ErrInvalidWildcards.Error(),
		},
		{
			name: "invalid_channel_whitespaces",
			subReq: &pb.Subscribe{
				SubscribeTypeData:    pb.Subscribe_Events,
				ClientID:             "test-client-id",
				Channel:              "test-channel  some white spaces",
				Group:                "",
				EventsStoreTypeData:  0,
				EventsStoreTypeValue: 0,
				XXX_NoUnkeyedLiteral: struct{}{},
				XXX_sizecache:        0,
			},
			errText: entities.ErrInvalidWhitespace.Error(),
		},
		{
			name: "sub_to_events_store_with_undefined",
			subReq: &pb.Subscribe{
				SubscribeTypeData:    pb.Subscribe_EventsStore,
				ClientID:             "test-client-id",
				Channel:              "test-channel",
				Group:                "",
				EventsStoreTypeData:  0,
				EventsStoreTypeValue: 0,
				XXX_NoUnkeyedLiteral: struct{}{},
				XXX_sizecache:        0,
			},
			errText: entities.ErrInvalidSubscriptionType.Error(),
		},
		{
			name: "sub_to_events_store_with_start_at_sequence_no_value",
			subReq: &pb.Subscribe{
				SubscribeTypeData:    pb.Subscribe_EventsStore,
				ClientID:             "test-client-id",
				Channel:              "test-channel",
				Group:                "",
				EventsStoreTypeData:  pb.Subscribe_StartAtSequence,
				EventsStoreTypeValue: -1,
				XXX_NoUnkeyedLiteral: struct{}{},
				XXX_sizecache:        0,
			},
			errText: entities.ErrInvalidStartSequenceValue.Error(),
		},
		{
			name: "sub_to_events_store_with_start_time_no_value",
			subReq: &pb.Subscribe{
				SubscribeTypeData:    pb.Subscribe_EventsStore,
				ClientID:             "test-client-id",
				Channel:              "test-channel",
				Group:                "",
				EventsStoreTypeData:  pb.Subscribe_StartAtTime,
				EventsStoreTypeValue: -1,
				XXX_NoUnkeyedLiteral: struct{}{},
				XXX_sizecache:        0,
			},
			errText: entities.ErrInvalidStartAtTimeValue.Error(),
		},
		{
			name: "sub_to_events_store_with_start_time_delta_no_value",
			subReq: &pb.Subscribe{
				SubscribeTypeData:    pb.Subscribe_EventsStore,
				ClientID:             "test-client-id",
				Channel:              "test-channel",
				Group:                "",
				EventsStoreTypeData:  pb.Subscribe_StartAtTimeDelta,
				EventsStoreTypeValue: -1,
				XXX_NoUnkeyedLiteral: struct{}{},
				XXX_sizecache:        0,
			},
			errText: entities.ErrInvalidStartAtTimeDeltaValue.Error(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msgCh := make(chan *pb.EventReceive)
			errCh := make(chan error, 1)
			err := SubscribeToEvents(ctx, appConfig, test.subReq, msgCh, errCh)
			require.NoError(t, err)
			err = <-errCh
			require.Contains(t, err.Error(), test.errText)
		})

	}
}
func TestGRPCServer_SendRequestError(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	for _, test := range test2.SendRequestErrorFixtures {
		t.Run(test.Name, func(t *testing.T) {
			_, err := SendRequest(ctx, appConfig, test.Request)
			require.Contains(t, err.Error(), test.ExpectedErrText)
		})
	}
}
func TestGRPCServer_SendResponseError(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	for _, test := range test2.SendResponseErrorFixtures {
		t.Run("send_response_"+test.Name, func(t *testing.T) {
			err := SendResponse(ctx, appConfig, test.Response)
			require.Contains(t, err.Error(), test.ExpectedErrText)
		})

	}
}
func TestGRPCServer_SubscribeToQueriesWithSendResponseCache(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	isReady := make(chan bool)
	go func() {
		reqCh := make(chan *pb.Request)
		errCh := make(chan error, 1)
		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Queries,
			ClientID:             "test-client-id",
			Channel:              "RequestReplyStreamWithSendResponse.>",
			Group:                "q1",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		err := SubscribeToRequests(ctx, appConfig, subReq, reqCh, errCh)
		require.NoError(t, err)

		isReady <- true
		for {
			select {
			case requestItem := <-reqCh:
				if requestItem.ReplyChannel != "" {
					err := SendResponse(ctx, appConfig, &pb.Response{
						ClientID:             "test-client-id",
						RequestID:            requestItem.RequestID,
						ReplyChannel:         requestItem.ReplyChannel,
						Metadata:             requestItem.Metadata,
						Body:                 requestItem.Body,
						CacheHit:             false,
						Timestamp:            time.Now().Unix(),
						Executed:             true,
						Error:                "",
						Span:                 nil,
						Tags:                 requestItem.Tags,
						XXX_NoUnkeyedLiteral: struct{}{},
						XXX_sizecache:        0,
					})
					assert.NoError(t, err)
					return
				}
			case <-errCh:
				return
			case <-ctx.Done():
				return

			}

		}
	}()
	<-isReady
	time.Sleep(1 * time.Second)

	req := &pb.Request{
		RequestID:            nuid.Next(),
		RequestTypeData:      pb.Request_Query,
		ClientID:             "test-client-id",
		Channel:              "RequestReplyStreamWithSendResponse.1",
		Metadata:             "",
		Body:                 []byte("some_request"),
		ReplyChannel:         "",
		Timeout:              2000,
		CacheKey:             "key",
		CacheTTL:             2000,
		Span:                 nil,
		Tags:                 map[string]string{"tag1": "tag1"},
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	client := getNewClient(ctx, t, appConfig)
	response1, err := client.SendRequest(ctx, req)
	require.NoError(t, err)
	require.Equal(t, false, response1.CacheHit)
	require.Equal(t, response1.RequestID, req.RequestID)
	require.EqualValues(t, response1.Tags, req.Tags)
	require.True(t, response1.Executed)
	require.NotZero(t, response1.Timestamp)

	req2 := &pb.Request{
		RequestID:            nuid.Next(),
		RequestTypeData:      pb.Request_Query,
		ClientID:             "test-client-id",
		Channel:              "RequestReplyStreamWithSendResponse.1",
		Metadata:             "",
		Body:                 []byte("some_request"),
		ReplyChannel:         "",
		Timeout:              2000,
		CacheKey:             "key",
		CacheTTL:             2000,
		Span:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	response2, err := client.SendRequest(ctx, req2)
	require.NoError(t, err)
	require.Equal(t, true, response2.CacheHit)
	require.Equal(t, response2.RequestID, req2.RequestID)
	require.True(t, response1.Executed)
	require.NotZero(t, response1.Timestamp)
}
func TestGRPCServer_SubscribeToCommandWithSendResponseCache(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	isReady := make(chan bool)
	go func() {
		reqCh := make(chan *pb.Request)
		errCh := make(chan error, 1)
		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Commands,
			ClientID:             "test-client-id",
			Channel:              "RequestReplyStreamWithSendResponse.>",
			Group:                "q1",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		err := SubscribeToRequests(ctx, appConfig, subReq, reqCh, errCh)
		require.NoError(t, err)

		isReady <- true
		for {
			select {
			case requestItem := <-reqCh:
				if requestItem.ReplyChannel != "" {
					err := SendResponse(ctx, appConfig, &pb.Response{
						ClientID:             "test-client-id",
						RequestID:            requestItem.RequestID,
						ReplyChannel:         requestItem.ReplyChannel,
						Metadata:             requestItem.Metadata,
						Body:                 requestItem.Body,
						CacheHit:             false,
						Timestamp:            time.Now().Unix(),
						Executed:             true,
						Error:                "",
						Span:                 nil,
						XXX_NoUnkeyedLiteral: struct{}{},
						XXX_sizecache:        0,
					})
					assert.NoError(t, err)
					return
				}
			case <-errCh:
				return
			case <-ctx.Done():
				return

			}

		}
	}()
	<-isReady
	time.Sleep(1 * time.Second)

	req := &pb.Request{
		RequestID:            nuid.Next(),
		RequestTypeData:      pb.Request_Command,
		ClientID:             "test-client-id",
		Channel:              "RequestReplyStreamWithSendResponse.1",
		Metadata:             "",
		Body:                 []byte("some_request"),
		ReplyChannel:         "",
		Timeout:              2000,
		CacheKey:             "key",
		CacheTTL:             2000,
		Span:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	client := getNewClient(ctx, t, appConfig)
	response1, err := client.SendRequest(ctx, req)
	require.NoError(t, err)
	require.Empty(t, response1.Body)
	require.Empty(t, response1.Metadata)
	require.Equal(t, response1.RequestID, req.RequestID)
	require.True(t, response1.Executed)
	require.NotZero(t, response1.Timestamp)

}
func TestGRPCServer_MultipleCommands(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	reqCh := make(chan *pb.Request, 10)
	errCh := make(chan error, 5)
	subReq := &pb.Subscribe{
		SubscribeTypeData:    pb.Subscribe_Commands,
		ClientID:             "test-client-id",
		Channel:              "TestGRPCServer_RequestResponseWithSendResponse.>",
		Group:                "q1",
		EventsStoreTypeData:  0,
		EventsStoreTypeValue: 0,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	err := SubscribeToRequests(ctx, appConfig, subReq, reqCh, errCh)
	require.NoError(t, err)
	go func() {
		for {
			select {
			case req := <-reqCh:
				if req.ReplyChannel != "" {
					err := SendResponse(ctx, appConfig, &pb.Response{
						ClientID:             "test-client-id",
						RequestID:            req.RequestID,
						ReplyChannel:         req.ReplyChannel,
						Metadata:             req.Metadata,
						Body:                 req.Body,
						CacheHit:             false,
						Timestamp:            time.Now().Unix(),
						Executed:             true,
						Error:                "",
						Span:                 nil,
						XXX_NoUnkeyedLiteral: struct{}{},
						XXX_sizecache:        0,
					})
					require.NoError(t, err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	time.Sleep(1 * time.Second)
	req := &pb.Request{
		RequestID:            nuid.Next(),
		RequestTypeData:      pb.Request_Command,
		ClientID:             "client_send_request.1",
		Channel:              "TestGRPCServer_RequestResponseWithSendResponse.1",
		Metadata:             "",
		Body:                 []byte("some_request"),
		ReplyChannel:         "",
		Timeout:              2000,
		CacheKey:             "",
		CacheTTL:             0,
		Span:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	for i := 0; i < 100; i++ {
		res, err := SendRequest(ctx, appConfig, req)
		require.NoError(t, err)
		require.NotNil(t, res)
	}
	time.Sleep(1 * time.Second)

}
func TestGRPCServer_SendEventsStoreMessageStream(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	msg := &pb.Event{
		EventID:              "",
		ClientID:             "test-client-id",
		Channel:              "foo",
		Metadata:             "some meta data",
		Body:                 []byte("some-body"),
		Store:                true,
		Tags:                 map[string]string{"tag1": "tag1"},
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	count := atomic.NewInt32(0)

	events := make(chan *pb.Event)
	results := make(chan *pb.Result)
	err := EventsStreamMessage(ctx, appConfig, events, results)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		events <- msg
		result := <-results
		require.NotEqual(t, "", result.EventID)
		require.True(t, result.Sent)
	}

	go func() {
		rxMsgCh := make(chan *pb.EventReceive, 1)
		errCh := make(chan error, 1)
		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_EventsStore,
			ClientID:             "test-client-id",
			Channel:              "foo",
			Group:                "bar",
			EventsStoreTypeData:  pb.Subscribe_StartFromFirst,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		go func() {
			_ = SubscribeToEvents(ctx, appConfig, subReq, rxMsgCh, errCh)
		}()
		require.NoError(t, err)
		for {
			select {
			case rxMsg := <-rxMsgCh:
				assert.Equal(t, msg.Body, rxMsg.Body)
				assert.EqualValues(t, msg.Tags, rxMsg.Tags)
				count.Inc()
			case <-ctx.Done():
				return
			}
		}
	}()
	err = waitForCount(func() int {
		return int(count.Load())
	}, 10, 5)
	require.NoError(t, err)
	time.Sleep(1 * time.Second)

	result, err := SendEvents(ctx, appConfig, msg)
	require.NotEqual(t, "", result.EventID)
	time.Sleep(1 * time.Second)
	require.Equal(t, 11, int(count.Load()))
}
func TestGRPCServer_SubscribeToEventsMultiSub(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	coxForClients, cancelForClients := context.WithCancel(ctx)
	for i := 0; i < 1000; i++ {
		time.Sleep(1 * time.Microsecond)
		go func(i int) {
			subReq := &pb.Subscribe{
				SubscribeTypeData:    pb.Subscribe_Events,
				ClientID:             fmt.Sprintf("test-client-id_%d", i),
				Channel:              "foo",
				Group:                "",
				EventsStoreTypeData:  0,
				EventsStoreTypeValue: 0,
				XXX_NoUnkeyedLiteral: struct{}{},
				XXX_sizecache:        0,
			}
			errCh := make(chan error, 1)
			resCh := make(chan *pb.EventReceive, 1)
			go func() {
				_ = SubscribeToEvents(coxForClients, appConfig, subReq, resCh, errCh)
			}()
			<-coxForClients.Done()

		}(i)
	}
	err := waitForCount(func() int {
		return int(svc.Array.ClientsCount())
	}, 1000, 10)
	require.NoError(t, err)
	cancelForClients()
	err = waitForCount(func() int {
		return int(svc.Array.ClientsCount())
	}, 0, 10)
	require.NoError(t, err)
}
func TestGRPCServer_Queue_SendAndReceive(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	clients := 10
	cycles := 10
	wg := sync.WaitGroup{}
	wg.Add(clients)
	for i := 0; i < clients; i++ {
		go func() {
			defer wg.Done()
			client, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithTransportType(sdk.TransportTypeGRPC))
			require.NoError(t, err)
			for i := 0; i < cycles; i++ {
				channel := nuid.Next()
				messages := client.QMB()
				for i := 0; i < 1000; i++ {
					msg := client.QM()
					msg.SetId(nuid.Next()).SetChannel(channel).SetBody([]byte(fmt.Sprintf("msg #%d", i)))
					messages.Add(msg)
				}
				res, err := messages.Send(ctx)
				require.NoError(t, err)
				require.NotNil(t, res)
				require.Equal(t, 1000, len(res))
				rxRes, err := client.RQM().SetChannel(channel).SetMaxNumberOfMessages(1000).SetWaitTimeSeconds(60).Send(ctx)
				require.NoError(t, err)
				require.NotNil(t, rxRes)
				require.Equal(t, 1000, len(rxRes.Messages))
			}

		}()
	}

	wg.Wait()

}
func TestGRPCServer_Queue_SendAndReceiveWithPeak(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	client, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId("some-client-id"), sdk.WithTransportType(sdk.TransportTypeGRPC))
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
func TestGRPCServer_Queue_SendAndReceiveWithAckAllAndPeak(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	client, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId("some-client-id"), sdk.WithTransportType(sdk.TransportTypeGRPC))
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
func TestGRPCServer_Queue_SendAndStreamReceive(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	client, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
	require.NoError(t, err)
	defer func() {
		_ = client.Close()
	}()
	channel := nuid.Next()
	counter := atomic.NewInt64(0)
	go func() {
		client2, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
		require.NoError(t, err)
		defer func() {
			_ = client2.Close()
		}()

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

			time.Sleep(110 * time.Millisecond)
		}
	}()

	go func() {
		client2, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
		require.NoError(t, err)
		defer func() {
			_ = client2.Close()
		}()

		require.NoError(t, err)
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
			time.Sleep(110 * time.Millisecond)
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
func TestGRPCServer_Queue_SendAndStreamReceiveWithReject(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())

	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	client, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
	require.NoError(t, err)
	require.NoError(t, err)
	defer func() {
		_ = client.Close()
	}()

	channel := "queue-channel"
	newChannel := "dead-letter-channel"
	msg := client.QM()
	result, err := msg.SetId(nuid.Next()).
		SetChannel(channel).
		SetBody([]byte("msg reject")).
		SetPolicyMaxReceiveCount(3).
		SetPolicyMaxReceiveQueue(newChannel).Send(ctx)

	require.NoError(t, err)
	require.NotNil(t, result)
	time.Sleep(1000 * time.Millisecond)
	client2, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
	require.NoError(t, err)
	defer func() {
		_ = client2.Close()
	}()

	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		stream := client2.SQM().SetChannel(channel)
		msg, err := stream.Next(ctx, 1, 5)
		if err != nil {
			continue
		}
		if msg != nil {
			err = msg.Reject()
			require.NoError(t, err)
		}
		time.Sleep(110 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	rxRes, err := client.NewReceiveQueueMessagesRequest().SetChannel(newChannel).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(10).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 1, len(rxRes.Messages))

}
func TestGRPCServer_Queue_SendAndStreamReceiveWithDisconnect(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	client, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
	require.NoError(t, err)
	defer func() {
		_ = client.Close()
	}()

	channel := "queue-channel"
	newChannel := "dead-letter-channel"
	msg := client.QM()
	result, err := msg.SetId(nuid.Next()).
		SetChannel(channel).
		SetBody([]byte("msg reject")).
		SetPolicyMaxReceiveCount(3).
		SetPolicyMaxReceiveQueue(newChannel).Send(ctx)

	require.NoError(t, err)
	require.NotNil(t, result)
	time.Sleep(1000 * time.Millisecond)

	go func() {
		for i := 0; i < 4; i++ {
			//ctx, cancel := context.WithCancel(ctx)
			client2, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
			if err != nil {
				continue
			}
			stream := client2.SQM().SetChannel(channel)
			_, err = stream.Next(ctx, 1, 5)
			if err != nil {
				fmt.Println(err)
			}
			//cancel()
			_ = client2.Close()
			//	time.Sleep(110 * time.Millisecond)
		}

	}()

	rxRes, err := client.NewReceiveQueueMessagesRequest().SetChannel(newChannel).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(10).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 1, len(rxRes.Messages))

}
func TestGRPCServer_Queue_SendAndStreamReceiveWithExtendVisibility(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	client, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
	require.NoError(t, err)
	defer func() {
		_ = client.Close()
	}()

	channel := nuid.Next()
	msg := client.QM()
	result, err := msg.SetId(nuid.Next()).
		SetChannel(channel).
		SetBody([]byte("msg extend")).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, result)
	time.Sleep(1000 * time.Millisecond)
	client2, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
	require.NoError(t, err)
	defer func() {
		_ = client2.Close()
	}()

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
	rxRes, err := client.RQM().SetChannel(channel).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(1).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 0, len(rxRes.Messages))

}
func TestGRPCServer_Queue_SendAndStreamReceiveWithResendToNewQueue(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	client, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
	require.NoError(t, err)
	defer func() {
		_ = client.Close()
	}()

	channel := nuid.Next()
	newChannel := nuid.Next()
	msg := client.QM()
	result, err := msg.SetId(nuid.Next()).
		SetChannel(channel).
		SetBody([]byte("msg resend")).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, result)
	time.Sleep(1000 * time.Millisecond)
	client2, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
	require.NoError(t, err)
	defer func() {
		_ = client2.Close()
	}()

	stream := client2.SQM().SetChannel(channel)
	msgRx, err := stream.Next(ctx, 2, 5)
	require.NoError(t, err)
	require.NotNil(t, msgRx)
	time.Sleep(1000 * time.Millisecond)
	err = msgRx.Resend(newChannel)
	require.NoError(t, err)
	time.Sleep(1000 * time.Millisecond)
	rxRes, err := client.RQM().SetChannel(channel).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(1).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 0, len(rxRes.Messages))
	rxRes, err = client.RQM().SetChannel(newChannel).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(1).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 1, len(rxRes.Messages))

}
func TestGRPCServer_Queue_SendAndStreamReceiveWithResendWithNewMessage(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig, gs, svc := setup(ctx, t)
	defer tearDown(gs, svc)
	defer cancel()
	client, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
	require.NoError(t, err)
	defer func() {
		_ = client.Close()
	}()

	channel := nuid.Next()
	newChannel := nuid.Next()
	msg := client.QM()
	result, err := msg.SetId(nuid.Next()).
		SetChannel(channel).
		SetBody([]byte("msg resend")).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, result)
	time.Sleep(1000 * time.Millisecond)
	client2, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
	require.NoError(t, err)
	defer func() {
		_ = client2.Close()
	}()

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
	rxRes, err := client.RQM().SetChannel(channel).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(1).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 0, len(rxRes.Messages))
	rxRes, err = client.RQM().SetChannel(newChannel).SetMaxNumberOfMessages(1).SetWaitTimeSeconds(1).Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, rxRes)
	require.Equal(t, 1, len(rxRes.Messages))

}
func TestGRPCServer_PayloadSize(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	time.Sleep(time.Second)
	client, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
	require.NoError(t, err)
	defer func() {
		_ = client.Close()
	}()
	channel := nuid.Next()
	batch := client.NewQueueMessages()
	for j := 0; j < 100; j++ {
		msgID := fmt.Sprintf("client-%d-%s", j, nuid.Next())
		batch.Add(client.NewQueueMessage().
			SetId(msgID).
			SetChannel(channel).
			SetBody(randStringBytes(100000)))
	}
	results, err := batch.Send(ctx)
	require.NoError(t, err)
	for _, result := range results {
		require.False(t, result.IsError)
	}
	receiveResult, err := client.NewReceiveQueueMessagesRequest().
		SetChannel(channel).
		SetMaxNumberOfMessages(100).
		SetWaitTimeSeconds(60).
		Send(ctx)
	require.NoError(t, err)
	require.NotNil(t, receiveResult)
	require.False(t, receiveResult.IsError)
	require.True(t, len(receiveResult.Messages) == 100)
}
func TestGRPCServer_QueuesClient_Send(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	time.Sleep(time.Second)
	queueClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("0.0.0.0", appConfig.Grpc.Port),
		queues_stream.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	messages := createBatchMessages("queues_stream.send", 10)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
}
func TestGRPCServer_QueuesClient_Send_WithInvalid_Data(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	time.Sleep(time.Second)
	queueClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("0.0.0.0", appConfig.Grpc.Port),
		queues_stream.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	messages := createBatchMessages("", 10)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(result.Results))
	for _, messageResult := range result.Results {
		require.True(t, messageResult.IsError)
	}
}
func TestGRPCServer_QueuesClient_Send_Concurrent(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	time.Sleep(time.Second)
	queueClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("0.0.0.0", appConfig.Grpc.Port),
		queues_stream.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	counter := *atomic.NewInt32(0)
	wg := sync.WaitGroup{}
	wg.Add(20)

	for i := 0; i < 20; i++ {
		go func(index int) {
			defer wg.Done()
			messages := createBatchMessages(fmt.Sprintf("queues_stream.send.%d", index), 20000)
			result, err := queueClient.Send(ctx, messages...)
			require.NoError(t, err)
			require.EqualValues(t, 20000, len(result.Results))
			for _, messageResult := range result.Results {
				require.False(t, messageResult.IsError)
			}
			counter.Add(int32(len(result.Results)))
		}(i)
	}
	wg.Wait()
	require.EqualValues(t, int32(400000), counter.Load())

}
func TestGRPCServer_QueuesClient_Poll_AutoAck(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	time.Sleep(time.Second)
	queueClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("0.0.0.0", appConfig.Grpc.Port),
		queues_stream.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 10
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	pollRequest2 := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response2.Messages))
}
func TestGRPCServer_QueuesClient_Poll_ManualAck_AckAll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	time.Sleep(time.Second)
	queueClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("0.0.0.0", appConfig.Grpc.Port),
		queues_stream.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 10
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)

	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	err = response.AckAll()
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	err = response.AckAll()
	require.NoError(t, err)
	pollRequest2 := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response2.Messages))
}
func TestGRPCServer_QueuesClient_Poll_ManualAck_NAckAll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	time.Sleep(time.Second)
	queueClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("0.0.0.0", appConfig.Grpc.Port),
		queues_stream.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 10
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}

	pollRequest := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)

	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))

	err = response.NAckAll()
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	//require.Error(t, response.NAckAll())

	pollRequest2 := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response2.Messages))
}
func TestGRPCServer_QueuesClient_Poll_ManualAck_ReQueueAll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	time.Sleep(time.Second)
	queueClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("0.0.0.0", appConfig.Grpc.Port),
		queues_stream.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 10
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)

	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	reQueueChannel := uuid.New()
	err = response.ReQueueAll(reQueueChannel)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	//	require.Error(t, response.ReQueueAll(reQueueChannel))

	pollRequest2 := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response2.Messages))
	pollRequest3 := queues_stream.NewPollRequest().
		SetChannel(reQueueChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response3, err := queueClient.Poll(ctx, pollRequest3)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response3.Messages))
}
func TestGRPCServer_QueuesClient_Poll_ManualAck_AckRange(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	time.Sleep(time.Second)
	queueClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("0.0.0.0", appConfig.Grpc.Port),
		queues_stream.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 1000
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	for i, msg := range response.Messages {
		err := msg.Ack()
		require.NoErrorf(t, err, "msg id %d", i)
	}
	pollRequest2 := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response2.Messages))
}
func TestGRPCServer_QueuesClient_Poll_ManualAck_NAckRange(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	time.Sleep(time.Second)
	queueClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("0.0.0.0", appConfig.Grpc.Port),
		queues_stream.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 1000
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	for i, msg := range response.Messages {
		err := msg.NAck()
		require.NoErrorf(t, err, "msg id %d", i)
	}
	pollRequest2 := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response2.Messages))
}
func TestGRPCServer_QueuesClient_Poll_ManualAck_ReQueueRange(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	time.Sleep(time.Second)
	queueClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("0.0.0.0", appConfig.Grpc.Port),
		queues_stream.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 1000
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	reQueueChannel := uuid.New()
	for i, msg := range response.Messages {
		err := msg.ReQueue(reQueueChannel)
		require.NoErrorf(t, err, "msg id %d", i)
	}
	pollRequest2 := queues_stream.NewPollRequest().
		SetChannel(reQueueChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response2.Messages))
	pollRequest3 := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response3, err := queueClient.Poll(ctx, pollRequest3)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response3.Messages))
}
func TestGRPCServer_QueuesClient_Poll_ManualAck_Close(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	time.Sleep(time.Second)
	queueClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("0.0.0.0", appConfig.Grpc.Port),
		queues_stream.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 1000
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	err = response.Close()
	require.NoError(t, err)
	pollRequest2 := queues_stream.NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response2.Messages))
}
func TestGRPCServer_QueuesClient_Poll_InvalidRequest(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	time.Sleep(time.Second)
	queueClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("0.0.0.0", appConfig.Grpc.Port),
		queues_stream.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	pollRequest := queues_stream.NewPollRequest().
		SetChannel("").
		SetAutoAck(false).
		SetMaxItems(0).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.Error(t, err)
	require.Nil(t, response)

}
func TestGRPCServer_QueuesClient_Poll_InvalidRequestByServer(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	time.Sleep(time.Second)
	queueClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("0.0.0.0", appConfig.Grpc.Port),
		queues_stream.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	pollRequest := queues_stream.NewPollRequest().
		SetChannel(">").
		SetAutoAck(false).
		SetMaxItems(0).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.Error(t, err)
	require.Nil(t, response)

}

