package array

import (
	"context"
	"github.com/kubemq-io/broker/pkg/pipe"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/nats-io/nuid"

	"io/ioutil"
	"os"
	"testing"

	"github.com/fortytw2/leaktest"

	"github.com/kubemq-io/kubemq-community/services/broker"

	"time"

	"fmt"

	"github.com/kubemq-io/kubemq-community/config"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

var testPort = *atomic.NewInt32(int32(39500))

func init() {
	_ = logging.CreateLoggerFactory(context.Background(), "test", &config.LogConfig{
		Level: "debug",
	})

}

func getAppConfig() *config.Config {
	tmp, err := ioutil.TempDir("./", "test_array")
	if err != nil {
		panic(err)
	}
	appConfig := config.GetCopyAppConfig("../../test/")
	appConfig.Broker.MonitoringPort = int(testPort.Inc())
	appConfig.Api.Port = int(testPort.Inc())
	appConfig.Broker.MemoryPipe = pipe.NewPipe(nuid.Next())
	appConfig.Store.StorePath = tmp
	return appConfig
}
func setup(ctx context.Context, t *testing.T, appConfigs ...*config.Config) (*Array, *broker.Service) {
	var appConfig *config.Config
	if len(appConfigs) == 0 {
		appConfig = getAppConfig()
	} else {
		appConfig = appConfigs[0]
	}
	var err error
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
	na, err := Start(ctx, appConfig)
	require.NoError(t, err)
	require.NotNil(t, na)
	return na, brokerServer
}

func tearDown(na *Array, ns *broker.Service) {
	na.Close()
	<-na.Stopped
	ns.Close()
	<-ns.Stopped
	err := os.RemoveAll(na.appConfig.Store.StorePath)
	if err != nil {
		panic(err)
	}

}

func TestArray_SendReceiveEvents(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	ready := make(chan struct{}, 10)
	body := "some-publish-message"
	channel := "TestArray_Subscribe"
	msg := &pb.Event{ClientID: "test-client-id", Channel: channel, Body: []byte(body)}
	go func() {
		rxCh := make(chan *pb.EventReceive, 10)
		errCh := make(chan error, 1)
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
		id, err := na.SubscribeEvents(ctx, subReq, rxCh, errCh)
		defer func() {
			_ = na.DeleteClient(id)
		}()

		require.NoError(t, err)
		ready <- struct{}{}
		for {
			select {
			case <-ctx.Done():
				return
			case rxMsg := <-rxCh:
				assert.EqualValues(t, msg.Body, rxMsg.Body)
			}
		}
	}()
	<-ready
	time.Sleep(time.Second)
	md, err := na.SendEvents(ctx, msg)
	require.NoError(t, err)
	require.True(t, md.Sent)

}
func TestArray_SendReceiveEventsStore(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	ready := make(chan struct{}, 10)
	body := "some-publish-message"
	channel := "TestArray_Subscribe"
	msg := &pb.Event{
		EventID:              "",
		ClientID:             "test-client-id",
		Channel:              channel,
		Metadata:             "",
		Body:                 []byte(body),
		Store:                true,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	go func() {
		rxCh := make(chan *pb.EventReceive, 10)
		errCh := make(chan error, 1)
		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_EventsStore,
			ClientID:             "test-client-id",
			Channel:              channel,
			Group:                "",
			EventsStoreTypeData:  2,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		id, err := na.SubscribeEventsStore(ctx, subReq, rxCh, errCh)
		require.NoError(t, err)

		defer func() {
			_ = na.DeleteClient(id)
		}()
		if err != nil {
			panic(err)
		}
		ready <- struct{}{}
		for {
			select {
			case <-ctx.Done():
				return
			case rxMsg := <-rxCh:
				assert.EqualValues(t, msg.Body, rxMsg.Body)
			}
		}
	}()
	<-ready
	time.Sleep(time.Second)
	md, err := na.SendEventsStore(ctx, msg)
	require.NoError(t, err)
	require.True(t, md.Sent)

}

func TestArray_CommandRequestResponse_Error(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)

	body := "some-request-message"
	channel := "TestArray_RequestResponse_Timeout"
	req := &pb.Request{
		Channel:  channel,
		Body:     []byte(body),
		Timeout:  1000,
		CacheKey: "",
		CacheTTL: 0,
		ClientID: "client_sub_request",
	}
	ready := make(chan struct{}, 10)
	go func() {
		rxCh := make(chan *pb.Request, 10)
		errCh := make(chan error, 1)

		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Commands,
			ClientID:             "test-client-id",
			Channel:              channel,
			Group:                "",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		id, err := na.SubscribeToCommands(ctx, subReq, rxCh, errCh)
		defer func() {
			_ = na.DeleteClient(id)
		}()
		if err != nil {
			panic(err)
		}

		ready <- struct{}{}
		for {
			select {
			case <-ctx.Done():
				return
			case rxReq := <-rxCh:
				assert.Equal(t, channel, rxReq.Channel)
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}

	}()
	<-ready
	time.Sleep(time.Second)
	_, err := na.SendCommand(ctx, req)
	require.Error(t, err)

}

func TestArray_CommandRequestResponse(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)

	body := "some-request-message"
	channel := "TestArray_RequestResponse_Timeout"
	req := &pb.Request{
		Channel:  channel,
		Body:     []byte(body),
		Timeout:  1000,
		CacheKey: "",
		CacheTTL: 0,
		ClientID: "client_sub_request",
	}
	ready := make(chan struct{}, 10)
	go func() {
		rxCh := make(chan *pb.Request, 10)
		errCh := make(chan error, 1)

		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Commands,
			ClientID:             "test-client-id",
			Channel:              channel,
			Group:                "",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		id, err := na.SubscribeToCommands(ctx, subReq, rxCh, errCh)
		defer func() {
			_ = na.DeleteClient(id)
		}()
		require.NoError(t, err)
		ready <- struct{}{}
		for {
			select {
			case <-ctx.Done():
				return
			case rxReq := <-rxCh:
				assert.Equal(t, channel, rxReq.Channel)
				err = na.SendResponse(ctx, &pb.Response{
					ClientID:             "some_client_id",
					RequestID:            rxReq.RequestID,
					ReplyChannel:         rxReq.ReplyChannel,
					Metadata:             "",
					Body:                 nil,
					CacheHit:             false,
					Timestamp:            time.Now().Unix(),
					Executed:             true,
					Error:                "",
					Span:                 nil,
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_sizecache:        0,
				})
				if err != nil {
					panic(err)
				}
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}

	}()
	<-ready
	time.Sleep(time.Second)
	res, err := na.SendCommand(ctx, req)
	require.NoError(t, err)
	require.True(t, res.Executed)
}

func TestArray_QueryRequestResponse_Error(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)

	body := "some-request-message"
	channel := "TestArray_RequestResponse_Timeout"
	req := &pb.Request{
		Channel:  channel,
		Body:     []byte(body),
		Timeout:  1000,
		CacheKey: "",
		CacheTTL: 0,
		ClientID: "client_sub_request",
	}
	ready := make(chan struct{}, 10)
	go func() {
		rxCh := make(chan *pb.Request, 10)
		errCh := make(chan error, 1)

		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Queries,
			ClientID:             "test-client-id",
			Channel:              channel,
			Group:                "",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		id, err := na.SubscribeToQueries(ctx, subReq, rxCh, errCh)
		defer func() {
			_ = na.DeleteClient(id)
		}()
		require.NoError(t, err)
		ready <- struct{}{}
		for {
			select {
			case <-ctx.Done():
				return
			case rxReq := <-rxCh:
				assert.Equal(t, channel, rxReq.Channel)
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}

	}()
	<-ready
	time.Sleep(time.Second)
	res, err := na.SendQuery(ctx, req)
	require.Error(t, err)
	require.Nil(t, res)
}

func TestArray_QueryRequestResponse(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)

	body := "some-request-message"
	channel := "TestArray_RequestResponse_Timeout"
	req := &pb.Request{
		Channel:  channel,
		Body:     []byte(body),
		Timeout:  1000,
		CacheKey: "",
		CacheTTL: 0,
		ClientID: "client_sub_request",
	}
	ready := make(chan struct{}, 10)
	go func() {
		rxCh := make(chan *pb.Request, 10)
		errCh := make(chan error, 1)

		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Queries,
			ClientID:             "test-client-id",
			Channel:              channel,
			Group:                "",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		id, err := na.SubscribeToQueries(ctx, subReq, rxCh, errCh)
		defer func() {
			_ = na.DeleteClient(id)
		}()
		require.NoError(t, err)
		ready <- struct{}{}
		for {
			select {
			case <-ctx.Done():
				return
			case rxReq := <-rxCh:
				assert.Equal(t, channel, rxReq.Channel)
				_ = na.SendResponse(ctx, &pb.Response{
					ClientID:             "some_clinet_id_receiver",
					RequestID:            rxReq.RequestID,
					ReplyChannel:         rxReq.ReplyChannel,
					Metadata:             "some_meta",
					Body:                 []byte("some_body"),
					CacheHit:             false,
					Timestamp:            time.Now().UTC().Unix(),
					Executed:             false,
					Error:                "",
					Span:                 nil,
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_sizecache:        0,
				})
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}

	}()
	<-ready
	time.Sleep(time.Second)
	res, err := na.SendQuery(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestArray_Store_PublishSubscribe(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	ready := make(chan struct{}, 10)
	count1 := atomic.NewInt32(0)
	channel := "TestArray_Store_PublishSubscribe"
	clientID := "publish_persistence_sender"
	msg := &pb.Event{
		ClientID: clientID,
		Channel:  channel,
		Body:     []byte("some-publish-message-sdvvvvvvvvvvvvvvvvvvvvvvvvvvvvv-s-dvsdvsdvds-sdvsdvsdv34r5532rnsdj,sdkdsjksfedjlefwhlwhjwqelwqeljweqljwqejlwqejlwejbsnambsacnmcbnmb.nqluwrehquwgrqnwdq,nmwd"),
		Store:    true,
	}
	messagesMap := make(map[string]string)
	for i := 0; i < 100; i++ {
		md, err := na.SendEventsStore(ctx, msg)
		require.NoError(t, err)
		require.True(t, md.Sent)
		messagesMap[md.EventID] = md.EventID
		msg.EventID = ""
	}
	time.Sleep(1 * time.Second)
	require.Equal(t, 100, len(messagesMap))
	go func() {
		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_EventsStore,
			ClientID:             "test-client-id",
			Channel:              channel,
			Group:                "",
			EventsStoreTypeData:  pb.Subscribe_StartFromFirst,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		msgCh := make(chan *pb.EventReceive)
		errCh := make(chan error, 1)
		id, err := na.SubscribeEventsStore(ctx, subReq, msgCh, errCh)
		defer func() {
			_ = na.DeleteClient(id)
		}()
		require.NoError(t, err)
		ready <- struct{}{}
		for {
			select {
			case msg := <-msgCh:
				if _, ok := messagesMap[msg.EventID]; ok {
					count1.Inc()
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	<-ready
	err := waitForCount(func() int {
		return int(count1.Load())
	}, 100, 5)
	require.NoError(t, err)

}

func TestArray_Store_DuplicateClientError(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := "TestArray_Store_DuplicateClientError"
	clientID := "duplicated_clientID"
	msgCh := make(chan *pb.EventReceive, 1)
	errCh := make(chan error, 1)

	subReq := &pb.Subscribe{
		SubscribeTypeData:    pb.Subscribe_EventsStore,
		ClientID:             clientID,
		Channel:              channel,
		Group:                "",
		EventsStoreTypeData:  pb.Subscribe_StartFromFirst,
		EventsStoreTypeValue: 0,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	_, err := na.SubscribeEventsStore(ctx, subReq, msgCh, errCh)
	require.NoError(t, err)
	_, err = na.SubscribeEventsStore(ctx, subReq, msgCh, errCh)
	require.Error(t, err)
	err = na.DeleteClient(clientID)
	require.NoError(t, err)
	_, err = na.SubscribeEventsStore(ctx, subReq, msgCh, errCh)
	require.NoError(t, err)
	err = na.DeleteClient(clientID)
	require.NoError(t, err)

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

func TestArray_Queue_SendAndReceive(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := "TestArray_Queue_SendReceive"
	clientID := "some-clientID"
	messagesList := &pb.QueueMessagesBatchRequest{
		BatchID:  nuid.Next(),
		Messages: []*pb.QueueMessage{},
	}
	sendSingleResponse, err := na.SendQueueMessage(ctx, &pb.QueueMessage{
		MessageID:            nuid.Next(),
		ClientID:             clientID,
		Channel:              channel,
		Metadata:             "",
		Body:                 []byte(fmt.Sprintf("msg #%d", 0)),
		Tags:                 nil,
		Attributes:           nil,
		Policy:               nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})
	require.NoError(t, err)
	require.False(t, sendSingleResponse.IsError)

	for i := 1; i < 10; i++ {
		messagesList.Messages = append(messagesList.Messages, &pb.QueueMessage{
			MessageID:            nuid.Next(),
			ClientID:             clientID,
			Channel:              channel,
			Metadata:             "",
			Body:                 []byte(fmt.Sprintf("msg #%d", i)),
			Tags:                 nil,
			Attributes:           nil,
			Policy:               nil,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		})
	}
	sendResponse, err := na.SendQueueMessagesBatch(ctx, messagesList)
	require.NoError(t, err)
	require.False(t, sendResponse.HaveErrors)

	res, err := na.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:            "",
		ClientID:             clientID,
		Channel:              channel,
		MaxNumberOfMessages:  10,
		WaitTimeSeconds:      1,
		IsPeak:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	require.NoError(t, err)
	require.Equal(t, 10, len(res.Messages))
}

func TestArray_Queue_SendAndReceiveWithPeak(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := "TestArray_Queue_SendReceive"
	clientID := "some-clientID"
	messagesList := &pb.QueueMessagesBatchRequest{
		BatchID:  nuid.Next(),
		Messages: []*pb.QueueMessage{},
	}
	sendSingleResponse, err := na.SendQueueMessage(ctx, &pb.QueueMessage{
		MessageID:            nuid.Next(),
		ClientID:             clientID,
		Channel:              channel,
		Metadata:             "",
		Body:                 []byte(fmt.Sprintf("msg #%d", 0)),
		Tags:                 nil,
		Attributes:           nil,
		Policy:               nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})
	require.NoError(t, err)
	require.False(t, sendSingleResponse.IsError)

	for i := 1; i < 10; i++ {
		messagesList.Messages = append(messagesList.Messages, &pb.QueueMessage{
			MessageID:            nuid.Next(),
			ClientID:             clientID,
			Channel:              channel,
			Metadata:             "",
			Body:                 []byte(fmt.Sprintf("msg #%d", i)),
			Tags:                 nil,
			Attributes:           nil,
			Policy:               nil,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		})
	}
	sendResponse, err := na.SendQueueMessagesBatch(ctx, messagesList)
	require.NoError(t, err)
	require.False(t, sendResponse.HaveErrors)

	res, err := na.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:            "",
		ClientID:             clientID,
		Channel:              channel,
		MaxNumberOfMessages:  6,
		WaitTimeSeconds:      1,
		IsPeak:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	require.NoError(t, err)
	require.Equal(t, 6, len(res.Messages))
	res, err = na.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:            "",
		ClientID:             clientID,
		Channel:              channel,
		MaxNumberOfMessages:  10,
		WaitTimeSeconds:      1,
		IsPeak:               true,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	require.NoError(t, err)
	require.Equal(t, 4, len(res.Messages))

	res, err = na.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:            "",
		ClientID:             clientID,
		Channel:              channel,
		MaxNumberOfMessages:  10,
		WaitTimeSeconds:      1,
		IsPeak:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	require.NoError(t, err)
	require.Equal(t, 4, len(res.Messages))

	res, err = na.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:            "",
		ClientID:             clientID,
		Channel:              channel,
		MaxNumberOfMessages:  10,
		WaitTimeSeconds:      1,
		IsPeak:               true,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	require.NoError(t, err)
	require.Equal(t, 0, len(res.Messages))

}

func TestArray_Queue_SendAndReceiveWithAckAll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := "TestArray_Queue_SendReceive"
	clientID := "some-clientID"
	messagesList := &pb.QueueMessagesBatchRequest{
		BatchID:  nuid.Next(),
		Messages: []*pb.QueueMessage{},
	}
	sendSingleResponse, err := na.SendQueueMessage(ctx, &pb.QueueMessage{
		MessageID:            nuid.Next(),
		ClientID:             clientID,
		Channel:              channel,
		Metadata:             "",
		Body:                 []byte(fmt.Sprintf("msg #%d", 0)),
		Tags:                 nil,
		Attributes:           nil,
		Policy:               nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})
	require.NoError(t, err)
	require.False(t, sendSingleResponse.IsError)

	for i := 1; i < 10; i++ {
		messagesList.Messages = append(messagesList.Messages, &pb.QueueMessage{
			MessageID:            nuid.Next(),
			ClientID:             clientID,
			Channel:              channel,
			Metadata:             "",
			Body:                 []byte(fmt.Sprintf("msg #%d", i)),
			Tags:                 nil,
			Attributes:           nil,
			Policy:               nil,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		})
	}
	sendResponse, err := na.SendQueueMessagesBatch(ctx, messagesList)
	require.NoError(t, err)
	require.False(t, sendResponse.HaveErrors)

	res, err := na.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:            "",
		ClientID:             clientID,
		Channel:              channel,
		MaxNumberOfMessages:  10,
		WaitTimeSeconds:      1,
		IsPeak:               true,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	require.NoError(t, err)
	require.Equal(t, 10, len(res.Messages))

	resAck, err := na.AckAllQueueMessages(ctx, &pb.AckAllQueueMessagesRequest{
		RequestID:            "",
		ClientID:             clientID,
		Channel:              channel,
		WaitTimeSeconds:      1,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	require.NoError(t, err)
	require.Equal(t, uint64(10), resAck.AffectedMessages)

	res, err = na.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:            "",
		ClientID:             clientID,
		Channel:              channel,
		MaxNumberOfMessages:  10,
		WaitTimeSeconds:      1,
		IsPeak:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	require.NoError(t, err)
	require.Equal(t, 0, len(res.Messages))

}

func TestArray_Queue_StreamSendAndReceive(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)
	channel := "TestArray_Queue_SendReceive"
	clientID := "some-clientID"
	messagesList := &pb.QueueMessagesBatchRequest{
		BatchID:  nuid.Next(),
		Messages: []*pb.QueueMessage{},
	}
	sendSingleResponse, err := na.SendQueueMessage(ctx, &pb.QueueMessage{
		MessageID:            nuid.Next(),
		ClientID:             clientID,
		Channel:              channel,
		Metadata:             "",
		Body:                 []byte(fmt.Sprintf("msg #%d", 0)),
		Tags:                 nil,
		Attributes:           nil,
		Policy:               nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})
	require.NoError(t, err)
	require.False(t, sendSingleResponse.IsError)

	for i := 1; i < 10; i++ {
		messagesList.Messages = append(messagesList.Messages, &pb.QueueMessage{
			MessageID:            nuid.Next(),
			ClientID:             clientID,
			Channel:              channel,
			Metadata:             "",
			Body:                 []byte(fmt.Sprintf("msg #%d", i)),
			Tags:                 nil,
			Attributes:           nil,
			Policy:               nil,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		})
	}
	sendResponse, err := na.SendQueueMessagesBatch(ctx, messagesList)
	require.NoError(t, err)
	require.False(t, sendResponse.HaveErrors)

	counter := atomic.NewInt64(0)
	reqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
	resCh := make(chan *pb.StreamQueueMessagesResponse, 1)
	done := make(chan bool, 1)
	streamCtx, streamCancel := context.WithCancel(ctx)
	id, err := na.StreamQueueMessage(streamCtx, reqCh, resCh, done)
	require.NoError(t, err)
	defer func() {
		_ = na.DeleteClient(id)
	}()

	reqCh <- &pb.StreamQueueMessagesRequest{
		RequestID:             "",
		ClientID:              clientID,
		StreamRequestTypeData: 1,
		Channel:               channel,
		VisibilitySeconds:     10,
		WaitTimeSeconds:       10,
		RefSequence:           0,
		ModifiedMessage:       nil,
	}
	select {
	case msg := <-resCh:
		reqCh <- &pb.StreamQueueMessagesRequest{
			RequestID:             "",
			ClientID:              clientID,
			StreamRequestTypeData: pb.StreamRequestType_AckMessage,
			Channel:               channel,
			VisibilitySeconds:     10,
			WaitTimeSeconds:       10,
			RefSequence:           msg.Message.Attributes.Sequence,
			ModifiedMessage:       nil,
		}
		select {
		case <-done:
			counter.Inc()
		case <-time.After(1 * time.Second):
			t.Fatal("timeout receiving ack")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout receiving message")
	}
	streamCancel()
	require.Equal(t, int64(1), counter.Load())
	res, err := na.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:            "",
		ClientID:             clientID,
		Channel:              channel,
		MaxNumberOfMessages:  10,
		WaitTimeSeconds:      1,
		IsPeak:               true,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	require.NoError(t, err)
	require.Equal(t, 9, len(res.Messages))

}
