package client

import (
	"context"
	"fmt"
	"github.com/fortytw2/leaktest"
	"github.com/kubemq-io/broker/pkg/pipe"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/services/broker"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/nats-io/nuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

var testPort = atomic.NewInt32(31111)

func init() {
	_ = logging.CreateLoggerFactory(context.Background(), "test", &config.LogConfig{
		Level: "debug",
	})

}

func waitForAtLeastCount(fn func() int, atLeast, timeout int) error {
	for i := 0; i < 10*timeout; i++ {
		if fn() >= atLeast {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout, want at least %d, got %d", atLeast, fn())
}

func getConfig(t *testing.T) *config.Config {
	c := config.GetCopyAppConfig("../../test/")
	c.Broker.Port = int(testPort.Inc())
	c.Broker.MemoryPipe = pipe.NewPipe("testing-pipe " + nuid.Next())
	c.Broker.MonitoringPort = int(testPort.Inc())
	return c
}
func getClientOptions(t *testing.T, c *config.Config) *Options {
	o := NewClientOptions(nuid.Next()).SetMemoryPipe(c.Broker.MemoryPipe).SetAutoReconnect(false)
	return o
}
func setupSingle(ctx context.Context, t *testing.T, c *config.Config) *broker.Service {
	tmp, err := ioutil.TempDir("./", "test_single_1")
	require.NoError(t, err)
	c.Store.StorePath = tmp
	brokerServer := broker.New(c)
	brokerServer, err = brokerServer.Start(ctx)
	require.NoError(t, err)
	brokerServer.DisableMetricsReporting()
	timer := time.NewTimer(10 * time.Second)
	for {
		if brokerServer.IsReady() {
			return brokerServer
		}

		select {
		case <-timer.C:
			require.True(t, brokerServer.IsReady())
		default:

		}
	}

}
func tearDownSingle(s *broker.Service, c *config.Config) {

	s.Close()
	<-s.Stopped
	err := os.RemoveAll(c.Store.StorePath)
	if err != nil {
		panic(err)
	}
}

func TestClient_NewClient(t *testing.T) {
	defer leaktest.Check(t)()
	a := getConfig(t)
	s1 := setupSingle(context.Background(), t, a)
	defer tearDownSingle(s1, a)
	o := getClientOptions(t, a)
	c, err := NewClient(o)
	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	_ = c.Disconnect()
	time.Sleep(1 * time.Second)
	assert.False(t, c.isUp.Load())

}
func TestStoreClient_NewClient(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)

	c, err := NewStoreClient(o)
	require.NoError(t, err)
	require.True(t, c.isUp.Load())
	ch := make(chan *pb.EventReceive, 1)
	require.NoError(t, err)
	subReq := &pb.Subscribe{
		SubscribeTypeData:    pb.Subscribe_Events,
		ClientID:             "test-client-id",
		Channel:              "some-channel",
		Group:                "",
		EventsStoreTypeData:  pb.Subscribe_StartFromLast,
		EventsStoreTypeValue: 0,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	err = c.SubscribeToEventsStore(ctx, subReq, ch)
	require.NoError(t, err)
	_ = c.Disconnect()
	time.Sleep(1 * time.Second)
	c2, err := NewStoreClient(o)
	assert.NoError(t, err)
	assert.True(t, c2.isUp.Load())
	err = c2.SubscribeToEventsStore(ctx, subReq, ch)
	assert.NoError(t, err)
	_ = c2.Disconnect()
}
func TestClient_Single_Events(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	require.EqualValues(t, broker.Standalone, s1.Snats.State())
	channel := "TestClient_Single_PublishSubscribe"
	ready := make(chan bool)
	pubMsg := &pb.Event{ClientID: "test-client-id", Channel: channel, Body: []byte("some-body")}
	go func() {
		c2, err := NewClient(o)
		require.NoError(t, err)
		defer func() {
			_ = c2.Disconnect()
		}()
		msgCh := make(chan *pb.EventReceive)
		require.NoError(t, err)
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
		err = c2.SubscribeToEvents(ctx, subReq, msgCh)
		require.NoError(t, err)
		ready <- true
		msg := <-msgCh
		assert.EqualValues(t, []byte("some-body"), msg.Body)

		ready <- true
	}()
	<-ready
	time.Sleep(time.Second)
	c, err := NewClient(o)
	require.NoError(t, err)
	defer func() {
		_ = c.Disconnect()
	}()

	md, err := c.SendEvents(ctx, pubMsg)
	require.NoError(t, err)
	require.True(t, md.Sent)
	<-ready
}
func TestClient_Single_EventsWithNoChannelError(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())

	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	require.EqualValues(t, broker.Standalone, s1.Snats.State())
	c, err := NewClient(o)
	require.NoError(t, err)
	defer func() {
		_ = c.Disconnect()
	}()

	msg := &pb.Event{ClientID: "test-client-id", Body: []byte("some-body")}
	md, err := c.SendEvents(ctx, msg)
	require.Error(t, err)
	require.Nil(t, md)
}
func TestClient_Single_EventsWithInvalidChannelChars(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	require.EqualValues(t, broker.Standalone, s1.Snats.State())
	c, err := NewClient(o)
	require.NoError(t, err)
	defer func() {
		_ = c.Disconnect()
	}()

	msg := &pb.Event{ClientID: "test-client-id", Channel: "some.*.invalid.channel.>", Body: []byte("some-body")}
	md, err := c.SendEvents(ctx, msg)
	require.Error(t, err)
	require.Nil(t, md)
}
func TestClient_Single_Query(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	count1 := atomic.NewInt32(0)
	count2 := atomic.NewInt32(0)
	count3 := atomic.NewInt32(0)

	go func() {
		o := getClientOptions(t, a)
		nc1, err := NewClient(o)
		require.NoError(t, err)
		defer func() {
			_ = nc1.Disconnect()
		}()
		rx := make(chan *pb.Request)

		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Queries,
			ClientID:             "test-client-id1",
			Channel:              "chan1",
			Group:                "q1",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		err = nc1.SubscribeToQueries(ctx, subReq, rx)
		require.NoError(t, err)
		for {
			select {
			case r := <-rx:
				err := nc1.SendResponse(ctx, &pb.Response{ClientID: "test-client-id", Body: r.Body, ReplyChannel: r.ReplyChannel, RequestID: r.RequestID})
				if err != nil {
					require.NoError(t, err)
				}
				count1.Inc()
			case <-ctx.Done():
				return
			}

		}
	}()
	go func() {
		o := getClientOptions(t, a)
		nc2, err := NewClient(o)
		require.NoError(t, err)
		defer func() {
			_ = nc2.Disconnect()
		}()
		rx := make(chan *pb.Request)
		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Queries,
			ClientID:             "test-client-id2",
			Channel:              "chan1",
			Group:                "q1",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		_ = nc2.SubscribeToQueries(ctx, subReq, rx)
		for {
			select {
			case r := <-rx:
				err := nc2.SendResponse(ctx, &pb.Response{ClientID: "test-client-id", Body: r.Body, ReplyChannel: r.ReplyChannel, RequestID: r.RequestID})
				if err != nil {
					require.NoError(t, err)
				}
				count2.Inc()
				return
			case <-ctx.Done():
				return
			}
		}

	}()
	time.Sleep(2 * time.Second)
	o := getClientOptions(t, a)
	nc, _ := NewClient(o)
	defer func() {
		_ = nc.Disconnect()
	}()
	for i := 0; i < 100; i++ {
		res, err := nc.SendQuery(ctx, &pb.Request{ClientID: "test-client-id", Channel: "chan1", Body: []byte("some-request"), Timeout: 1000})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, string(res.Body), ("some-request"))
		if err == nil {
			count3.Inc()
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(t, count3.Load(), count2.Load()+count1.Load())
}
func TestStoreClient_Single_StartFromLast(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	o2 := getClientOptions(t, a)
	require.EqualValues(t, broker.Standalone, s1.Snats.State())
	c, err := NewStoreClient(o)
	require.NoError(t, err)
	defer func() {
		_ = c.Disconnect()
	}()

	channel := "TestStoreClient_Single_StartFromLast"
	ready := make(chan bool, 1)
	count1 := atomic.NewInt32(0)
	go func() {
		c2, err := NewStoreClient(o2)
		require.NoError(t, err)
		defer func() {
			_ = c2.Disconnect()
		}()
		ch := make(chan *pb.EventReceive, 1)

		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_EventsStore,
			ClientID:             "test-client-id",
			Channel:              channel,
			Group:                "",
			EventsStoreTypeData:  pb.Subscribe_StartNewOnly,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		err = c2.SubscribeToEventsStore(ctx, subReq, ch)
		require.NoError(t, err)
		ready <- true
		msg := <-ch
		require.EqualValues(t, []byte("some-body"), msg.Body)
		count1.Inc()
		//ready <- true
	}()
	<-ready
	msg := &pb.Event{
		EventID:              "",
		ClientID:             "test-client-id",
		Channel:              channel,
		Metadata:             "",
		Body:                 []byte("some-body"),
		Store:                true,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	md, err := c.SendEventsStore(ctx, msg)
	require.NoError(t, err)
	require.True(t, md.Sent)
	time.Sleep(1 * time.Second)
	assert.Equal(t, int32(1), count1.Load())
}
func TestStoreClient_Single_StartFromTimeDelta(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o1 := getClientOptions(t, a)
	o2 := getClientOptions(t, a)
	o3 := getClientOptions(t, a)
	channel := "TestStoreClient_Cluster_StartFromTimeDelta"
	count1 := atomic.NewInt32(0)
	count2 := atomic.NewInt32(0)

	c, err := NewStoreClient(o1)
	require.NoError(t, err)
	defer func() {
		_ = c.Disconnect()
	}()

	// start sending 10 messages from t1
	t1 := time.Now()
	for i := 0; i < 100; i++ {
		msg := &pb.Event{
			EventID:              "",
			ClientID:             "test-client-id",
			Channel:              channel,
			Metadata:             fmt.Sprintf("%d", i),
			Body:                 []byte("some-body"),
			Store:                true,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		md, err := c.SendEventsStore(ctx, msg)
		require.NoError(t, err)
		require.True(t, md.Sent)
	}
	time.Sleep(1 * time.Second)

	go func() {
		c2, err := NewStoreClient(o2)
		require.NoError(t, err)
		defer func() {
			_ = c2.Disconnect()
		}()
		ch := make(chan *pb.EventReceive, 1)
		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_EventsStore,
			ClientID:             "test-client-id",
			Channel:              channel,
			Group:                "",
			EventsStoreTypeData:  pb.Subscribe_StartAtTimeDelta,
			EventsStoreTypeValue: time.Now().UnixNano() - t1.UnixNano(),
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		err = c2.SubscribeToEventsStore(ctx, subReq, ch)
		require.NoError(t, err)

		for {
			select {
			case msg := <-ch:
				require.EqualValues(t, []byte("some-body"), msg.Body)
				count1.Inc()
			case <-ctx.Done():
				return
			}
		}

	}()
	time.Sleep(1 * time.Second)
	go func() {
		c3, err := NewStoreClient(o3)
		require.NoError(t, err)
		defer func() {
			_ = c3.Disconnect()
		}()
		ch := make(chan *pb.EventReceive, 1)
		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_EventsStore,
			ClientID:             "test-client-id",
			Channel:              channel,
			Group:                "",
			EventsStoreTypeData:  pb.Subscribe_StartAtTimeDelta,
			EventsStoreTypeValue: 1,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		err = c3.SubscribeToEventsStore(ctx, subReq, ch)
		require.NoError(t, err)

		for {
			select {
			case msg := <-ch:
				require.EqualValues(t, []byte("some-body"), msg.Body)
				count2.Inc()
			case <-ctx.Done():
				return
			}
		}

	}()
	time.Sleep(1 * time.Second)
	//checking we got all the messages
	err = waitForAtLeastCount(func() int {
		return int(count1.Load() + count2.Load())
	}, 100, 5)
	assert.NoError(t, err)
}
func TestStoreClient_Single_MaxMessagesPerQueue(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	a.Store.MaxMessages = 5
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o1 := getClientOptions(t, a)
	o2 := getClientOptions(t, a)
	channel := "TestStoreClient_Single_MaxMessagesPerQueue"
	count1 := atomic.NewInt32(0)
	c, err := NewStoreClient(o1)
	require.NoError(t, err)
	defer func() {
		_ = c.Disconnect()
	}()

	for i := 0; i < 100; i++ {
		msg := &pb.Event{
			EventID:              "",
			ClientID:             "test-client-id",
			Channel:              channel,
			Metadata:             fmt.Sprintf("%d", i),
			Body:                 []byte("some-body"),
			Store:                true,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		md, err := c.SendEventsStore(ctx, msg)
		require.NoError(t, err)
		require.True(t, md.Sent)
	}
	time.Sleep(1 * time.Second)

	go func() {
		c2, err := NewStoreClient(o2)
		defer func() {
			_ = c2.Disconnect()
		}()
		ch := make(chan *pb.EventReceive, 1)
		require.NoError(t, err)
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
		err = c2.SubscribeToEventsStore(ctx, subReq, ch)
		require.NoError(t, err)

		for {
			select {

			case msg := <-ch:
				require.EqualValues(t, []byte("some-body"), msg.Body)
				count1.Inc()
			case <-ctx.Done():
				return
			}
		}

	}()

	time.Sleep(2 * time.Second)
	assert.Equal(t, int32(5), count1.Load())
}
func TestStoreClient_Single_MaxSizeOfMessageForQueue(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())

	a := getConfig(t)
	a.Store.MaxQueueSize = 2600
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o1 := getClientOptions(t, a)
	o2 := getClientOptions(t, a)

	channel := "TestStoreClient_Single_MaxMessagesPerQueue"
	count1 := atomic.NewInt32(0)
	c, err := NewStoreClient(o1)
	require.NoError(t, err)
	defer func() {
		_ = c.Disconnect()
	}()

	for i := 0; i < 10; i++ {
		msg := &pb.Event{
			EventID:              "",
			ClientID:             "test-client-id",
			Channel:              channel,
			Metadata:             fmt.Sprintf("%d", i),
			Body:                 []byte("bigger-message - some-body"),
			Store:                true,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		md, err := c.SendEventsStore(ctx, msg)
		require.NoError(t, err)
		require.True(t, md.Sent)
	}

	time.Sleep(1 * time.Second)

	go func() {
		c2, err := NewStoreClient(o2)
		defer func() {
			_ = c2.Disconnect()
		}()
		ch := make(chan *pb.EventReceive, 1)
		require.NoError(t, err)
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
		err = c2.SubscribeToEventsStore(ctx, subReq, ch)
		require.NoError(t, err)
		for {
			select {
			case <-ch:
				count1.Inc()
			case <-ctx.Done():
				return
			}
		}
	}()

	time.Sleep(1 * time.Second)
	assert.Equal(t, int32(10), count1.Load())
}
func TestStoreClient_Single_MaxQueues(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	a.Store.MaxQueues = 1
	o1 := getClientOptions(t, a)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	channel1 := "TestStoreClient_Single_MaxMessagesPerQueue.1"
	channel2 := "TestStoreClient_Single_MaxMessagesPerQueue.2"
	c, err := NewStoreClient(o1)
	require.NoError(t, err)
	defer func() {
		_ = c.Disconnect()
	}()

	msg1 := &pb.Event{
		EventID:              "",
		ClientID:             "test-client-id",
		Channel:              channel1,
		Metadata:             "",
		Body:                 []byte("some-body"),
		Store:                true,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	md, err := c.SendEventsStore(ctx, msg1)
	time.Sleep(2 * time.Second)
	require.NoError(t, err)
	require.True(t, md.Sent)

	msg2 := &pb.Event{
		EventID:              "",
		ClientID:             "test-client-id",
		Channel:              channel2,
		Metadata:             "",
		Body:                 []byte("some-body"),
		Store:                true,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	md, err = c.SendEventsStore(ctx, msg2)
	time.Sleep(2 * time.Second)
	require.NoError(t, err)
	require.True(t, md.Sent)

}
