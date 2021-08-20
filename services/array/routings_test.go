package array

import (
	"context"
	"fmt"
	"github.com/fortytw2/leaktest"
	"github.com/kubemq-io/kubemq-community/services/routing"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/nats-io/nuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"reflect"
	"testing"
	"time"
)

type testRoutingClient struct {
	a *Array
}

func (rc *testRoutingClient) setEventSubscriber(ctx context.Context, channel string, requiredMeta string, requiredBody []byte, messages *atomic.Int32) {
	go func() {
		rxCh := make(chan *pb.EventReceive, 10)
		errCh := make(chan error, 1)
		subReq := &pb.Subscribe{
			SubscribeTypeData: pb.Subscribe_Events,
			ClientID:          "test-client-id",
			Channel:           channel,
			Group:             "",
		}
		id, err := rc.a.SubscribeEvents(ctx, subReq, rxCh, errCh)
		if err != nil {
			errCh <- err
			return
		}
		defer func() {
			_ = rc.a.DeleteClient(id)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case rxMsg := <-rxCh:
				if rxMsg.Channel != channel {
					fmt.Printf("event receive invalid channel: %s\n", rxMsg.Channel)
					return
				}
				if rxMsg.Metadata != requiredMeta {
					fmt.Printf("event receive invalid metadata: %s\n", rxMsg.Metadata)
					return
				}
				if !reflect.DeepEqual(rxMsg.Body, requiredBody) {
					fmt.Printf("event receive invalid body: %s\n", string(rxMsg.Body))
					return
				}
				messages.Inc()
			}
		}
	}()
}
func (rc *testRoutingClient) setEventStoreSubscriber(ctx context.Context, channel string, requiredMeta string, requiredBody []byte, messages *atomic.Int32) {
	go func() {
		rxCh := make(chan *pb.EventReceive, 10)
		errCh := make(chan error, 1)
		subReq := &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_EventsStore,
			ClientID:             "test-client-id" + nuid.Next(),
			Channel:              channel,
			Group:                "",
			EventsStoreTypeData:  1,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		}
		id, err := rc.a.SubscribeEventsStore(ctx, subReq, rxCh, errCh)
		if err != nil {
			errCh <- err
			panic(err)
		}
		defer func() {
			_ = rc.a.DeleteClient(id)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case rxMsg := <-rxCh:
				if rxMsg.Channel != channel {
					fmt.Printf("event store receive invalid channel: %s\n", rxMsg.Channel)
					return
				}
				if rxMsg.Metadata != requiredMeta {
					fmt.Printf("event store receive invalid metadata: %s\n", rxMsg.Metadata)
					return
				}
				if !reflect.DeepEqual(rxMsg.Body, requiredBody) {
					fmt.Printf("event store receive invalid body: %s\n", string(rxMsg.Body))
					return
				}
				messages.Inc()

			}
		}
	}()
}
func (rc *testRoutingClient) setQueueMessageSubscriber(ctx context.Context, channel string, requiredMeta string, requiredBody []byte, messages *atomic.Int32) {
	go func() {
		res, err := rc.a.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
			RequestID:           "",
			ClientID:            "test-client-id" + nuid.Next(),
			Channel:             channel,
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     4,
			IsPeak:              false,
		})
		if err != nil {
			fmt.Printf("queue message receive error: %s\n", err.Error())
			return
		}
		if !res.IsError {
			if len(res.Messages) == 1 {
				msg := res.Messages[0]
				if msg.Channel != channel {
					fmt.Printf("queue message receive invalid channel: %s\n", msg.Channel)
					return
				}
				if msg.Metadata != requiredMeta {
					fmt.Printf("queue message receive invalid metadata: %s\n", msg.Metadata)
					return
				}
				if !reflect.DeepEqual(msg.Body, requiredBody) {
					fmt.Printf("queue message receive invalid body: %s\n", string(msg.Body))
					return
				}
				messages.Inc()
			} else {
				fmt.Printf("queue message count is not 1: %d\n", len(res.Messages))
			}
		} else {
			fmt.Printf("queue message receive error: %s\n", res.Error)
		}
	}()
}
func TestRouting_SendEvents(t *testing.T) {
	defer leaktest.Check(t)()
	tests := []struct {
		name                    string
		event                   *pb.Event
		rxEventsChannels        []string
		rxEventsStoreChannels   []string
		rxQueueMessagesChannels []string
		wantErr                 bool
	}{
		{
			name: "no events channel defined",
			event: &pb.Event{
				EventID:  "router-event-id",
				ClientID: "router-event-client-id",
				Channel:  "events_store:foo.bar",
				Metadata: "router-event-metadata",
				Body:     []byte("router-event-body"),
				Store:    false,
				Tags:     nil,
			},
			rxEventsChannels:        []string{},
			rxEventsStoreChannels:   []string{},
			rxQueueMessagesChannels: []string{},
			wantErr:                 true,
		},
		{
			name: "one channel entry",
			event: &pb.Event{
				EventID:  "router-event-id",
				ClientID: "router-event-client-id",
				Channel:  "foo.bar",
				Metadata: "router-event-metadata",
				Body:     []byte("router-event-body"),
				Store:    false,
				Tags:     nil,
			},
			rxEventsChannels:        []string{"foo.bar"},
			rxEventsStoreChannels:   []string{},
			rxQueueMessagesChannels: []string{},
			wantErr:                 false,
		},
		{
			name: "two channels entry",
			event: &pb.Event{
				EventID:  "router-event-id",
				ClientID: "router-event-client-id",
				Channel:  "foo.bar;events:foo.bar.1",
				Metadata: "router-event-metadata",
				Body:     []byte("router-event-body"),
				Store:    false,
				Tags:     nil,
			},
			rxEventsChannels:        []string{"foo.bar", "foo.bar.1"},
			rxEventsStoreChannels:   []string{},
			rxQueueMessagesChannels: []string{},
			wantErr:                 false,
		},
		{
			name: "one event and one event store",
			event: &pb.Event{
				EventID:  "router-event-id",
				ClientID: "router-event-client-id",
				Channel:  "foo.bar;events_store:foo.bar.1",
				Metadata: "router-event-metadata",
				Body:     []byte("router-event-body"),
				Store:    false,
				Tags:     nil,
			},
			rxEventsChannels:        []string{"foo.bar"},
			rxEventsStoreChannels:   []string{"foo.bar.1"},
			rxQueueMessagesChannels: []string{},
			wantErr:                 false,
		},
		{
			name: "one event and one event store and one queue message ",
			event: &pb.Event{
				EventID:  "router-event-id",
				ClientID: "router-event-client-id",
				Channel:  "foo.bar;events_store:foo.bar.1;queues:foo.bar.2",
				Metadata: "router-event-metadata",
				Body:     []byte("router-event-body"),
				Store:    false,
				Tags:     nil,
			},
			rxEventsChannels:        []string{"foo.bar"},
			rxEventsStoreChannels:   []string{"foo.bar.1"},
			rxQueueMessagesChannels: []string{"foo.bar.2"},
			wantErr:                 false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			na, ns := setup(ctx, t)
			defer tearDown(na, ns)
			var err error
			na.router, err = routing.CreateRoutingService(ctx, na.appConfig)
			require.NoError(t, err)
			rc := &testRoutingClient{a: na}
			eventsCounter := atomic.NewInt32(0)
			eventsStoreCounter := atomic.NewInt32(0)
			queueMessageCounter := atomic.NewInt32(0)

			for _, channel := range tt.rxEventsChannels {
				rc.setEventSubscriber(ctx, channel, tt.event.Metadata, tt.event.Body, eventsCounter)
			}
			for _, channel := range tt.rxEventsStoreChannels {
				rc.setEventStoreSubscriber(ctx, channel, tt.event.Metadata, tt.event.Body, eventsStoreCounter)
			}
			for _, channel := range tt.rxQueueMessagesChannels {
				rc.setQueueMessageSubscriber(ctx, channel, tt.event.Metadata, tt.event.Body, queueMessageCounter)
			}
			time.Sleep(time.Second)
			result, err := na.SendEvents(ctx, tt.event)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NotNil(t, result)
			}
			<-time.After(2 * time.Second)
			assert.Equal(t, len(tt.rxEventsChannels), int(eventsCounter.Load()))
			assert.Equal(t, len(tt.rxEventsStoreChannels), int(eventsStoreCounter.Load()))
			assert.Equal(t, len(tt.rxQueueMessagesChannels), int(queueMessageCounter.Load()))

		})
	}
}
func TestRouting_SendStoreEvents(t *testing.T) {
	defer leaktest.Check(t)()
	tests := []struct {
		name                    string
		event                   *pb.Event
		rxEventsChannels        []string
		rxEventsStoreChannels   []string
		rxQueueMessagesChannels []string
		wantErr                 bool
	}{
		{
			name: "no events store channel defined",
			event: &pb.Event{
				EventID:  "router-event-store-id",
				ClientID: "router-event-store-client-id",
				Channel:  "events:foo.bar",
				Metadata: "router-event-store-metadata",
				Body:     []byte("router-event-store-body"),
				Store:    true,
				Tags:     nil,
			},
			rxEventsChannels:        []string{},
			rxEventsStoreChannels:   []string{},
			rxQueueMessagesChannels: []string{},
			wantErr:                 true,
		},
		{
			name: "one channel entry",
			event: &pb.Event{
				EventID:  "router-event-store-id",
				ClientID: "router-event-store-client-id",
				Channel:  "foo.bar",
				Metadata: "router-event-store-metadata",
				Body:     []byte("router-event-store-body"),
				Store:    true,
				Tags:     nil,
			},
			rxEventsChannels:        []string{},
			rxEventsStoreChannels:   []string{"foo.bar"},
			rxQueueMessagesChannels: []string{},
			wantErr:                 false,
		},
		{
			name: "two channels entry",
			event: &pb.Event{
				EventID:  "router-event-store-id",
				ClientID: "router-event-store-client-id",
				Channel:  "foo.bar;events_store:foo.bar.1",
				Metadata: "router-event-store-metadata",
				Body:     []byte("router-event-store-body"),
				Store:    true,
				Tags:     nil,
			},
			rxEventsChannels:        []string{},
			rxEventsStoreChannels:   []string{"foo.bar", "foo.bar.1"},
			rxQueueMessagesChannels: []string{},
			wantErr:                 false,
		},
		{
			name: "one event and one event store",
			event: &pb.Event{
				EventID:  "router-event-store-id",
				ClientID: "router-event-store-client-id",
				Channel:  "foo.bar.1;events:foo.bar",
				Metadata: "router-event-store-metadata",
				Body:     []byte("router-event-store-body"),
				Store:    true,
				Tags:     nil,
			},
			rxEventsChannels:        []string{"foo.bar"},
			rxEventsStoreChannels:   []string{"foo.bar.1"},
			rxQueueMessagesChannels: []string{},
			wantErr:                 false,
		},
		{
			name: "one event and one event store and one queue message ",
			event: &pb.Event{
				EventID:  "router-event-store-id",
				ClientID: "router-event-store-client-id",
				Channel:  "foo.bar;events:foo.bar.1;queues:foo.bar.2",
				Metadata: "router-event-store-metadata",
				Body:     []byte("router-event-store-body"),
				Store:    true,
				Tags:     nil,
			},
			rxEventsChannels:        []string{"foo.bar.1"},
			rxEventsStoreChannels:   []string{"foo.bar"},
			rxQueueMessagesChannels: []string{"foo.bar.2"},
			wantErr:                 false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			na, ns := setup(ctx, t)
			defer tearDown(na, ns)
			na.router, _ = routing.CreateRoutingService(ctx, na.appConfig)
			rc := &testRoutingClient{a: na}
			eventsCounter := atomic.NewInt32(0)
			eventsStoreCounter := atomic.NewInt32(0)
			queueMessageCounter := atomic.NewInt32(0)

			for _, channel := range tt.rxEventsChannels {
				rc.setEventSubscriber(ctx, channel, tt.event.Metadata, tt.event.Body, eventsCounter)
			}
			for _, channel := range tt.rxEventsStoreChannels {
				rc.setEventStoreSubscriber(ctx, channel, tt.event.Metadata, tt.event.Body, eventsStoreCounter)
			}
			for _, channel := range tt.rxQueueMessagesChannels {
				rc.setQueueMessageSubscriber(ctx, channel, tt.event.Metadata, tt.event.Body, queueMessageCounter)
			}
			time.Sleep(time.Second)
			result, err := na.SendEventsStore(ctx, tt.event)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NotNil(t, result)
			}
			<-time.After(2 * time.Second)
			assert.Equal(t, len(tt.rxEventsChannels), int(eventsCounter.Load()))
			assert.Equal(t, len(tt.rxEventsStoreChannels), int(eventsStoreCounter.Load()))
			assert.Equal(t, len(tt.rxQueueMessagesChannels), int(queueMessageCounter.Load()))

		})
	}
}
func TestRouting_SendQueueMessages(t *testing.T) {
	defer leaktest.Check(t)()
	tests := []struct {
		name                    string
		queueMessage            *pb.QueueMessage
		rxEventsChannels        []string
		rxEventsStoreChannels   []string
		rxQueueMessagesChannels []string
		wantErr                 bool
	}{
		{
			name: "no queue message channel defined",
			queueMessage: &pb.QueueMessage{
				MessageID:  "router-queue-message-message-id",
				ClientID:   "router-queue-message-client-id",
				Channel:    "events:foo.bar",
				Metadata:   "router-queue-message-metadata",
				Body:       []byte("router-queue-message-body"),
				Tags:       nil,
				Attributes: nil,
				Policy:     nil,
			},
			rxEventsChannels:        []string{},
			rxEventsStoreChannels:   []string{},
			rxQueueMessagesChannels: []string{},
			wantErr:                 true,
		},
		{
			name: "one queue message channel defined",
			queueMessage: &pb.QueueMessage{
				MessageID:  "router-queue-message-message-id",
				ClientID:   "router-queue-message-client-id",
				Channel:    "foo.bar",
				Metadata:   "router-queue-message-metadata",
				Body:       []byte("router-queue-message-body"),
				Tags:       nil,
				Attributes: nil,
				Policy:     nil,
			},
			rxEventsChannels:        []string{},
			rxEventsStoreChannels:   []string{},
			rxQueueMessagesChannels: []string{"foo.bar"},
			wantErr:                 false,
		},
		{
			name: "two queue messages channel defined",
			queueMessage: &pb.QueueMessage{
				MessageID:  "router-queue-message-message-id",
				ClientID:   "router-queue-message-client-id",
				Channel:    "foo.bar;queues:foo.bar.1",
				Metadata:   "router-queue-message-metadata",
				Body:       []byte("router-queue-message-body"),
				Tags:       nil,
				Attributes: nil,
				Policy:     nil,
			},
			rxEventsChannels:        []string{},
			rxEventsStoreChannels:   []string{},
			rxQueueMessagesChannels: []string{"foo.bar", "foo.bar.1"},
			wantErr:                 false,
		},
		{
			name: "one queue message and one event message",
			queueMessage: &pb.QueueMessage{
				MessageID:  "router-queue-message-message-id",
				ClientID:   "router-queue-message-client-id",
				Channel:    "foo.bar;events:foo.bar.1",
				Metadata:   "router-queue-message-metadata",
				Body:       []byte("router-queue-message-body"),
				Tags:       nil,
				Attributes: nil,
				Policy:     nil,
			},
			rxEventsChannels:        []string{"foo.bar.1"},
			rxEventsStoreChannels:   []string{},
			rxQueueMessagesChannels: []string{"foo.bar"},
			wantErr:                 false,
		},
		{
			name: "one queue message  one event message and one events store message",
			queueMessage: &pb.QueueMessage{
				MessageID:  "router-queue-message-message-id",
				ClientID:   "router-queue-message-client-id",
				Channel:    "foo.bar;events:foo.bar.1;events_store:foo.bar.2",
				Metadata:   "router-queue-message-metadata",
				Body:       []byte("router-queue-message-body"),
				Tags:       nil,
				Attributes: nil,
				Policy:     nil,
			},
			rxEventsChannels:        []string{"foo.bar.1"},
			rxEventsStoreChannels:   []string{"foo.bar.2"},
			rxQueueMessagesChannels: []string{"foo.bar"},
			wantErr:                 false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			na, ns := setup(ctx, t)
			defer tearDown(na, ns)
			na.router, _ = routing.CreateRoutingService(ctx, na.appConfig)
			rc := &testRoutingClient{a: na}
			eventsCounter := atomic.NewInt32(0)
			eventsStoreCounter := atomic.NewInt32(0)
			queueMessageCounter := atomic.NewInt32(0)

			for _, channel := range tt.rxEventsChannels {
				rc.setEventSubscriber(ctx, channel, tt.queueMessage.Metadata, tt.queueMessage.Body, eventsCounter)
			}
			for _, channel := range tt.rxEventsStoreChannels {
				rc.setEventStoreSubscriber(ctx, channel, tt.queueMessage.Metadata, tt.queueMessage.Body, eventsStoreCounter)
			}
			for _, channel := range tt.rxQueueMessagesChannels {
				rc.setQueueMessageSubscriber(ctx, channel, tt.queueMessage.Metadata, tt.queueMessage.Body, queueMessageCounter)
			}
			time.Sleep(time.Second)
			result, err := na.SendQueueMessage(ctx, tt.queueMessage)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NotNil(t, result)
			}
			<-time.After(2 * time.Second)
			assert.Equal(t, len(tt.rxEventsChannels), int(eventsCounter.Load()))
			assert.Equal(t, len(tt.rxEventsStoreChannels), int(eventsStoreCounter.Load()))
			assert.Equal(t, len(tt.rxQueueMessagesChannels), int(queueMessageCounter.Load()))

		})
	}
}
func TestRouting_SendBatchQueueMessages(t *testing.T) {
	defer leaktest.Check(t)()
	tests := []struct {
		name                    string
		queueMessages           []*pb.QueueMessage
		rxEventsChannels        []string
		rxEventsStoreChannels   []string
		rxQueueMessagesChannels []string
		wantErr                 bool
	}{
		{
			name: "multiple queue message",
			queueMessages: []*pb.QueueMessage{
				&pb.QueueMessage{
					MessageID:  "router-queue-message-message-id",
					ClientID:   "router-queue-message-client-id",
					Channel:    "foo.bar",
					Metadata:   "router-queue-message-metadata",
					Body:       []byte("router-queue-message-body"),
					Tags:       nil,
					Attributes: nil,
					Policy:     nil,
				},
				&pb.QueueMessage{
					MessageID:  "router-queue-message-message-id",
					ClientID:   "router-queue-message-client-id",
					Channel:    "foo.bar.1;events_store:foo.bar.1",
					Metadata:   "router-queue-message-metadata",
					Body:       []byte("router-queue-message-body"),
					Tags:       nil,
					Attributes: nil,
					Policy:     nil,
				},
				&pb.QueueMessage{
					MessageID:  "router-queue-message-message-id",
					ClientID:   "router-queue-message-client-id",
					Channel:    "foo.bar.2;events:foo.bar.1",
					Metadata:   "router-queue-message-metadata",
					Body:       []byte("router-queue-message-body"),
					Tags:       nil,
					Attributes: nil,
					Policy:     nil,
				},
			},
			rxEventsChannels:        []string{"foo.bar.1"},
			rxEventsStoreChannels:   []string{"foo.bar.1"},
			rxQueueMessagesChannels: []string{"foo.bar", "foo.bar.1", "foo.bar.2"},
			wantErr:                 false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			na, ns := setup(ctx, t)

			na.router, _ = routing.CreateRoutingService(ctx, na.appConfig)
			defer tearDown(na, ns)
			rc := &testRoutingClient{a: na}
			eventsCounter := atomic.NewInt32(0)
			eventsStoreCounter := atomic.NewInt32(0)
			queueMessageCounter := atomic.NewInt32(0)
			body := []byte("router-queue-message-body")
			metadata := "router-queue-message-metadata"
			for _, channel := range tt.rxEventsChannels {
				rc.setEventSubscriber(ctx, channel, metadata, body, eventsCounter)
			}
			for _, channel := range tt.rxEventsStoreChannels {
				rc.setEventStoreSubscriber(ctx, channel, metadata, body, eventsStoreCounter)
			}
			for _, channel := range tt.rxQueueMessagesChannels {
				rc.setQueueMessageSubscriber(ctx, channel, metadata, body, queueMessageCounter)
			}
			time.Sleep(time.Second)

			result, err := na.SendQueueMessagesBatch(ctx, &pb.QueueMessagesBatchRequest{
				BatchID:  "some-batch",
				Messages: tt.queueMessages,
			})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NotNil(t, result)
			}
			<-time.After(2 * time.Second)
			assert.Equal(t, len(tt.rxEventsChannels), int(eventsCounter.Load()))
			assert.Equal(t, len(tt.rxEventsStoreChannels), int(eventsStoreCounter.Load()))
			assert.Equal(t, len(tt.rxQueueMessagesChannels), int(queueMessageCounter.Load()))

		})
	}
}
