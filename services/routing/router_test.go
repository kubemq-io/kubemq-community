package routing

import (
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

var baseEvent = &pb.Event{
	EventID:  "router-event-id",
	ClientID: "router-event-client-id",
	Channel:  "",
	Metadata: "router-event-metadata",
	Body:     []byte("router-event-body"),
	Store:    false,
	Tags:     nil,
}

var copiedEventFromEvent = &pb.Event{
	EventID:  "router-event-id",
	ClientID: "router-event-client-id",
	Channel:  "",
	Metadata: "router-event-metadata",
	Body:     []byte("router-event-body"),
	Store:    false,
	Tags:     map[string]string{"X-KUBEMQ-ROUTED": "true"},
}

var copiedEventFromEventStore = &pb.Event{
	EventID:  "router-event-store-id",
	ClientID: "router-event-store-client-id",
	Channel:  "",
	Metadata: "router-event-store-metadata",
	Body:     []byte("router-event-store-body"),
	Store:    false,
	Tags:     map[string]string{"key1": "value1", "X-KUBEMQ-ROUTED": "true"},
}
var copiedEventFromQueueMessage = &pb.Event{
	EventID:  "router-queue-message-id",
	ClientID: "router-queue-message-client-id",
	Channel:  "",
	Metadata: "router-queue-message-metadata",
	Body:     []byte("router-queue-message-body"),
	Store:    false,
	Tags:     map[string]string{"key1": "value1", "X-KUBEMQ-ROUTED": "true"},
}

var baseEventStore = &pb.Event{
	EventID:  "router-event-store-id",
	ClientID: "router-event-store-client-id",
	Channel:  "",
	Metadata: "router-event-store-metadata",
	Body:     []byte("router-event-store-body"),
	Store:    true,
	Tags:     map[string]string{"key1": "value1"},
}
var copiedEventStoreFromEvent = &pb.Event{
	EventID:  "router-event-id",
	ClientID: "router-event-client-id",
	Channel:  "",
	Metadata: "router-event-metadata",
	Body:     []byte("router-event-body"),
	Store:    true,
	Tags:     map[string]string{"X-KUBEMQ-ROUTED": "true"},
}
var copiedEventStoreFromEventStore = &pb.Event{
	EventID:  "router-event-store-id",
	ClientID: "router-event-store-client-id",
	Channel:  "",
	Metadata: "router-event-store-metadata",
	Body:     []byte("router-event-store-body"),
	Store:    true,
	Tags:     map[string]string{"key1": "value1", "X-KUBEMQ-ROUTED": "true"},
}

var copiedEventStoreFromQueueMessage = &pb.Event{
	EventID:  "router-queue-message-id",
	ClientID: "router-queue-message-client-id",
	Channel:  "",
	Metadata: "router-queue-message-metadata",
	Body:     []byte("router-queue-message-body"),
	Store:    true,
	Tags:     map[string]string{"key1": "value1", "X-KUBEMQ-ROUTED": "true"},
}

var baseQueueMessage = &pb.QueueMessage{
	MessageID:  "router-queue-message-id",
	ClientID:   "router-queue-message-client-id",
	Channel:    "",
	Metadata:   "router-queue-message-metadata",
	Body:       []byte("router-queue-message-body"),
	Tags:       map[string]string{"key1": "value1"},
	Attributes: nil,
	Policy: &pb.QueueMessagePolicy{
		ExpirationSeconds: 1,
		DelaySeconds:      2,
		MaxReceiveCount:   3,
		MaxReceiveQueue:   "max-receive-queue",
	},
}

var copiedQueueMessageFromEvent = &pb.QueueMessage{
	MessageID:  "router-event-id",
	ClientID:   "router-event-client-id",
	Channel:    "",
	Metadata:   "router-event-metadata",
	Body:       []byte("router-event-body"),
	Tags:       map[string]string{"X-KUBEMQ-ROUTED": "true"},
	Attributes: nil,
	Policy:     nil,
}

var copiedQueueMessageFromEventStore = &pb.QueueMessage{
	MessageID:  "router-event-store-id",
	ClientID:   "router-event-store-client-id",
	Channel:    "",
	Metadata:   "router-event-store-metadata",
	Body:       []byte("router-event-store-body"),
	Tags:       map[string]string{"key1": "value1", "X-KUBEMQ-ROUTED": "true"},
	Attributes: nil,
	Policy:     nil,
}

var copiedQueueMessageFromQueueMessage = &pb.QueueMessage{
	MessageID:  "router-queue-message-id",
	ClientID:   "router-queue-message-client-id",
	Channel:    "",
	Metadata:   "router-queue-message-metadata",
	Body:       []byte("router-queue-message-body"),
	Tags:       map[string]string{"key1": "value1", "X-KUBEMQ-ROUTED": "true"},
	Attributes: nil,
	Policy: &pb.QueueMessagePolicy{
		ExpirationSeconds: 1,
		DelaySeconds:      2,
		MaxReceiveCount:   3,
		MaxReceiveQueue:   "max-receive-queue",
	},
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

type testRouterMessage struct {
	*RouteMessages
}

func (rm *testRouterMessage) testEventsRouteMessagesResult(t *testing.T) {
	var eventsRoutes []string

	eventsRoutes = append(eventsRoutes, rm.Route.RoutesMap[""]...)
	eventsRoutes = append(eventsRoutes, rm.Route.RoutesMap["events"]...)
	eventsStoreroutes := rm.Route.RoutesMap["events_store"]
	queueMessagesroutes := rm.Route.RoutesMap["queues"]

	require.Equal(t, len(eventsRoutes), len(rm.Events))

	for i := 0; i < len(eventsRoutes); i++ {
		copiedEventFromEvent.Channel = eventsRoutes[i]
		copiedEvent := rm.Events[i]
		require.EqualValues(t, copiedEventFromEvent, copiedEvent)
		copiedEventFromEvent.Channel = ""
	}
	require.Equal(t, len(eventsStoreroutes), len(rm.EventsStore))
	for i := 0; i < len(eventsStoreroutes); i++ {
		copiedEventStoreFromEvent.Channel = eventsStoreroutes[i]
		copiedEventStore := rm.EventsStore[i]
		require.EqualValues(t, copiedEventStoreFromEvent, copiedEventStore)
		copiedEventStoreFromEvent.Channel = ""
	}
	require.Equal(t, len(queueMessagesroutes), len(rm.QueueMessages))
	for i := 0; i < len(queueMessagesroutes); i++ {
		copiedQueueMessageFromEvent.Channel = queueMessagesroutes[i]
		copiedQueueMessage := rm.QueueMessages[i]
		require.EqualValues(t, copiedQueueMessageFromEvent, copiedQueueMessage)
		copiedQueueMessageFromEvent.Channel = ""
	}
}

func (rm *testRouterMessage) testEventsStoreRouteMessagesResult(t *testing.T) {
	var eventsStoreRoutes []string

	eventsStoreRoutes = append(eventsStoreRoutes, rm.Route.RoutesMap[""]...)
	eventsStoreRoutes = append(eventsStoreRoutes, rm.Route.RoutesMap["events_store"]...)
	eventsRoutes := rm.Route.RoutesMap["events"]
	queueMessagesRoutes := rm.Route.RoutesMap["queues"]
	require.Equal(t, len(eventsStoreRoutes), len(rm.EventsStore))

	for i := 0; i < len(eventsStoreRoutes); i++ {
		copiedEventStoreFromEventStore.Channel = eventsStoreRoutes[i]
		copiedEventStore := rm.EventsStore[i]
		require.EqualValues(t, copiedEventStoreFromEventStore, copiedEventStore)
		copiedEventStoreFromEventStore.Channel = ""
	}
	require.Equal(t, len(eventsRoutes), len(rm.Events))
	for i := 0; i < len(eventsRoutes); i++ {
		copiedEventFromEventStore.Channel = eventsRoutes[i]
		copiedEvent := rm.Events[i]
		require.EqualValues(t, copiedEventFromEventStore, copiedEvent)
		copiedEventFromEventStore.Channel = ""
	}
	require.Equal(t, len(queueMessagesRoutes), len(rm.QueueMessages))
	for i := 0; i < len(queueMessagesRoutes); i++ {
		copiedQueueMessageFromEventStore.Channel = queueMessagesRoutes[i]
		copiedQueueMessage := rm.QueueMessages[i]
		require.EqualValues(t, copiedQueueMessageFromEventStore, copiedQueueMessage)
		copiedQueueMessageFromEventStore.Channel = ""
	}
}
func (rm *testRouterMessage) testQueueMessagesRouteMessagesResult(t *testing.T) {
	var queueMessagesRoutes []string

	queueMessagesRoutes = append(queueMessagesRoutes, rm.Route.RoutesMap[""]...)
	queueMessagesRoutes = append(queueMessagesRoutes, rm.Route.RoutesMap["queues"]...)
	eventsRoutes := rm.Route.RoutesMap["events"]
	eventsStoreRoutes := rm.Route.RoutesMap["events_store"]
	require.Equal(t, len(queueMessagesRoutes), len(rm.QueueMessages))

	for i := 0; i < len(queueMessagesRoutes); i++ {
		copiedQueueMessageFromQueueMessage.Channel = queueMessagesRoutes[i]
		copiedQueueMessage := rm.QueueMessages[i]
		require.EqualValues(t, copiedQueueMessageFromQueueMessage, copiedQueueMessage)
		copiedQueueMessageFromQueueMessage.Channel = ""
	}
	require.Equal(t, len(eventsRoutes), len(rm.Events))
	for i := 0; i < len(eventsRoutes); i++ {
		copiedEventFromQueueMessage.Channel = eventsRoutes[i]
		copiedEvent := rm.Events[i]
		require.EqualValues(t, copiedEventFromQueueMessage, copiedEvent)
		copiedEventFromQueueMessage.Channel = ""
	}
	require.Equal(t, len(eventsStoreRoutes), len(rm.EventsStore))
	for i := 0; i < len(eventsStoreRoutes); i++ {
		copiedEventStoreFromQueueMessage.Channel = eventsStoreRoutes[i]
		copiedEventStore := rm.EventsStore[i]
		require.EqualValues(t, copiedEventStoreFromQueueMessage, copiedEventStore)
		copiedEventStoreFromQueueMessage.Channel = ""
	}
}
