package routing

import (
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/routing"
	pb "github.com/kubemq-io/protobuf/go"
)

type RouteMessages struct {
	Route         *routing.Route
	Events        []*pb.Event
	EventsStore   []*pb.Event
	QueueMessages []*pb.QueueMessage
}

func (rm *RouteMessages) processEventsRouting(event *pb.Event) (*RouteMessages, error) {
	for _, channel := range rm.Route.RoutesMap[""] {
		newEvent := copyEvent(event, false)
		newEvent.Channel = channel
		rm.Events = append(rm.Events, newEvent)
	}
	for _, channel := range rm.Route.RoutesMap["events"] {
		newEvent := copyEvent(event, false)
		newEvent.Channel = channel
		rm.Events = append(rm.Events, newEvent)
	}

	if len(rm.Events) == 0 {
		return nil, fmt.Errorf("no valid events channel destination was found in routes")
	}
	for _, channel := range rm.Route.RoutesMap["events_store"] {
		newEventStore := copyEvent(event, true)
		newEventStore.Channel = channel
		rm.EventsStore = append(rm.EventsStore, newEventStore)
	}
	for _, channel := range rm.Route.RoutesMap["queues"] {
		newQueueMessage := copyEventToQueueMessage(event)
		newQueueMessage.Channel = channel
		rm.QueueMessages = append(rm.QueueMessages, newQueueMessage)
	}
	return rm, nil
}

func (rm *RouteMessages) processEventsStoreRouting(event *pb.Event) (*RouteMessages, error) {
	for _, channel := range rm.Route.RoutesMap[""] {
		newEventStore := copyEvent(event, true)
		newEventStore.Channel = channel
		rm.EventsStore = append(rm.EventsStore, newEventStore)
	}
	for _, channel := range rm.Route.RoutesMap["events_store"] {
		newEventStore := copyEvent(event, true)
		newEventStore.Channel = channel
		rm.EventsStore = append(rm.EventsStore, newEventStore)
	}
	if len(rm.EventsStore) == 0 {
		return nil, fmt.Errorf("no valid events_store channel destination was found in routes")
	}
	for _, channel := range rm.Route.RoutesMap["events"] {
		newEvent := copyEvent(event, false)
		newEvent.Channel = channel
		rm.Events = append(rm.Events, newEvent)
	}
	for _, channel := range rm.Route.RoutesMap["queues"] {
		newQueueMessage := copyEventToQueueMessage(event)
		newQueueMessage.Channel = channel
		rm.QueueMessages = append(rm.QueueMessages, newQueueMessage)
	}
	return rm, nil
}
func (rm *RouteMessages) processQueueMessageRouting(msg *pb.QueueMessage) (*RouteMessages, error) {
	for _, channel := range rm.Route.RoutesMap[""] {
		newQueueMessage := copyQueueMessageToQueueMessage(msg)
		newQueueMessage.Channel = channel
		rm.QueueMessages = append(rm.QueueMessages, newQueueMessage)
	}
	for _, channel := range rm.Route.RoutesMap["queues"] {
		newQueueMessage := copyQueueMessageToQueueMessage(msg)
		newQueueMessage.Channel = channel
		rm.QueueMessages = append(rm.QueueMessages, newQueueMessage)
	}
	if len(rm.QueueMessages) == 0 {
		return nil, fmt.Errorf("no valid queue message channel destination was found in routes")
	}

	for _, channel := range rm.Route.RoutesMap["events_store"] {
		newEventStore := copyQueueMessageToEvent(msg, true)
		newEventStore.Channel = channel
		rm.EventsStore = append(rm.EventsStore, newEventStore)
	}
	for _, channel := range rm.Route.RoutesMap["events"] {
		newEvent := copyQueueMessageToEvent(msg, false)
		newEvent.Channel = channel
		rm.Events = append(rm.Events, newEvent)
	}
	return rm, nil
}

func copyBytesBuffer(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
func copyTagsMap(src map[string]string) map[string]string {
	dst := make(map[string]string)
	for key, value := range src {
		dst[key] = value
	}
	dst["X-KUBEMQ-ROUTED"] = "true"
	return dst
}

func copyEvent(event *pb.Event, isStore bool) *pb.Event {
	return &pb.Event{
		EventID:  event.EventID,
		ClientID: event.ClientID,
		Channel:  event.Channel,
		Metadata: event.Metadata,
		Body:     copyBytesBuffer(event.Body),
		Store:    isStore,
		Tags:     copyTagsMap(event.Tags),
	}
}

func copyPolicy(src *pb.QueueMessagePolicy) *pb.QueueMessagePolicy {
	if src == nil {
		return nil
	}
	return &pb.QueueMessagePolicy{
		ExpirationSeconds: src.ExpirationSeconds,
		DelaySeconds:      src.DelaySeconds,
		MaxReceiveCount:   src.MaxReceiveCount,
		MaxReceiveQueue:   src.MaxReceiveQueue,
	}

}
func copyEventToQueueMessage(event *pb.Event) *pb.QueueMessage {
	return &pb.QueueMessage{
		MessageID:  event.EventID,
		ClientID:   event.ClientID,
		Channel:    event.Channel,
		Metadata:   event.Metadata,
		Body:       copyBytesBuffer(event.Body),
		Tags:       copyTagsMap(event.Tags),
		Attributes: nil,
		Policy:     nil,
	}
}

func copyQueueMessageToEvent(msg *pb.QueueMessage, isStore bool) *pb.Event {
	return &pb.Event{
		EventID:  msg.MessageID,
		ClientID: msg.ClientID,
		Channel:  msg.Channel,
		Metadata: msg.Metadata,
		Body:     copyBytesBuffer(msg.Body),
		Store:    isStore,
		Tags:     copyTagsMap(msg.Tags),
	}
}

func copyQueueMessageToQueueMessage(msg *pb.QueueMessage) *pb.QueueMessage {
	return &pb.QueueMessage{
		MessageID:  msg.MessageID,
		ClientID:   msg.ClientID,
		Channel:    msg.Channel,
		Metadata:   msg.Metadata,
		Body:       copyBytesBuffer(msg.Body),
		Tags:       copyTagsMap(msg.Tags),
		Attributes: nil,
		Policy:     copyPolicy(msg.Policy),
	}
}
