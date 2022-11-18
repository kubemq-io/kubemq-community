package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/api/actions"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	sdk "github.com/kubemq-io/kubemq-go"
	pb "github.com/kubemq-io/protobuf/go"
	"strconv"
	"time"
)

func createPubSubChannelWithInternalClient(ctx context.Context, client *InternalClient, name string, isStore bool) error {
	newCtx, _ := context.WithTimeout(ctx, 1*time.Second)
	errChan := make(chan error, 1)
	var err error
	subId := ""
	var pattern string
	if isStore {
		msgCh := make(chan *pb.EventReceive, 1)
		subId, err = client.arrayService.SubscribeEventsStore(newCtx, &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_EventsStore,
			ClientID:             client.clientID,
			Channel:              name,
			Group:                "",
			EventsStoreTypeData:  pb.Subscribe_StartNewOnly,
			EventsStoreTypeValue: 0,
		}, msgCh, errChan)
		if err != nil {
			return err
		}
		pattern = "events_store"
	} else {
		msgCh := make(chan *pb.EventReceive, 1)
		subId, err = client.arrayService.SubscribeEvents(newCtx, &pb.Subscribe{
			SubscribeTypeData: pb.Subscribe_Events,
			ClientID:          client.clientID,
			Channel:           name,
		}, msgCh, errChan)
		if err != nil {
			return err
		}
		pattern = "events"
	}
	metrics.ReportClient(pattern, "receive", name, 1)
	time.Sleep(1 * time.Second)
	err = client.arrayService.DeleteClient(subId)
	if err != nil {
		return err
	}
	metrics.ReportClient(pattern, "receive", name, -1)
	return nil
}

func sendPubSubMessageWithInternalClient(ctx context.Context, client *InternalClient, message *actions.SendPubSubMessageRequest) error {
	body, _, err := detectAndConvertToBytesArray(message.Body)
	if err != nil {
		return err
	}
	if message.IsEvents {
		event := &pb.Event{
			EventID:  message.MessageId,
			ClientID: client.clientID,
			Channel:  message.Channel,
			Metadata: message.Metadata,
			Body:     body,
			Store:    false,
			Tags:     message.TagsKeyValue(),
		}
		res, err := client.arrayService.SendEvents(ctx, event)
		if err != nil {
			return err
		}
		if res.Error != "" {
			return fmt.Errorf("error sending pubsub message on channel: %s , error: %s", message.Channel, res.Error)
		}

	} else {
		event := &pb.Event{
			EventID:  message.MessageId,
			ClientID: client.clientID,
			Channel:  message.Channel,
			Metadata: message.Metadata,
			Body:     body,
			Store:    true,
			Tags:     message.TagsKeyValue(),
		}
		res, err := client.arrayService.SendEventsStore(ctx, event)
		if err != nil {
			return err
		}
		if res.Error != "" {
			return fmt.Errorf("error sending pubsub message on channel: %s , error: %s", message.Channel, res.Error)
		}
	}
	return nil

}
func subscribeToEventsWithInternalClient(ctx context.Context, client *InternalClient, channel, group string, messagesCh chan *actions.SubscribePubSubMessage, errChan chan error) error {

	eventsCh := make(chan *pb.EventReceive, 1)
	subReq := &pb.Subscribe{
		SubscribeTypeData: pb.Subscribe_Events,
		ClientID:          client.clientID,
		Channel:           channel,
		Group:             group,
	}
	subId, err := client.arrayService.SubscribeEvents(ctx, subReq, eventsCh, errChan)
	if err != nil {
		return err
	}
	metrics.ReportClient("events", "receive", channel, 1)
	go func() {
		defer func() {
			_ = client.arrayService.DeleteClient(subId)
			metrics.ReportClient("events", "receive", channel, -1)
		}()
		for {
			select {
			case event, ok := <-eventsCh:
				if !ok {
					return
				}
				msg := actions.NewSubscribePubSubMessage()
				if len(event.Tags) > 0 {
					data, _ := json.Marshal(event.Tags)
					msg.SetTags(string(data))
				}
				msg.SetBody(detectAndConvertToAny(event.Body)).
					SetMetadata(event.Metadata).
					SetTimestamp(time.Now()).
					SetMessageId(event.EventID)
				messagesCh <- msg
				metrics.ReportEventReceive(event, subReq)
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}
func subscribeToEvents(ctx context.Context, client *sdk.Client, channel, group string, messagesCh chan *actions.SubscribePubSubMessage, errChan chan error) error {
	eventsChan, err := client.SubscribeToEvents(ctx, channel, group, errChan)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case event, ok := <-eventsChan:
				if !ok {
					return
				}
				msg := actions.NewSubscribePubSubMessage()
				if len(event.Tags) > 0 {
					data, _ := json.Marshal(event.Tags)
					msg.SetTags(string(data))
				}
				msg.SetBody(detectAndConvertToAny(event.Body)).
					SetMetadata(event.Metadata).
					SetTimestamp(time.Now()).
					SetMessageId(event.Id)
				messagesCh <- msg
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func subscribeToEventsStoreWithInternalClient(ctx context.Context, client *InternalClient, channel, group, clientId, subType, subValue string, messagesCh chan *actions.SubscribePubSubMessage, errChan chan error) error {
	var subOption pb.Subscribe_EventsStoreType
	var subOptionIntValue int64

	switch subType {
	case "1":
		subOption = pb.Subscribe_StartNewOnly
	case "2":
		subOption = pb.Subscribe_StartFromFirst
	case "3":
		subOption = pb.Subscribe_StartFromLast
	case "4":
		seq, err := strconv.ParseInt(subValue, 10, 64)
		if err != nil {
			return fmt.Errorf("error parsing sequence value, error: %s", err.Error())
		}
		subOption = pb.Subscribe_StartAtSequence
		subOptionIntValue = seq
	case "5":
		timeIso, err := time.Parse(time.RFC3339, subValue)
		if err != nil {
			return fmt.Errorf("error parsing time value, error: %s", err.Error())
		}
		subOption = pb.Subscribe_StartAtTime
		subOptionIntValue = timeIso.UnixNano()
	case "6":
		durationInt, err := strconv.ParseInt(subValue, 10, 64)
		if err != nil {
			return fmt.Errorf("error parsing duration value, error: %s", err.Error())
		}
		subOption = pb.Subscribe_StartAtTimeDelta
		subOptionIntValue = durationInt
	default:
		return fmt.Errorf("invalid subscription type, valid values are 1,2,3,4,5,6")
	}
	eventsCh := make(chan *pb.EventReceive, 1)
	subReq := &pb.Subscribe{
		SubscribeTypeData:    pb.Subscribe_EventsStore,
		ClientID:             clientId,
		Channel:              channel,
		Group:                group,
		EventsStoreTypeData:  subOption,
		EventsStoreTypeValue: subOptionIntValue,
	}
	subId, err := client.arrayService.SubscribeEventsStore(ctx, subReq, eventsCh, errChan)
	if err != nil {
		return err
	}
	metrics.ReportClient("events_store", "receive", channel, 1)
	go func() {
		defer func() {
			_ = client.arrayService.DeleteClient(subId)
			metrics.ReportClient("events_store", "receive", channel, -1)
		}()
		for {
			select {
			case event, ok := <-eventsCh:
				if !ok {
					return
				}
				msg := actions.NewSubscribePubSubMessage()
				if len(event.Tags) > 0 {
					data, _ := json.Marshal(event.Tags)
					msg.SetTags(string(data))
				}
				msg.SetBody(detectAndConvertToAny(event.Body)).
					SetMetadata(event.Metadata).
					SetMessageId(event.EventID).
					SetTimestamp(time.Unix(0, event.Timestamp)).
					SetSequence(int64(event.Sequence))
				messagesCh <- msg
				metrics.ReportEventReceive(event, subReq)
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}
