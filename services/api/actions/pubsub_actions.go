package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/api/actions"
	sdk "github.com/kubemq-io/kubemq-go"
	"strconv"
	"time"
)

func createPubSubChannel(ctx context.Context, client *sdk.Client, name string, isStore bool) error {
	newCtx, _ := context.WithTimeout(ctx, 1*time.Second)
	errChan := make(chan error, 1)
	if isStore {
		_, err := client.SubscribeToEventsStore(newCtx, name, "", errChan, sdk.StartFromNewEvents())
		if err != nil {
			return err
		}

	} else {
		_, err := client.SubscribeToEvents(newCtx, name, "", errChan)
		if err != nil {
			return err
		}
	}
	time.Sleep(1 * time.Second)
	return nil
}

func sendPubSubMessage(ctx context.Context, client *sdk.Client, message *actions.SendPubSubMessageRequest) error {

	body, _, err := detectAndConvertToBytesArray(message.Body)
	if err != nil {
		return err
	}

	if message.IsEvents {
		err := client.E().
			SetChannel(message.Channel).
			SetBody(body).
			SetId(message.MessageId).
			SetMetadata(message.Metadata).
			SetTags(message.TagsKeyValue()).
			Send(ctx)
		if err != nil {
			return err
		}
	} else {
		res, err := client.ES().
			SetChannel(message.Channel).
			SetBody(body).
			SetId(message.MessageId).
			SetMetadata(message.Metadata).
			SetTags(message.TagsKeyValue()).
			Send(ctx)
		if err != nil {
			return err
		}
		if res.Err != nil {
			return fmt.Errorf("error sending pubsub message on channel: %s , error: %s", message.Channel, res.Err.Error())
		}

	}
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
func subscribeToEventsStore(ctx context.Context, client *sdk.Client, channel, group, clientId, subType, subValue string, messagesCh chan *actions.SubscribePubSubMessage, errChan chan error) error {
	var subOption sdk.SubscriptionOption
	switch subType {
	case "1":
		subOption = sdk.StartFromNewEvents()
	case "2":
		subOption = sdk.StartFromFirstEvent()
	case "3":
		subOption = sdk.StartFromLastEvent()
	case "4":
		seq, err := strconv.ParseInt(subValue, 10, 64)
		if err != nil {
			return fmt.Errorf("error parsing sequence value, error: %s", err.Error())
		}
		subOption = sdk.StartFromSequence(int(seq))
	case "5":
		timeIso, err := time.Parse(time.RFC3339, subValue)
		if err != nil {
			return fmt.Errorf("error parsing time value, error: %s", err.Error())
		}
		subOption = sdk.StartFromTime(timeIso)
	case "6":
		durationInt, err := strconv.ParseInt(subValue, 10, 64)
		if err != nil {
			return fmt.Errorf("error parsing duration value, error: %s", err.Error())
		}
		subOption = sdk.StartFromTimeDelta(time.Duration(durationInt) * time.Second)
	default:
		return fmt.Errorf("invalid subscription type, valid values are 1,2,3,4,5,6")
	}

	eventsStoreChan, err := client.SubscribeToEventsStoreWithRequest(ctx, &sdk.EventsStoreSubscription{
		Channel:          channel,
		Group:            group,
		ClientId:         clientId,
		SubscriptionType: subOption}, errChan)

	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case event, ok := <-eventsStoreChan:
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
					SetMessageId(event.Id).
					SetTimestamp(event.Timestamp).
					SetSequence(int64(event.Sequence))
				messagesCh <- msg
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}
