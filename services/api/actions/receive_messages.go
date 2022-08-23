package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/api/actions"
	sdk "github.com/kubemq-io/kubemq-go"
)

func receiveQueueMessages(ctx context.Context, client *sdk.Client, actionReq *actions.ReceiveQueueMessagesRequest) ([]*actions.ReceiveQueueMessageResponse, error) {
	res, err := client.RQM().
		SetChannel(actionReq.Channel).
		SetIsPeak(actionReq.IsPeek).
		SetMaxNumberOfMessages(actionReq.Count).
		SetWaitTimeSeconds(1).
		Send(ctx)

	if err != nil {
		return nil, err
	}

	if res.IsError {
		return nil, fmt.Errorf("error receiving queue messages, error: %s", res.Error)
	}
	var messages []*actions.ReceiveQueueMessageResponse
	for _, message := range res.Messages {
		queueMessage := actions.NewReceiveQueueMessageResponse().
			SetMessageId(message.MessageID).
			SetBody(detectAndConvertToAny(message.Body)).
			SetMetadata(message.Metadata).
			SetClientId(message.ClientID).
			SetDelayedTo(message.Attributes.DelayedTo).
			SetExpirationAt(message.Attributes.ExpirationAt).
			SetReceivedCount(int64(message.Attributes.ReceiveCount)).
			SetReRoutedFrom(message.Attributes.ReRoutedFromQueue).
			SetTimestamp(message.Attributes.Timestamp).
			SetSequence(int64(message.Attributes.Sequence))

		if len(message.Tags) > 0 {
			data, _ := json.Marshal(message.Tags)
			queueMessage.SetTags(string(data))
		}
		messages = append(messages, queueMessage)
	}

	return messages, nil

}
