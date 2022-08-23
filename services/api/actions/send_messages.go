package actions

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/api/actions"
	sdk "github.com/kubemq-io/kubemq-go"
)

func sendQueueMessage(ctx context.Context, client *sdk.Client, message *actions.SendQueueMessageRequest) (*actions.SendQueueMessageResponse, error) {

	body, _, err := detectAndConvertToBytesArray(message.Body)
	if err != nil {
		return nil, err
	}
	res, err := client.QM().
		SetChannel(message.Channel).
		SetBody(body).
		SetId(message.MessageId).
		SetMetadata(message.Metadata).
		SetTags(message.TagsKeyValue()).
		SetPolicyMaxReceiveCount(message.MaxReceiveCount).
		SetPolicyMaxReceiveQueue(message.MaxReceiveQueue).
		SetPolicyExpirationSeconds(message.ExpirationAt).
		SetPolicyDelaySeconds(message.DelayedTo).
		Send(ctx)

	if err != nil {
		return nil, err
	}

	if res.IsError {
		return nil, fmt.Errorf("error sending queue message on channel: %s , error: %s", message.Channel, res.Error)
	}
	return actions.NewSendQueueMessageResponse().
		SetMessageId(res.MessageID).
		SetSentAt(res.SentAt).
		SetExpiresAt(res.ExpirationAt).
		SetDelayedTo(res.DelayedTo), nil

}
