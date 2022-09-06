package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/api/actions"
	sdk "github.com/kubemq-io/kubemq-go"
)

func createQueueChannel(ctx context.Context, client *sdk.Client, name string) error {
	rec, err := client.RQM().
		SetChannel(name).
		SetMaxNumberOfMessages(1).
		SetWaitTimeSeconds(1).
		Send(ctx)
	if err != nil {
		return err
	}
	if rec.IsError {
		return fmt.Errorf("error creating queue channel %s , error: %s", name, rec.Error)
	}
	return nil
}
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
func purgeQueueChannel(ctx context.Context, client *sdk.Client, actionReq *actions.PurgeQueueChannelRequest) (*actions.PurgeQueueChannelResponse, error) {
	res, err := client.AQM().
		SetChannel(actionReq.Channel).
		SetWaitTimeSeconds(2).
		Send(ctx)

	if err != nil {
		return nil, err
	}

	if res.IsError {
		return nil, fmt.Errorf("error purging queue messages, error: %s", res.Error)
	}
	return actions.NewPurgeQueueChannelResponse().
		SetCount(res.AffectedMessages), nil
}

func streamQueueMessages(ctx context.Context, streamClient *sdk.QueuesClient, requests chan *actions.StreamQueueMessagesRequest, responses chan *actions.StreamQueueMessagesResponse) {
	var currentResponse *sdk.QueueTransactionMessageResponse
	var doneChan chan struct{}
	defer func() {
		fmt.Println("closing stream queue messages")
		if doneChan != nil {
			doneChan <- struct{}{}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			if doneChan != nil {
				doneChan <- struct{}{}
			}
			return
		case request := <-requests:
			switch request.RequestType {
			case actions.PollQueueMessagesRequestType:
				fmt.Println("polling queue messages")
				if currentResponse != nil {
					responses <- actions.NewStreamQueueMessageResponse().
						SetRequestType(request.RequestType).
						SetError(fmt.Errorf("error polling queue messages, previous request is not finished yet"))
					continue
				}
				tx, err := streamClient.Transaction(ctx,
					sdk.NewQueueTransactionMessageRequest().
						SetChannel(request.Channel).
						SetVisibilitySeconds(request.VisibilitySeconds).
						SetWaitTimeSeconds(request.WaitSeconds))
				if err != nil {
					responses <- actions.NewStreamQueueMessageResponse().
						SetRequestType(request.RequestType).
						SetError(err)
					continue
				}
				if tx.Message != nil {
					queueMessage := actions.NewReceiveQueueMessageResponse().
						SetMessageId(tx.Message.MessageID).
						SetBody(detectAndConvertToAny(tx.Message.Body)).
						SetMetadata(tx.Message.Metadata).
						SetClientId(tx.Message.ClientID).
						SetDelayedTo(tx.Message.Attributes.DelayedTo).
						SetExpirationAt(tx.Message.Attributes.ExpirationAt).
						SetReceivedCount(int64(tx.Message.Attributes.ReceiveCount)).
						SetReRoutedFrom(tx.Message.Attributes.ReRoutedFromQueue).
						SetTimestamp(tx.Message.Attributes.Timestamp).
						SetSequence(int64(tx.Message.Attributes.Sequence))

					if len(tx.Message.Tags) > 0 {
						data, _ := json.Marshal(tx.Message.Tags)
						queueMessage.SetTags(string(data))
					}
					responses <- actions.NewStreamQueueMessageResponse().
						SetRequestType(request.RequestType).
						SetMessage(queueMessage)
					currentResponse = tx
				} else {
					currentResponse = nil
				}

			case actions.AckQueueMessagesRequestType, actions.RejectQueueMessagesRequestType:
				if currentResponse != nil {
					if currentResponse.Message == nil {
						responses <- actions.NewStreamQueueMessageResponse().
							SetRequestType(request.RequestType).
							SetError(fmt.Errorf("error acknowledging queue messages, no message found"))
					} else {
						var err error
						if request.RequestType == actions.AckQueueMessagesRequestType {
							err = currentResponse.Ack()
						} else {
							err = currentResponse.Reject()
						}
						if err != nil {
							responses <- actions.NewStreamQueueMessageResponse().
								SetRequestType(request.RequestType).
								SetError(err)

						} else {
							responses <- actions.NewStreamQueueMessageResponse().
								SetRequestType(request.RequestType)

						}
					}
					currentResponse = nil
				} else {
					responses <- actions.NewStreamQueueMessageResponse().
						SetRequestType(request.RequestType).
						SetError(fmt.Errorf("no active stream"))
				}
			}
		}
	}
}
