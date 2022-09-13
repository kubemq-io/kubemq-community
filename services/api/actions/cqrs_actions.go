package actions

import (
	"context"
	"encoding/json"
	"github.com/kubemq-io/kubemq-community/pkg/api/actions"
	sdk "github.com/kubemq-io/kubemq-go"
	"time"
)

func createCQRSChannel(ctx context.Context, client *sdk.Client, name string, isCommand bool) error {
	newCtx, _ := context.WithTimeout(ctx, 1*time.Second)
	errChan := make(chan error, 1)
	if isCommand {
		_, err := client.SubscribeToCommands(newCtx, name, "", errChan)
		if err != nil {
			return err
		}

	} else {
		_, err := client.SubscribeToQueries(newCtx, name, "", errChan)
		if err != nil {
			return err
		}
	}
	time.Sleep(1 * time.Second)
	return nil
}

func sendCQRSMessageRequest(ctx context.Context, client *sdk.Client, message *actions.SendCQRSMessageRequest) (*actions.ReceiveCQRSResponse, error) {

	body, _, err := detectAndConvertToBytesArray(message.Body)
	if err != nil {
		return nil, err
	}

	if message.IsCommands {
		resp, err := client.C().
			SetChannel(message.Channel).
			SetBody(body).
			SetId(message.RequestId).
			SetMetadata(message.Metadata).
			SetTags(message.TagsKeyValue()).
			SetTimeout(time.Duration(message.Timeout) * time.Second).
			Send(ctx)
		if err != nil {
			return nil, err
		}
		actionResponse := actions.NewReceiveCQRSResponse().
			SetExecuted(resp.Executed).
			SetError(resp.Error).
			SetTimestamp(resp.ExecutedAt.UnixMilli())
		if len(resp.Tags) > 0 {
			data, _ := json.Marshal(resp.Tags)
			actionResponse.SetTags(string(data))
		}
		return actionResponse, nil

	} else {
		resp, err := client.Q().
			SetChannel(message.Channel).
			SetBody(body).
			SetId(message.RequestId).
			SetMetadata(message.Metadata).
			SetTags(message.TagsKeyValue()).
			SetTimeout(time.Duration(message.Timeout) * time.Second).
			Send(ctx)
		if err != nil {
			return nil, err
		}
		actionResponse := actions.NewReceiveCQRSResponse().
			SetBody(detectAndConvertToAny(resp.Body)).
			SetMetadata(resp.Metadata).
			SetExecuted(resp.Executed).
			SetError(resp.Error).
			SetTimestamp(resp.ExecutedAt.UnixMilli())
		if len(resp.Tags) > 0 {
			data, _ := json.Marshal(resp.Tags)
			actionResponse.SetTags(string(data))
		}
		return actionResponse, nil

	}

}

func sendCQRSResponse(ctx context.Context, client *sdk.Client, resp *actions.SendCQRSMessageResponse) error {
	body, _, err := detectAndConvertToBytesArray(resp.Body)
	if err != nil {
		return err
	}
	return client.R().
		SetRequestId(resp.RequestId).
		SetBody(body).
		SetMetadata(resp.Metadata).
		SetTags(resp.TagsKeyValue()).
		SetResponseTo(resp.ReplyChannel).
		SetError(resp.GetError()).
		SetExecutedAt(time.Now()).
		Send(ctx)

}
func subscribeToCommands(ctx context.Context, client *sdk.Client, channel, group string, messagesCh chan *actions.SubscribeCQRSRequestMessage, errChan chan error) error {
	commandsChan, err := client.SubscribeToCommands(ctx, channel, group, errChan)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case command, ok := <-commandsChan:
				if !ok {
					return
				}
				msg := actions.NewSubscribeCQRSRequestMessage()
				if len(command.Tags) > 0 {
					data, _ := json.Marshal(command.Tags)
					msg.SetTags(string(data))
				}
				msg.SetBody(detectAndConvertToAny(command.Body)).
					SetMetadata(command.Metadata).
					SetTimestamp(time.Now().UnixMilli()).
					SetRequestId(command.Id).
					SetBody(detectAndConvertToAny(command.Body)).
					SetReplyChannel(command.ResponseTo).
					SetIsCommand(true)
				messagesCh <- msg
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func subscribeToQueries(ctx context.Context, client *sdk.Client, channel, group string, messagesCh chan *actions.SubscribeCQRSRequestMessage, errChan chan error) error {
	queriesChan, err := client.SubscribeToQueries(ctx, channel, group, errChan)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case query, ok := <-queriesChan:
				if !ok {
					return
				}
				msg := actions.NewSubscribeCQRSRequestMessage()
				if len(query.Tags) > 0 {
					data, _ := json.Marshal(query.Tags)
					msg.SetTags(string(data))
				}
				msg.SetBody(detectAndConvertToAny(query.Body)).
					SetMetadata(query.Metadata).
					SetTimestamp(time.Now().UnixMilli()).
					SetRequestId(query.Id).
					SetBody(detectAndConvertToAny(query.Body)).
					SetReplyChannel(query.ResponseTo).
					SetIsCommand(false)
				messagesCh <- msg
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}
