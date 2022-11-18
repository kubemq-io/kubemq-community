package actions

import (
	"context"
	"encoding/json"
	"github.com/kubemq-io/kubemq-community/pkg/api/actions"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	pb "github.com/kubemq-io/protobuf/go"
	"time"
)

func createCQRSChannelWIthInternalClient(ctx context.Context, client *InternalClient, name string, isCommand bool) error {
	newCtx, _ := context.WithTimeout(ctx, 1*time.Second)
	errChan := make(chan error, 1)
	var err error
	subId := ""
	var pattern string
	if isCommand {
		msgCh := make(chan *pb.Request, 1)
		subId, err = client.arrayService.SubscribeToCommands(newCtx, &pb.Subscribe{
			SubscribeTypeData: pb.Subscribe_Commands,
			ClientID:          client.clientID,
			Channel:           name,
			Group:             "",
		}, msgCh, errChan)
		if err != nil {
			return err
		}
		pattern = "commands"
	} else {
		msgCh := make(chan *pb.Request, 1)
		subId, err = client.arrayService.SubscribeToQueries(newCtx, &pb.Subscribe{
			SubscribeTypeData: pb.Subscribe_Queries,
			ClientID:          client.clientID,
			Channel:           name,
			Group:             "",
		}, msgCh, errChan)
		if err != nil {
			return err
		}
		pattern = "queries"
	}
	metrics.ReportClient("pattern", "receive", name, 1)
	time.Sleep(1 * time.Second)
	err = client.arrayService.DeleteClient(subId)
	if err != nil {
		return err
	}
	metrics.ReportClient(pattern, "receive", name, -1)
	return nil
}

func sendCQRSMessageRequestWithInternalClient(ctx context.Context, client *InternalClient, message *actions.SendCQRSMessageRequest) (*actions.ReceiveCQRSResponse, error) {
	body, _, err := detectAndConvertToBytesArray(message.Body)
	if err != nil {
		return nil, err
	}

	if message.IsCommands {
		req := &pb.Request{
			RequestID:       message.RequestId,
			RequestTypeData: pb.Request_Command,
			ClientID:        client.clientID,
			Channel:         message.Channel,
			Metadata:        message.Metadata,
			Body:            body,
			Timeout:         int32(message.Timeout * 1000),
			Tags:            message.TagsKeyValue(),
		}
		resp, err := client.arrayService.SendCommand(ctx, req)
		if err != nil {
			return nil, err
		}
		metrics.ReportRequest(req, resp, err)
		actionResponse := actions.NewReceiveCQRSResponse().
			SetExecuted(resp.Executed).
			SetError(resp.Error).
			SetTimestamp(time.Unix(0, resp.Timestamp).UnixMilli())
		if len(resp.Tags) > 0 {
			data, _ := json.Marshal(resp.Tags)
			actionResponse.SetTags(string(data))
		}
		return actionResponse, nil

	} else {
		req := &pb.Request{
			RequestID:       message.RequestId,
			RequestTypeData: pb.Request_Query,
			ClientID:        client.clientID,
			Channel:         message.Channel,
			Metadata:        message.Metadata,
			Body:            body,
			Timeout:         int32(message.Timeout * 1000),
			Tags:            message.TagsKeyValue(),
		}

		resp, err := client.arrayService.SendQuery(ctx, req)
		if err != nil {
			return nil, err
		}
		metrics.ReportRequest(req, resp, err)
		actionResponse := actions.NewReceiveCQRSResponse().
			SetBody(detectAndConvertToAny(resp.Body)).
			SetMetadata(resp.Metadata).
			SetExecuted(resp.Executed).
			SetError(resp.Error).
			SetTimestamp(time.Unix(0, resp.Timestamp).UnixMilli())
		if len(resp.Tags) > 0 {
			data, _ := json.Marshal(resp.Tags)
			actionResponse.SetTags(string(data))
		}
		return actionResponse, nil

	}

}

func sendCQRSResponseWithInternalClient(ctx context.Context, client *InternalClient, resp *actions.SendCQRSMessageResponse) error {
	body, _, err := detectAndConvertToBytesArray(resp.Body)
	if err != nil {
		return err
	}
	response := &pb.Response{
		ClientID:     client.clientID,
		RequestID:    resp.RequestId,
		ReplyChannel: resp.ReplyChannel,
		Metadata:     resp.Metadata,
		Body:         body,
		CacheHit:     false,
		Timestamp:    time.Now().UnixNano(),
		Executed:     resp.Executed,
		Error:        resp.Error,
		Span:         nil,
		Tags:         resp.TagsKeyValue(),
	}
	metrics.ReportResponse(response, err)
	return client.arrayService.SendResponse(ctx, response)
}

func subscribeToCommandsWithInternalClient(ctx context.Context, client *InternalClient, channel, group string, messagesCh chan *actions.SubscribeCQRSRequestMessage, errChan chan error) error {
	commandsChan := make(chan *pb.Request, 1)
	request := &pb.Subscribe{
		SubscribeTypeData: pb.Subscribe_Commands,
		ClientID:          client.clientID,
		Channel:           channel,
		Group:             group,
	}
	subId, err := client.arrayService.SubscribeToCommands(ctx, request, commandsChan, errChan)
	if err != nil {
		return err
	}
	metrics.ReportClient("commands", "receive", channel, 1)
	go func() {
		defer func() {
			_ = client.arrayService.DeleteClient(subId)
			metrics.ReportClient("commands", "receive", channel, -1)
		}()
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
					SetRequestId(command.RequestID).
					SetBody(detectAndConvertToAny(command.Body)).
					SetReplyChannel(command.ReplyChannel).
					SetIsCommand(true)
				messagesCh <- msg
				metrics.ReportRequest(command, nil, nil)
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func subscribeToQueriesWithInternalClient(ctx context.Context, client *InternalClient, channel, group string, messagesCh chan *actions.SubscribeCQRSRequestMessage, errChan chan error) error {

	queriesChan := make(chan *pb.Request, 1)
	subId, err := client.arrayService.SubscribeToQueries(ctx, &pb.Subscribe{
		SubscribeTypeData: pb.Subscribe_Queries,
		ClientID:          client.clientID,
		Channel:           channel,
		Group:             group,
	}, queriesChan, errChan)
	if err != nil {
		return err
	}
	metrics.ReportClient("queries", "receive", channel, 1)
	go func() {
		defer func() {
			_ = client.arrayService.DeleteClient(subId)
			metrics.ReportClient("queries", "receive", channel, -1)
		}()
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
					SetRequestId(query.RequestID).
					SetBody(detectAndConvertToAny(query.Body)).
					SetReplyChannel(query.ReplyChannel).
					SetIsCommand(false)
				messagesCh <- msg
				metrics.ReportRequest(query, nil, nil)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}
