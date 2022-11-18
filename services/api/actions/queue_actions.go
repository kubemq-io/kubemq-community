package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/api/actions"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	pb "github.com/kubemq-io/protobuf/go"
	"time"
)

func createQueueChannelWithInternalClient(ctx context.Context, client *InternalClient, name string) error {
	metrics.ReportClient("queues", "receive", name, 1)
	defer metrics.ReportClient("queues", "receive", name, -1)
	rec, err := client.arrayService.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		RequestID:           "",
		ClientID:            client.clientID,
		Channel:             name,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
		IsPeak:              true,
	})
	if err != nil {
		return err
	}
	if rec.IsError {
		return fmt.Errorf("error creating queue channel %s , error: %s", name, rec.Error)
	}
	return nil
}
func sendQueueMessageWithInternalClient(ctx context.Context, client *InternalClient, message *actions.SendQueueMessageRequest) (*actions.SendQueueMessageResponse, error) {
	body, _, err := detectAndConvertToBytesArray(message.Body)
	if err != nil {
		return nil, err
	}
	msg := &pb.QueueMessage{
		MessageID:  message.MessageId,
		ClientID:   client.clientID,
		Channel:    message.Channel,
		Metadata:   message.Metadata,
		Body:       body,
		Tags:       message.TagsKeyValue(),
		Attributes: nil,
		Policy: &pb.QueueMessagePolicy{
			ExpirationSeconds: int32(message.ExpirationAt),
			DelaySeconds:      int32(message.DelayedTo),
			MaxReceiveCount:   int32(message.MaxReceiveCount),
			MaxReceiveQueue:   message.MaxReceiveQueue,
		},
	}
	res, err := client.arrayService.SendQueueMessage(ctx, msg)
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

func receiveQueueMessagesWithInternalClient(ctx context.Context, client *InternalClient, actionReq *actions.ReceiveQueueMessagesRequest) ([]*actions.ReceiveQueueMessageResponse, error) {
	req := &pb.ReceiveQueueMessagesRequest{
		RequestID:           "",
		ClientID:            client.clientID,
		Channel:             actionReq.Channel,
		MaxNumberOfMessages: int32(actionReq.Count),
		WaitTimeSeconds:     1,
		IsPeak:              actionReq.IsPeek,
	}
	res, err := client.arrayService.ReceiveQueueMessages(ctx, req)

	if err != nil {
		return nil, err
	}

	if res.IsError {
		return nil, fmt.Errorf("error receiving queue messages, error: %s", res.Error)
	}
	metrics.ReportReceiveQueueMessages(req, res)
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

func purgeQueueChannelWithInternalClient(ctx context.Context, client *InternalClient, actionReq *actions.PurgeQueueChannelRequest) (*actions.PurgeQueueChannelResponse, error) {
	res, err := client.arrayService.AckAllQueueMessages(ctx, &pb.AckAllQueueMessagesRequest{
		RequestID:            "",
		ClientID:             client.clientID,
		Channel:              actionReq.Channel,
		WaitTimeSeconds:      2,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	})
	if err != nil {
		return nil, err
	}

	if res.IsError {
		return nil, fmt.Errorf("error purging queue messages, error: %s", res.Error)
	}
	return actions.NewPurgeQueueChannelResponse().
		SetCount(res.AffectedMessages), nil
}

func streamQueueMessagesWithInternalClient(ctx context.Context, streamClient *InternalClient, requests chan *actions.StreamQueueMessagesRequest, responses chan *actions.StreamQueueMessagesResponse) {
	messagesRequestsCh := make(chan *pb.QueuesDownstreamRequest, 1)
	messagesResponsesCh := make(chan *pb.QueuesDownstreamResponse, 1)
	doneCh := make(chan bool, 1)
	err := streamClient.arrayService.QueuesDownstream(ctx, messagesRequestsCh, messagesResponsesCh, doneCh)
	if err != nil {
		responses <- actions.NewStreamQueueMessageResponse().SetError(err)
		return
	}
	defer func() {
		doneCh <- true
	}()

	var currentResponse *pb.QueuesDownstreamResponse

	for {
		select {
		case <-ctx.Done():
			return
		case request := <-requests:
			switch request.RequestType {
			case actions.PollQueueMessagesRequestType:
				if currentResponse != nil {
					responses <- actions.NewStreamQueueMessageResponse().
						SetRequestType(request.RequestType).
						SetError(fmt.Errorf("error polling queue messages, previous request is not finished yet"))
					continue
				}
				req := &pb.QueuesDownstreamRequest{
					RequestID:       "",
					ClientID:        streamClient.clientID,
					RequestTypeData: pb.QueuesDownstreamRequestType_Get,
					Channel:         request.Channel,
					MaxItems:        1,
					WaitTimeout:     3600 * 1000,
				}
				select {
				case <-ctx.Done():
					return
				case messagesRequestsCh <- req:

				}
				select {
				case <-ctx.Done():
					return
				case response := <-messagesResponsesCh:
					if response.IsError {
						responses <- actions.NewStreamQueueMessageResponse().
							SetRequestType(request.RequestType).
							SetError(fmt.Errorf(response.Error))
						continue
					} else {
						if len(response.Messages) == 0 {
							currentResponse = nil
							continue
						}
						currentResponse = response
						currentMessage := response.Messages[0]
						queueMessage := actions.NewReceiveQueueMessageResponse().
							SetMessageId(currentMessage.MessageID).
							SetBody(detectAndConvertToAny(currentMessage.Body)).
							SetMetadata(currentMessage.Metadata).
							SetClientId(currentMessage.ClientID).
							SetDelayedTo(currentMessage.Attributes.DelayedTo).
							SetExpirationAt(currentMessage.Attributes.ExpirationAt).
							SetReceivedCount(int64(currentMessage.Attributes.ReceiveCount)).
							SetReRoutedFrom(currentMessage.Attributes.ReRoutedFromQueue).
							SetTimestamp(currentMessage.Attributes.Timestamp).
							SetSequence(int64(currentMessage.Attributes.Sequence))

						if len(currentMessage.Tags) > 0 {
							data, _ := json.Marshal(currentMessage.Tags)
							queueMessage.SetTags(string(data))
						}
						responses <- actions.NewStreamQueueMessageResponse().
							SetRequestType(request.RequestType).
							SetMessage(queueMessage)
						metrics.ReportReceiveStreamQueueMessage(currentMessage)
					}

				}
			case actions.AckQueueMessagesRequestType, actions.RejectQueueMessagesRequestType:
				if currentResponse != nil {
					currentMessage := currentResponse.Messages[0]
					var reqType pb.QueuesDownstreamRequestType
					if request.RequestType == actions.AckQueueMessagesRequestType {
						reqType = pb.QueuesDownstreamRequestType_AckRange
					} else {
						reqType = pb.QueuesDownstreamRequestType_NAckRange
					}
					req := &pb.QueuesDownstreamRequest{
						RequestID:        currentResponse.RefRequestId,
						ClientID:         streamClient.clientID,
						RequestTypeData:  reqType,
						Channel:          currentMessage.Channel,
						SequenceRange:    []int64{int64(currentMessage.Attributes.Sequence)},
						RefTransactionId: currentResponse.TransactionId,
					}
					messagesRequestsCh <- req
					select {
					case <-ctx.Done():
						return
					case response := <-messagesResponsesCh:
						if response.IsError {
							responses <- actions.NewStreamQueueMessageResponse().
								SetRequestType(request.RequestType).
								SetError(fmt.Errorf(response.Error))
							continue
						} else {
							responses <- actions.NewStreamQueueMessageResponse().
								SetRequestType(request.RequestType)

						}
					case <-time.After(200 * time.Millisecond):
						responses <- actions.NewStreamQueueMessageResponse().
							SetRequestType(request.RequestType)
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
