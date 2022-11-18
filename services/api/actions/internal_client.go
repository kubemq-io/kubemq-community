package actions

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/api/actions"
	"github.com/kubemq-io/kubemq-community/services/array"
	"github.com/kubemq-io/kubemq-community/services/broker"
	"go.uber.org/atomic"
)

type InternalClient struct {
	arrayService    *array.Array
	isAcceptTraffic *atomic.Bool
	clientID        string
}

func NewInternalClient() *InternalClient {
	return &InternalClient{
		arrayService:    nil,
		isAcceptTraffic: atomic.NewBool(false),
		clientID:        "kubemq-web-client",
	}
}

func (c *InternalClient) Init(ctx context.Context, arrayService *array.Array, brokerService *broker.Service) error {
	if arrayService == nil {
		return fmt.Errorf("array service is not initialized")
	}
	c.arrayService = arrayService
	if brokerService != nil {
		c.isAcceptTraffic.Store(brokerService.IsReady())
		brokerService.RegisterToNotifyState("grpc", c.updateBrokerState)
	} else {
		return fmt.Errorf("broker service is not initialized")
	}
	return nil
}

func (c *InternalClient) updateBrokerState(state bool) {
	c.isAcceptTraffic.Store(state)
}

func (c *InternalClient) CreateChannel(ctx context.Context, request *actions.CreateChannelRequest) error {
	if !c.isAcceptTraffic.Load() {
		return fmt.Errorf("can't create channel, broker is not ready to accept traffic")
	}
	switch request.Type {
	case "queues":
		err := createQueueChannelWithInternalClient(ctx, c, request.Name)
		if err != nil {
			return err
		}
	case "events", "events_store":
		err := createPubSubChannelWithInternalClient(ctx, c, request.Name, request.Type == "events_store")
		if err != nil {
			return err
		}
	case "commands", "queries":
		err := createCQRSChannelWIthInternalClient(ctx, c, request.Name, request.Type == "commands")
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported channel type %s", request.Type)
	}
	return nil
}

func (c *InternalClient) SendQueueMessage(ctx context.Context, request *actions.SendQueueMessageRequest) (*actions.SendQueueMessageResponse, error) {
	if !c.isAcceptTraffic.Load() {
		return nil, fmt.Errorf("can't send message, broker is not ready to accept traffic")
	}
	res, err := sendQueueMessageWithInternalClient(ctx, c, request)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (c *InternalClient) SendPubSubMessage(ctx context.Context, request *actions.SendPubSubMessageRequest) error {
	if !c.isAcceptTraffic.Load() {
		return fmt.Errorf("can't send message, broker is not ready to accept traffic")
	}
	err := sendPubSubMessageWithInternalClient(ctx, c, request)
	if err != nil {
		return err
	}
	return nil
}

func (c *InternalClient) ReceiveQueueMessages(ctx context.Context, request *actions.ReceiveQueueMessagesRequest) ([]*actions.ReceiveQueueMessageResponse, error) {
	if !c.isAcceptTraffic.Load() {
		return nil, fmt.Errorf("can't receive messages, broker is not ready to accept traffic")
	}
	res, err := receiveQueueMessagesWithInternalClient(ctx, c, request)
	if err != nil {
		return nil, err
	}
	return res, nil
}
func (c *InternalClient) PurgeQueueChannel(ctx context.Context, request *actions.PurgeQueueChannelRequest) (*actions.PurgeQueueChannelResponse, error) {
	if !c.isAcceptTraffic.Load() {
		return nil, fmt.Errorf("can't purge queue channel, broker is not ready to accept traffic")
	}
	res, err := purgeQueueChannelWithInternalClient(ctx, c, request)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (c *InternalClient) SubscribeToEvents(ctx context.Context, channel, group string, messagesChan chan *actions.SubscribePubSubMessage, errChan chan error) error {
	if !c.isAcceptTraffic.Load() {
		return fmt.Errorf("can't subscribe to events, broker is not ready to accept traffic")
	}
	err := subscribeToEventsWithInternalClient(ctx, c, channel, group, messagesChan, errChan)
	if err != nil {
		return err
	}
	return nil
}

func (c *InternalClient) SubscribeToEventsStore(ctx context.Context, channel, group, clientId, subType, subValue string, messagesChan chan *actions.SubscribePubSubMessage, errChan chan error) error {
	if !c.isAcceptTraffic.Load() {
		return fmt.Errorf("can't subscribe to events, broker is not ready to accept traffic")
	}
	err := subscribeToEventsStoreWithInternalClient(ctx, c, channel, group, clientId, subType, subValue, messagesChan, errChan)
	if err != nil {
		return err
	}
	return nil
}

func (c *InternalClient) StreamQueueMessages(ctx context.Context, requests chan *actions.StreamQueueMessagesRequest, responses chan *actions.StreamQueueMessagesResponse) {
	if !c.isAcceptTraffic.Load() {
		responses <- &actions.StreamQueueMessagesResponse{
			RequestType: actions.PollQueueMessagesRequestType,
			Message:     nil,
			Error:       "can't stream messages, broker is not ready to accept traffic",
			IsError:     true,
		}
		return
	}
	streamQueueMessagesWithInternalClient(ctx, c, requests, responses)
}
func (c *InternalClient) SendCQRSMessageRequest(ctx context.Context, request *actions.SendCQRSMessageRequest) (*actions.ReceiveCQRSResponse, error) {
	if !c.isAcceptTraffic.Load() {
		return nil, fmt.Errorf("can't send message, broker is not ready to accept traffic")
	}
	return sendCQRSMessageRequestWithInternalClient(ctx, c, request)
}

func (c *InternalClient) SendCQRSMessageResponse(ctx context.Context, response *actions.SendCQRSMessageResponse) error {
	if !c.isAcceptTraffic.Load() {
		return fmt.Errorf("can't send response, broker is not ready to accept traffic")
	}
	return sendCQRSResponseWithInternalClient(ctx, c, response)
}

func (c *InternalClient) SubscribeToCommands(ctx context.Context, channel, group string, messagesChan chan *actions.SubscribeCQRSRequestMessage, errChan chan error) error {
	if !c.isAcceptTraffic.Load() {
		return fmt.Errorf("can't subscribe to commands, broker is not ready to accept traffic")
	}
	err := subscribeToCommandsWithInternalClient(ctx, c, channel, group, messagesChan, errChan)
	if err != nil {
		return err
	}
	return nil
}
func (c *InternalClient) SubscribeToQueries(ctx context.Context, channel, group string, messagesChan chan *actions.SubscribeCQRSRequestMessage, errChan chan error) error {
	if !c.isAcceptTraffic.Load() {
		return fmt.Errorf("can't subscribe to queries, broker is not ready to accept traffic")
	}
	err := subscribeToQueriesWithInternalClient(ctx, c, channel, group, messagesChan, errChan)
	if err != nil {
		return err
	}
	return nil
}
