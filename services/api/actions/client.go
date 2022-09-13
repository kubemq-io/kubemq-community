package actions

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/api/actions"
	sdk "github.com/kubemq-io/kubemq-go"
)

type Client struct {
	client       *sdk.Client
	streamClient *sdk.QueuesClient
}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) Init(ctx context.Context, localPort int) error {
	//host, err := os.Hostname()
	//if err != nil {
	//	return err
	//}
	var err error
	c.client, err = sdk.NewClient(ctx,
		sdk.WithAddress("0.0.0.0", localPort),
		sdk.WithTransportType(sdk.TransportTypeGRPC),
		sdk.WithClientId("kubemq-admin-client"))
	if err != nil {
		return err
	}
	c.streamClient, err = sdk.NewQueuesStreamClient(ctx,
		sdk.WithAddress("0.0.0.0", localPort),
		sdk.WithTransportType(sdk.TransportTypeGRPC),
		sdk.WithClientId("kubemq-admin-client"))
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) CreateChannel(ctx context.Context, request *actions.CreateChannelRequest) error {
	switch request.Type {
	case "queues":
		err := createQueueChannel(ctx, c.client, request.Name)
		if err != nil {
			return err
		}
	case "events", "events_store":
		err := createPubSubChannel(ctx, c.client, request.Name, request.Type == "events_store")
		if err != nil {
			return err
		}
	case "commands", "queries":
		err := createCQRSChannel(ctx, c.client, request.Name, request.Type == "commands")
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported channel type %s", request.Type)
	}
	return nil
}

func (c *Client) SendQueueMessage(ctx context.Context, request *actions.SendQueueMessageRequest) (*actions.SendQueueMessageResponse, error) {
	res, err := sendQueueMessage(ctx, c.client, request)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (c *Client) SendPubSubMessage(ctx context.Context, request *actions.SendPubSubMessageRequest) error {
	err := sendPubSubMessage(ctx, c.client, request)
	if err != nil {
		return err
	}
	return nil
}
func (c *Client) ReceiveQueueMessages(ctx context.Context, request *actions.ReceiveQueueMessagesRequest) ([]*actions.ReceiveQueueMessageResponse, error) {
	res, err := receiveQueueMessages(ctx, c.client, request)
	if err != nil {
		return nil, err
	}
	return res, nil
}
func (c *Client) PurgeQueueChannel(ctx context.Context, request *actions.PurgeQueueChannelRequest) (*actions.PurgeQueueChannelResponse, error) {
	res, err := purgeQueueChannel(ctx, c.client, request)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (c *Client) SubscribeToEvents(ctx context.Context, channel, group string, messagesChan chan *actions.SubscribePubSubMessage, errChan chan error) error {
	err := subscribeToEvents(ctx, c.client, channel, group, messagesChan, errChan)
	if err != nil {
		return err
	}
	return nil
}
func (c *Client) SubscribeToEventsStore(ctx context.Context, channel, group, clientId, subType, subValue string, messagesChan chan *actions.SubscribePubSubMessage, errChan chan error) error {
	err := subscribeToEventsStore(ctx, c.client, channel, group, clientId, subType, subValue, messagesChan, errChan)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) StreamQueueMessages(ctx context.Context, requests chan *actions.StreamQueueMessagesRequest, responses chan *actions.StreamQueueMessagesResponse) {
	streamQueueMessages(ctx, c.streamClient, requests, responses)
}
func (c *Client) SendCQRSMessageRequest(ctx context.Context, request *actions.SendCQRSMessageRequest) (*actions.ReceiveCQRSResponse, error) {
	return sendCQRSMessageRequest(ctx, c.client, request)
}

func (c *Client) SendCQRSMessageResponse(ctx context.Context, response *actions.SendCQRSMessageResponse) error {
	return sendCQRSResponse(ctx, c.client, response)
}

func (c *Client) SubscribeToCommands(ctx context.Context, channel, group string, messagesChan chan *actions.SubscribeCQRSRequestMessage, errChan chan error) error {
	err := subscribeToCommands(ctx, c.client, channel, group, messagesChan, errChan)
	if err != nil {
		return err
	}
	return nil
}
func (c *Client) SubscribeToQueries(ctx context.Context, channel, group string, messagesChan chan *actions.SubscribeCQRSRequestMessage, errChan chan error) error {
	err := subscribeToQueries(ctx, c.client, channel, group, messagesChan, errChan)
	if err != nil {
		return err
	}
	return nil
}
