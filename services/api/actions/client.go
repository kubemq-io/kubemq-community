package actions

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/api/actions"
	"os"
)
import sdk "github.com/kubemq-io/kubemq-go"

type Client struct {
	client *sdk.Client
}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) Init(ctx context.Context, localPort int) error {
	host, err := os.Hostname()
	if err != nil {
		return err
	}
	c.client, err = sdk.NewClient(ctx,
		sdk.WithAddress(host, localPort),
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
		err := createCommandsQueriesChannel(ctx, c.client, request.Name, request.Type == "commands")
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
