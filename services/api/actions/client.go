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
	c.client, err = sdk.NewClient(ctx, sdk.WithAddress(host, localPort), sdk.WithTransportType(sdk.TransportTypeGRPC))
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
