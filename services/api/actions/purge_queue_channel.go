package actions

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/api/actions"
	sdk "github.com/kubemq-io/kubemq-go"
)

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
