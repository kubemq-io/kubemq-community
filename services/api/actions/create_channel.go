package actions

import (
	"context"
	sdk "github.com/kubemq-io/kubemq-go"
	"time"
)

func createCommandsQueriesChannel(ctx context.Context, client *sdk.Client, name string, isCommand bool) error {
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
