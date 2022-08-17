package actions

import (
	"context"
	"fmt"
	sdk "github.com/kubemq-io/kubemq-go"
	"time"
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

func createPubSubChannel(ctx context.Context, client *sdk.Client, name string, isStore bool) error {
	newCtx, _ := context.WithTimeout(ctx, 1*time.Second)
	errChan := make(chan error, 1)
	if isStore {
		_, err := client.SubscribeToEventsStore(newCtx, name, "", errChan, sdk.StartFromNewEvents())
		if err != nil {
			return err
		}

	} else {
		_, err := client.SubscribeToEvents(newCtx, name, "", errChan)
		if err != nil {
			return err
		}
	}
	time.Sleep(1 * time.Second)
	return nil
}

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
