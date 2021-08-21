package queries

import (
	"context"
	"fmt"
	"github.com/AlecAivazis/survey/v2"
	"github.com/kubemq-io/kubemq-community/pkg/utils"

	"github.com/kubemq-io/kubemq-community/config"
	"github.com/spf13/cobra"
	"time"
)

type ReceiveOptions struct {
	cfg          *config.Config
	channel      string
	group        string
	autoResponse bool
}

var queriesReceiveExamples = `
	# Receive 'queries'  from a 'queries' channel (blocks until next body)
	kubemq queries receive some-channel

	# Receive 'queries' from a 'queries' channel with group(blocks until next body)
	kubemq queries receive some-channel -g G1
`
var queriesReceiveLong = `Queries receive (Subscribe) command allows to receive a body from a 'queries' channel and response with appropriate reply`
var queriesReceiveShort = `Queries receive a body from a 'queries' channel`

func NewCmdQueriesReceive(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &ReceiveOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "receive",
		Aliases: []string{"r", "rec", "subscribe", "sub"},
		Short:   queriesReceiveShort,
		Long:    queriesReceiveLong,
		Example: queriesReceiveExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}

	cmd.PersistentFlags().StringVarP(&o.group, "group", "g", "", "set 'queries' channel consumer group (load balancing)")
	cmd.PersistentFlags().BoolVarP(&o.autoResponse, "auto-response", "a", false, "set auto response executed query")
	return cmd
}

func (o *ReceiveOptions) Complete(args []string) error {
	if len(args) >= 1 {
		o.channel = args[0]
		return nil
	}
	return fmt.Errorf("missing channel argument")
}

func (o *ReceiveOptions) Validate() error {
	return nil
}

func (o *ReceiveOptions) Run(ctx context.Context) error {
	client, err := utils.GetKubeMQClient(ctx, o.cfg)
	if err != nil {
		return fmt.Errorf("create kubemq client, %s", err.Error())
	}

	defer func() {
		_ = client.Close()
	}()

	errChan := make(chan error, 1)
	queriesChan, err := client.SubscribeToQueries(ctx, o.channel, o.group, errChan)

	if err != nil {
		utils.Println(fmt.Errorf("receive 'queries' messages, %s", err.Error()).Error())
	}
	for {
		utils.Println("waiting for the next query body...")
		select {
		case err := <-errChan:
			return fmt.Errorf("server disconnected with error: %s", err.Error())

		case query, opened := <-queriesChan:
			if !opened {
				utils.Println("server disconnected")
				return nil
			}
			printQueryReceive(query)
			if o.autoResponse {
				err = client.R().SetRequestId(query.Id).SetExecutedAt(time.Now()).SetResponseTo(query.ResponseTo).SetBody([]byte("executed your query")).Send(ctx)
				if err != nil {
					return err
				}
				utils.Println("auto execution sent executed response ")
				continue
			}
			var isExecuted bool
			prompt := &survey.Confirm{
				Renderer: survey.Renderer{},
				Message:  "Set executed ?",
				Help:     "",
			}
			err := survey.AskOne(prompt, &isExecuted)

			if err != nil {
				return err
			}
			if isExecuted {

				respBody := ""
				prompt := &survey.Input{
					Renderer: survey.Renderer{},
					Message:  "Set response body",
					Default:  "response-to",
					Help:     "",
				}
				err := survey.AskOne(prompt, &respBody)
				if err != nil {
					return err
				}
				err = client.R().SetRequestId(query.Id).SetExecutedAt(time.Now()).SetResponseTo(query.ResponseTo).SetBody([]byte(respBody)).Send(ctx)
				if err != nil {
					return err
				}
				continue
			}
			err = client.R().SetRequestId(query.Id).SetError(fmt.Errorf("query not executed")).SetResponseTo(query.ResponseTo).Send(ctx)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}

}
