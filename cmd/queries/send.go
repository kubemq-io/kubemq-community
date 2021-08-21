package queries

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/utils"
	"github.com/kubemq-io/kubemq-community/pkg/uuid"
	"time"

	"github.com/kubemq-io/kubemq-community/config"
	"github.com/spf13/cobra"
)

type SendOptions struct {
	cfg      *config.Config
	channel  string
	body     string
	metadata string
	timeout  int
}

var queriesSendExamples = `
	# Send query to a 'queries' channel
	kubemq queries send some-channel some-query
	
	# Send query to a 'queries' channel with metadata
	kubemq queries send some-channel some-body -m some-metadata
	
	# Send query to a 'queries' channel with 120 seconds timeout
	kubemq queries send some-channel some-body -o 120
	
`
var queriesSendLong = `Queries send command allow to send messages to 'queries' channel with an option to set query time-out and caching parameters`
var queriesSendShort = `Queries send messages to a 'queries' channel command`

func NewCmdQueriesSend(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &SendOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "send",
		Aliases: []string{"s"},
		Short:   queriesSendShort,
		Long:    queriesSendLong,
		Example: queriesSendExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}
	cmd.PersistentFlags().StringVarP(&o.metadata, "metadata", "m", "", "Set metadata body")
	cmd.PersistentFlags().IntVarP(&o.timeout, "timeout", "o", 30, "Set command timeout")
	return cmd
}

func (o *SendOptions) Complete(args []string) error {
	if len(args) >= 1 {
		o.channel = args[0]

	} else {
		return fmt.Errorf("missing channel argument")
	}
	if len(args) >= 2 {
		o.body = args[1]
	} else {
		return fmt.Errorf("missing body argument")
	}
	return nil
}

func (o *SendOptions) Validate() error {
	return nil
}

func (o *SendOptions) Run(ctx context.Context) error {
	client, err := utils.GetKubeMQClient(ctx, o.cfg)
	if err != nil {
		return fmt.Errorf("create kubemq client, %s", err.Error())
	}

	defer func() {
		_ = client.Close()
	}()
	fmt.Println("Sending Query:")
	msg := client.Q().
		SetChannel(o.channel).
		SetId(uuid.New()).
		SetBody([]byte(o.body)).
		SetMetadata(o.metadata).
		SetTimeout(time.Duration(o.timeout) * time.Second)

	printQuery(msg)
	res, err := msg.Send(ctx)
	if err != nil {
		return fmt.Errorf("sending query body, %s", err.Error())
	}
	fmt.Println("Getting Query Response:")
	printQueryResponse(res)
	return nil
}
