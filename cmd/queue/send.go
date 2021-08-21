package queue

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/utils"
	"github.com/spf13/cobra"
)

type SendOptions struct {
	cfg        *config.Config
	expiration int
	delay      int
	channel    string
	body       string
	maxReceive int
	metadata   string
	deadLetter string
	messages   int
}

var queueSendExamples = `
	# Send message to a queue channel channel
	kubemq queue send q1 some-message
	
	# Send message to a queue channel with metadata
	kubemq queue send q1 some-message --metadata some-metadata
	
	# Send 5 messages to a queues channel with metadata
	kubemq queue send q1 some-message --metadata some-metadata -m 5
	
	# Send message to a queue channel with a message expiration of 5 seconds
	kubemq queue send q1 some-message -e 5

	# Send message to a queue channel with a message delay of 5 seconds
	kubemq queue send q1 some-message -d 5

	# Send message to a queue channel with a message policy of max receive 5 times and dead-letter queue 'dead-letter'
	kubemq queue send q1 some-message -r 5 -q dead-letter
`
var queueSendLong = `Queues send command allows to send one or many message to a queue channel`
var queueSendShort = `Queues send a message to a queue channel command`

func NewCmdQueueSend(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &SendOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{
		Use:        "send",
		Aliases:    []string{"s"},
		SuggestFor: nil,
		Short:      queueSendShort,
		Long:       queueSendLong,
		Example:    queueSendExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}
	cmd.PersistentFlags().IntVarP(&o.expiration, "expiration", "e", 0, "set queue message expiration seconds")
	cmd.PersistentFlags().IntVarP(&o.delay, "delay", "d", 0, "set queue message sending delay seconds")
	cmd.PersistentFlags().IntVarP(&o.maxReceive, "max-receive", "r", 0, "set dead-letter max receive count")
	cmd.PersistentFlags().IntVarP(&o.messages, "messages", "i", 1, "set how many messages to send in a batch")
	cmd.PersistentFlags().StringVarP(&o.deadLetter, "dead-letter-queue", "q", "", "set dead-letter queue name")
	cmd.PersistentFlags().StringVarP(&o.metadata, "metadata", "m", "", "set queue message metadata field")

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

	for i := 0; i < o.messages; i++ {
		msg := client.QM().
			SetChannel(o.channel).
			SetBody([]byte(o.body)).
			SetMetadata(o.metadata).
			SetPolicyExpirationSeconds(o.expiration).
			SetPolicyDelaySeconds(o.delay).
			SetPolicyMaxReceiveCount(o.maxReceive).
			SetPolicyMaxReceiveQueue(o.deadLetter)
		utils.Println("Sending Queue Message:")
		printQueueMessage(msg)
		res, err := msg.Send(ctx)
		if err != nil {
			return fmt.Errorf("error sending queue message, %s", err.Error())
		}
		if res != nil {
			utils.Println("Response:")
			printQueueMessageResult(res)
		}
	}

	return nil
}
