package queue

import (
	"context"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/utils"
	"github.com/spf13/cobra"
)

var queueExamples = `
	# Execute send 'queues' command
	kubemqctl queues send

	# Execute attached to 'queues' command
	kubemqctl queues attach

	# Execute receive 'queues' command
	kubemqctl queues receive
	
	# Execute list 'queues' command
	kubemqctl queues list

	# Execute peek 'queues' command
	kubemqctl queues peek

	# Execute ack 'queues' command
	 kubemqctl queues ack

	# Execute stream 'queues' command
	kubemqctl queues stream
`
var queueLong = `Execute Kubemq 'queues' commands`
var queueShort = `Execute Kubemq 'queues' commands`

func NewCmdQueue(ctx context.Context, cfg *config.Config) *cobra.Command {

	cmd := &cobra.Command{
		Use:       "queues",
		Aliases:   []string{"q", "qu", "queue"},
		Short:     queueShort,
		Long:      queueLong,
		Example:   queueExamples,
		ValidArgs: []string{"send", "receive", "attach", "peek", "ack", "list", "stream"},
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(cmd.Help())
		},
	}
	cmd.AddCommand(NewCmdQueueSend(ctx, cfg))
	cmd.AddCommand(NewCmdQueueReceive(ctx, cfg))
	cmd.AddCommand(NewCmdQueuePeek(ctx, cfg))
	cmd.AddCommand(NewCmdQueueAck(ctx, cfg))
	cmd.AddCommand(NewCmdQueueList(ctx, cfg))
	cmd.AddCommand(NewCmdQueueStream(ctx, cfg))
	cmd.AddCommand(NewCmdQueueAttach(ctx, cfg))

	return cmd
}
