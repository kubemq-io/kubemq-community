package commands

import (
	"context"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/utils"
	"github.com/spf13/cobra"
)

var commandsExamples = `
	# Execute send commands 
	kubemq commands send 

	# Execute receive commands
	kubemq commands receive 

	# Execute attach to 'commands' channel
	kubemq commands attach 
`
var commandsLong = `Execute Kubemq 'commands `
var commandsShort = `Execute Kubemq 'commands'`

func NewCmdCommands(ctx context.Context, cfg *config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:       "commands",
		Aliases:   []string{"cmd", "command"},
		Short:     commandsShort,
		Long:      commandsLong,
		Example:   commandsExamples,
		ValidArgs: []string{"send", "receive", "attach"},
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(cmd.Help())
		},
	}
	cmd.AddCommand(NewCmdCommandsSend(ctx, cfg))
	cmd.AddCommand(NewCmdCommandsReceive(ctx, cfg))
	cmd.AddCommand(NewCmdCommandsAttach(ctx, cfg))
	return cmd
}
