package root

import (
	"context"
	"github.com/kubemq-io/kubemq-community/cmd/commands"
	"github.com/kubemq-io/kubemq-community/cmd/events"
	"github.com/kubemq-io/kubemq-community/cmd/events_store"
	"github.com/kubemq-io/kubemq-community/cmd/queries"
	"github.com/kubemq-io/kubemq-community/cmd/queue"
	"github.com/kubemq-io/kubemq-community/cmd/server"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/pkg/utils"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use: "kubemq",
}

func Execute(version string, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := config.GetAppConfig("./", "./config")
	err := cfg.Validate()
	if err != nil {
		return err
	}
	cfg.SetVersion(version)
	logging.CreateLoggerFactory(ctx, cfg.Host, cfg.Log)
	rootCmd.Version = version
	rootCmd.AddCommand(server.NewCmdServer(ctx, cfg))
	rootCmd.AddCommand(commands.NewCmdCommands(ctx, cfg))
	rootCmd.AddCommand(queries.NewCmdQueries(ctx, cfg))
	rootCmd.AddCommand(events.NewCmdEvents(ctx, cfg))
	rootCmd.AddCommand(events_store.NewCmdEventsStore(ctx, cfg))
	rootCmd.AddCommand(queue.NewCmdQueue(ctx, cfg))
	utils.CheckErr(rootCmd.Execute())
	return nil
}
