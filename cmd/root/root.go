package root

import (
	"context"
	"github.com/kubemq-io/kubemq-community/cmd/server"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/pkg/utils"
	"github.com/spf13/cobra"
)



var rootCmd = &cobra.Command{
	Use:       "kubemq",
}


func Execute(version string, args []string) error{
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.GetAppConfig("./","./config")
	err := cfg.Validate()
	if err != nil {
		return err
	}
	cfg.SetVersion(version)
	logging.CreateLoggerFactory(ctx, cfg.Host, cfg.Log)
	rootCmd.Version = version
	rootCmd.AddCommand(server.NewCmdServer(ctx, cfg))
	utils.CheckErr(rootCmd.Execute())
	return nil
}

