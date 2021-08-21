package server

import (
	"context"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/interfaces"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/pkg/utils"
	"github.com/kubemq-io/kubemq-community/services"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

type Options struct {
	cfg *config.Config
}

func NewCmdServer(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &Options{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "server",
		Aliases: []string{"s", "serve"},
		Short:   "Start kubemq server",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Run(ctx))
		},
	}

	return cmd
}

func (o *Options) Complete(args []string) error {
	return nil
}

func (o *Options) Validate() error {
	return nil
}

func (o *Options) Run(ctx context.Context) error {
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	logger := logging.GetLogFactory().NewLogger("server")
	logger.Infof("starting kubemq community edition version: %s, cores: %d", o.cfg.GetVersion(), runtime.NumCPU())
	srvs, err := services.Start(ctx, o.cfg)
	if err != nil {
		return err
	}
	ifcs, err := interfaces.StartInterfaces(srvs, o.cfg)
	if err != nil {
		return err
	}
	<-gracefulShutdown
	logger.Warnw("kubemq server shutdown started")
	if ifcs != nil {
		ifcs.Close()
	}
	srvs.Close()
	<-srvs.Stopped
	logger.Warnw("kubemq server shutdown completed")
	logging.Close()
	return nil
}
