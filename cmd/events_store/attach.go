package events_store

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/utils"
	"github.com/spf13/cobra"
)

type AttachOptions struct {
	cfg       *config.Config
	include   []string
	exclude   []string
	resources []string
}

var eventsAttachExamples = `
	# Attach to all events store channels and output running messages
	kubemq events_store attach *
	
	# Attach to some-events-store 'events store' channel and output running messages
	kubemq events_store attach some-events-store

	# Attach to some-events-store1 and some-events-store2 'events store' channels and output running messages
	kubemq events_store attach some-events-store1 some-events-store2 

	# Attach to some-events-store 'events store' channel and output running messages filter by include regex (some*)
	kubemq events_store attach some-events -i some*

	# Attach to some-events-store 'events store' channel and output running messages filter by exclude regex (not-some*)
	kubemq events_store attach some-events -e not-some*
`
var eventsAttachLong = `Attach command allows to display 'events_store' channel content for debugging proposes`
var eventsAttachShort = `Attach to events store channels command`

func NewCmdEventsStoreAttach(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &AttachOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "attach",
		Aliases: []string{"a", "att", "at"},
		Short:   eventsAttachShort,
		Long:    eventsAttachLong,
		Example: eventsAttachExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}
	cmd.PersistentFlags().StringArrayVarP(&o.include, "include", "i", []string{}, "set (regex) strings to include")
	cmd.PersistentFlags().StringArrayVarP(&o.exclude, "exclude", "e", []string{}, "set (regex) strings to exclude")
	return cmd
}

func (o *AttachOptions) Complete(args []string) error {

	if len(args) == 0 {
		return fmt.Errorf("missing channel argument")
	}

	for _, a := range args {
		rsc := fmt.Sprintf("commands/%s", a)
		o.resources = append(o.resources, rsc)
		utils.Printlnf("adding '%s' to attach list", a)
	}
	return nil
}
func (o *AttachOptions) Validate() error {
	return nil
}

func (o *AttachOptions) Run(ctx context.Context) error {
	err := utils.Run(ctx, o.cfg, o.resources, o.include, o.exclude)
	if err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}
