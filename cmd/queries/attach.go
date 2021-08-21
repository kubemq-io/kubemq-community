package queries

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

var queriesAttachExamples = `
	# attach to all 'queries' channels and output running messages
	kubemq queries attach *
	
	# attach to some-query 'queries' channel and output running messages
	kubemq queries attach some-query

	# attach to some-queries1 and some-queries2 'queries' channels and output running messages
	kubemq queries attach some-queries1 some-queries2 

	# attach to some-queries 'queries' channel and output running messages filter by include regex (some*)
	kubemq queries attach some-queries -i some*

	# attach to some-queries 'queries' channel and output running messages filter by exclude regex (not-some*)
	kubemq queries attach some-queries -e not-some*
`
var queriesAttachLong = `Attach command allows to display 'queries' channel content for debugging proposes`
var queriesAttachShort = `Attach to 'queries' channels command`

func NewCmdQueriesAttach(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &AttachOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "attach",
		Aliases: []string{"a", "att", "at"},
		Short:   queriesAttachShort,
		Long:    queriesAttachLong,
		Example: queriesAttachExamples,
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
