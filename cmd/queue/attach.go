package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
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

var queueAttachExamples = `
	# Attach to all active 'queues' channels and output running messages
	kubemq queue attach all
	
	# Attach to some-queue queue channel and output running messages
	kubemq queue attach some-queue

	# Attach to some-queue1 and some-queue2 queue channels and output running messages
	kubemq queue attach some-queue1 some-queue2 

	# Attach to some-queue queue channel and output running messages filter by include regex (some*)
	kubemq queue attach some-queue -i some*

	# Attach to some-queue queue channel and output running messages filter by exclude regex (not-some*)
	kubemq queue attach some-queue -e not-some*
`
var queueAttachLong = `Queues attach command allows to display 'queues' channel content for debugging proposes`
var queueAttachShort = `Queues attach to 'queues' channels command`

func NewCmdQueueAttach(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &AttachOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "attach",
		Aliases: []string{"a", "att", "at"},
		Short:   queueAttachShort,
		Long:    queueAttachLong,
		Example: queueAttachExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}
	cmd.PersistentFlags().StringArrayVarP(&o.include, "include", "i", []string{}, "aet (regex) strings to include")
	cmd.PersistentFlags().StringArrayVarP(&o.exclude, "exclude", "e", []string{}, "set (regex) strings to exclude")
	return cmd
}

func (o *AttachOptions) Complete(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("missing channel argument")

	}
	if len(args) == 1 && args[0] == "all" {
		utils.Println("retrieve all active 'queues' channels list...")
		resp := &Response{}
		queues := &Queues{}

		r, err := resty.New().R().SetResult(resp).SetError(resp).Get(fmt.Sprintf("%s/v1/stats/queues", o.cfg.Client.ApiAddress))
		if err != nil {
			return err
		}
		if !r.IsSuccess() {
			return fmt.Errorf("not available in current Kubemq version, consider upgrade Kubemq version")
		}
		if resp.Error {
			return fmt.Errorf(resp.ErrorString)
		}
		err = json.Unmarshal(resp.Data, queues)
		if err != nil {
			return err
		}
		utils.Printlnf("found %d active 'queues' channels.", queues.Total)
		for _, q := range queues.Queues {
			rsc := fmt.Sprintf("queue/%s", q.Name)
			o.resources = append(o.resources, rsc)
			utils.Printlnf("adding '%s' to attach list", q.Name)
		}
		return nil
	}
	for _, a := range args {
		rsc := fmt.Sprintf("queue/%s", a)
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
