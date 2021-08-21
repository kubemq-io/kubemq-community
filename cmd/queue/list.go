package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/utils"

	"github.com/spf13/cobra"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

type ListOptions struct {
	cfg    *config.Config
	filter string
}

var queueListExamples = `
	# Get a list of queues / clients
	kubemq queue list
	
	# Get a list of queues / clients filtered by 'some-queue' channel only
	kubemq queue list -f some-queue
`
var queueListLong = `Queues list command allows to get a list of 'queues' channels`
var queueListShort = `Queues list of 'queues' channels`

func NewCmdQueueList(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &ListOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "list",
		Aliases: []string{"l"},
		Short:   queueListShort,
		Long:    queueListLong,
		Example: queueListExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}
	cmd.PersistentFlags().StringVarP(&o.filter, "filter", "f", "", "set filter for channel / client name")
	return cmd
}

func (o *ListOptions) Complete(args []string) error {

	return nil
}

func (o *ListOptions) Validate() error {
	return nil
}

func (o *ListOptions) Run(ctx context.Context) error {
	resp := &Response{}
	q := &Queues{}

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
	err = json.Unmarshal(resp.Data, q)
	if err != nil {
		return err
	}
	q.printChannelsTab(o.filter)
	return nil
}

type Response struct {
	Node        string          `json:"node"`
	Error       bool            `json:"error"`
	ErrorString string          `json:"error_string"`
	Data        json.RawMessage `json:"data"`
}

type Queues struct {
	Now    time.Time `json:"now"`
	Total  int       `json:"total"`
	Queues []*Queue  `json:"queues"`
}

type Queue struct {
	Name          string    `json:"name"`
	Messages      int64     `json:"messages"`
	Bytes         int64     `json:"bytes"`
	FirstSequence int64     `json:"first_sequence"`
	LastSequence  int64     `json:"last_sequence"`
	Clients       []*Client `json:"clients"`
}

type Client struct {
	ClientId         string `json:"client_id"`
	Active           bool   `json:"active"`
	LastSequenceSent int64  `json:"last_sequence_sent"`
	IsStalled        bool   `json:"is_stalled"`
	Pending          int64  `json:"pending"`
}

func (q *Queues) printChannelsTab(filter string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.TabIndent)
	fmt.Fprintf(w, "CHANNELS:\n")
	fmt.Fprintln(w, "NAME\tCLIENTS\tMESSAGES\tBYTES\tFIRST_SEQUENCE\tLAST_SEQUENCE")
	cnt := 0
	for _, q := range q.Queues {
		if filter == "" || strings.Contains(q.Name, filter) {
			fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%d\n", q.Name, len(q.Clients), q.Messages, q.Bytes, q.FirstSequence, q.LastSequence)
			cnt++
		}

	}
	fmt.Fprintf(w, "\nTOTAL CHANNELS:\t%d\n", cnt)
	w.Flush()
}
