package events_store

import (
	"context"
	"fmt"
	"github.com/AlecAivazis/survey/v2"
	"github.com/kubemq-io/kubemq-community/pkg/utils"
	"github.com/kubemq-io/kubemq-go"

	"github.com/kubemq-io/kubemq-community/config"
	"github.com/spf13/cobra"
	"strconv"
	"time"
)

type ReceiveOptions struct {
	cfg           *config.Config
	channel       string
	group         string
	startNew      bool
	startFirst    bool
	startLast     bool
	startSequence int
	startTime     string
	startDuration string
	subOptions    kubemq.SubscriptionOption
}

var eventsReceiveExamples = `
	# Receive messages from an 'events store' channel (blocks until next body)
	kubemq events_store receive some-channel

	# Receive messages from an 'events channel' with group(blocks until next body)
	kubemq events_store receive some-channel -g G1
`
var eventsReceiveLong = `Events-Store receive (Subscribe) command allows to consume messages from an 'events store' with options to set offset parameters`
var eventsReceiveShort = `Events-Store receive a messages from an 'events store'`

func NewCmdEventsStoreReceive(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &ReceiveOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "receive",
		Aliases: []string{"r", "rec"},
		Short:   eventsReceiveShort,
		Long:    eventsReceiveLong,
		Example: eventsReceiveExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}

	cmd.PersistentFlags().StringVarP(&o.group, "group", "g", "", "set 'events_store' channel consumer group (load balancing)")
	cmd.PersistentFlags().BoolVar(&o.startNew, "start-new", false, "start from new body only")
	cmd.PersistentFlags().BoolVar(&o.startFirst, "start-first", false, "start from first body in the channel")
	cmd.PersistentFlags().BoolVar(&o.startLast, "start-last", false, "start from last body in the channel")
	cmd.PersistentFlags().IntVar(&o.startSequence, "start-sequence", 0, "start from body sequence")
	cmd.PersistentFlags().StringVar(&o.startTime, "start-time", "", "start from timestamp format 2006-01-02 15:04:05")
	cmd.PersistentFlags().StringVar(&o.startDuration, "start-duration", "", "start from time duration i.e. 1h")
	return cmd
}

func (o *ReceiveOptions) Complete(args []string) error {

	if len(args) >= 1 {
		o.channel = args[0]
	} else {
		return fmt.Errorf("missing channel argument")
	}

	if o.startNew {
		o.subOptions = kubemq.StartFromNewEvents()
		return nil
	}
	if o.startFirst {
		o.subOptions = kubemq.StartFromFirstEvent()
		return nil
	}
	if o.startLast {
		o.subOptions = kubemq.StartFromLastEvent()
		return nil
	}
	if o.startSequence > 0 {
		o.subOptions = kubemq.StartFromSequence(o.startSequence)
		return nil
	}
	if o.startTime != "" {
		t, err := time.Parse("2006-01-02 15:04:05", o.startTime)
		if err != nil {
			return fmt.Errorf("start time format error, %s", err.Error())
		}
		o.subOptions = kubemq.StartFromTime(t.UTC())
		return nil
	}
	if o.startDuration != "" {
		d, err := time.ParseDuration(o.startDuration)
		if err != nil {
			return fmt.Errorf("start duration format error, %s", err.Error())
		}
		o.subOptions = kubemq.StartFromTimeDelta(d)
		return nil
	}
	err := o.promptOptions()
	if err != nil {
		return err
	}
	return nil
}

func (o *ReceiveOptions) Validate() error {
	return nil
}

func (o *ReceiveOptions) Run(ctx context.Context) error {
	client, err := utils.GetKubeMQClient(ctx, o.cfg)
	if err != nil {
		return fmt.Errorf("create kubemq client, %s", err.Error())
	}

	defer func() {
		_ = client.Close()
	}()

	errChan := make(chan error, 1)
	eventsChan, err := client.SubscribeToEventsStore(ctx, o.channel, o.group, errChan, o.subOptions)

	if err != nil {
		utils.Println(fmt.Errorf("receive 'events store' messages, %s", err.Error()).Error())
	}
	utils.Println("waiting for 'events store' messages...")

	for {
		select {
		case ev, opened := <-eventsChan:
			if !opened {
				utils.Println("server disconnected")
				return nil
			}
			printEventReceive(ev)
		case err := <-errChan:
			return fmt.Errorf("server disconnected with error: %s", err.Error())
		case <-ctx.Done():
			return nil
		}
	}

}

func (o *ReceiveOptions) promptOptions() error {
	action := ""
	prompt := &survey.Select{
		Message: "Start receive events store messages options:",
		Options: []string{"start from new messages only", "start from first body", "start from last body", "start from sequence", "start from time", "start from duration"},
	}
	err := survey.AskOne(prompt, &action)
	if err != nil {
		return err
	}
	switch action {
	case "start from new messages only":
		o.subOptions = kubemq.StartFromNewEvents()
		return nil
	case "start from first body":
		o.subOptions = kubemq.StartFromFirstEvent()
		return nil
	case "start from last body":
		o.subOptions = kubemq.StartFromLastEvent()
		return nil
	case "start from sequence":
		seqStr := ""
		prompt := &survey.Input{
			Renderer: survey.Renderer{},
			Message:  "Set sequence:",
			Default:  "1",
			Help:     "1 is the first body",
		}

		err := survey.AskOne(prompt, &seqStr)
		if err != nil {
			return err
		}

		seq, err := strconv.Atoi(seqStr)
		if err != nil {
			return err
		}
		o.subOptions = kubemq.StartFromSequence(seq)
		return nil
	case "start from time":
		timeStr := ""
		prompt := &survey.Input{
			Renderer: survey.Renderer{},
			Message:  "Set time (UTC):",
			Default:  time.Now().UTC().Add(-1 * time.Minute).Format("2006-01-02 15:04:05"),
			Help:     "Time format '2006-01-02 15:04:05'",
		}

		err := survey.AskOne(prompt, &timeStr)
		if err != nil {
			return err
		}
		t, err := time.Parse("2006-01-02 15:04:05", timeStr)

		if err != nil {
			return err
		}
		o.subOptions = kubemq.StartFromTime(t.UTC())
		return nil
	case "start from duration":
		durationStr := ""
		prompt := &survey.Input{
			Renderer: survey.Renderer{},
			Message:  fmt.Sprintf("Set duration (Current UTC time: %s):", time.Now().UTC().Format("2006-01-02 15:04:05")),
			Default:  "1h",
			Help:     "Duration time such 1s, 1h, 24h",
		}

		err := survey.AskOne(prompt, &durationStr)
		if err != nil {
			return err
		}
		d, err := time.ParseDuration(durationStr)

		if err != nil {
			return err
		}
		o.subOptions = kubemq.StartFromTimeDelta(d)
		return nil
	}
	return fmt.Errorf("invalid input")
}
