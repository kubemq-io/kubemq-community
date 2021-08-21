package queue

import (
	"context"
	"fmt"
	"github.com/AlecAivazis/survey/v2"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/utils"
	"github.com/spf13/cobra"
	"strconv"
	"strings"
)

type StreamOptions struct {
	cfg        *config.Config
	channel    string
	visibility int
	wait       int
}

var queueStreamExamples = `
	# Stream 'queues' message in transaction mode
	kubemq queue stream q1

	# Stream 'queues' message in transaction mode with visibility set to 120 seconds and wait time of 180 seconds
	kubemq queue stream q1 -v 120 -w 180
`
var queueStreamLong = `Queues stream command allows to receive message from a queue in push mode response an appropriate action`
var queueStreamShort = `Queues stream a message from a queue command`

func NewCmdQueueStream(ctx context.Context, cfg *config.Config) *cobra.Command {
	o := &StreamOptions{
		cfg: cfg,
	}
	cmd := &cobra.Command{

		Use:     "stream",
		Aliases: []string{"st"},
		Short:   queueStreamShort,
		Long:    queueStreamLong,
		Example: queueStreamExamples,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			utils.CheckErr(o.Complete(args), cmd)
			utils.CheckErr(o.Validate())
			utils.CheckErr(o.Run(ctx))
		},
	}
	cmd.PersistentFlags().IntVarP(&o.visibility, "visibility", "v", 30, "set initial visibility seconds")
	cmd.PersistentFlags().IntVarP(&o.wait, "wait", "w", 60, "set how many seconds to wait for 'queues' messages")

	return cmd
}

func (o *StreamOptions) Complete(args []string) error {
	if len(args) >= 1 {
		o.channel = args[0]
		return nil
	}
	return fmt.Errorf("missing channel argument")
}

func (o *StreamOptions) Validate() error {
	return nil
}

func (o *StreamOptions) Run(ctx context.Context) error {
	client, err := utils.GetKubeMQClient(ctx, o.cfg)
	if err != nil {
		return fmt.Errorf("create kubemq client, %s", err.Error())
	}

	defer func() {
		_ = client.Close()
	}()

	for {
		stream := client.NewStreamQueueMessage().SetChannel(o.channel)
		utils.Printlnf("waiting for the message in the queue: (waiting for %d seconds, visibility set to %d seconds)", o.wait, o.visibility)
		msg, err := stream.Next(ctx, int32(o.visibility), int32(o.wait))
		if err != nil {
			return err
		}
		utils.Printlnf("[channel: %s] [client id: %s] -> {id: %s, metadata: %s, body: %s}", msg.Channel, msg.ClientID, msg.MessageID, msg.Metadata, msg.Body)
	PROMPT:
		action, result, err := o.prompt()
		if err != nil {
			return err
		}
		switch action {
		case "Ack":
			err := msg.Ack()
			if err != nil {
				return err
			}
			utils.Println("Message Acked")
		case "Reject":
			err := msg.Reject()
			if err != nil {
				return err
			}
			utils.Println("Message Rejected")
		case "Extend visibility":
			val, err := strconv.Atoi(result)
			if err != nil {
				return err
			}
			err = msg.ExtendVisibility(int32(val))
			if err != nil {
				return err
			}
			utils.Printlnf("Visibility Extended By %s Seconds.", result)
			goto PROMPT
		case "Resend to another queue":
			err = msg.Resend(result)
			if err != nil {
				return err
			}
			utils.Printlnf("Message Resent to %s.", result)
		case "Ack and send new message":
			pair := strings.Split(result, ",")
			if len(pair) != 2 {
				return fmt.Errorf("invalid queue-name,message-body format")
			}
			newMessage := client.QM().SetChannel(pair[0]).SetBody([]byte(pair[1]))
			err := stream.ResendWithNewMessage(newMessage)
			if err != nil {
				return err
			}
			utils.Println("New Message Sent.")
		case "Abort":
			utils.Println("Aborting.")
			return nil
		}

	}

}
func (o *StreamOptions) prompt() (string, string, error) {
	action := ""
	prompt := &survey.Select{
		Message: "What next:",
		Options: []string{"Ack", "Reject", "Extend visibility", "Resend to another queue", "Ack and send new message", "Abort"},
	}
	err := survey.AskOne(prompt, &action)
	if err != nil {
		return "", "", err
	}
	switch action {
	case "Ack", "Reject", "Abort":
		return action, "", nil
	case "Extend visibility":
		visibility := ""
		prompt := &survey.Input{
			Renderer: survey.Renderer{},
			Message:  "How long to extend visibility",
			Default:  "60",
			Help:     "In seconds",
		}
		err := survey.AskOne(prompt, &visibility)
		if err != nil {
			return "", "", err
		}
		return action, visibility, nil
	case "Resend to another queue":
		queueName := ""
		prompt := &survey.Input{
			Renderer: survey.Renderer{},
			Message:  "New queue name:",
			Default:  "new-queue",
			Help:     "",
		}
		err := survey.AskOne(prompt, &queueName, survey.WithValidator(survey.MinLength(1)))
		if err != nil {
			return "", "", err
		}
		return action, queueName, nil
	case "Ack and send new message":
		newMessage := ""
		prompt := &survey.Input{
			Renderer: survey.Renderer{},
			Message:  "New Message:",
			Default:  "new-queue,new-message",
			Help:     "Format queue-name,message-body ",
		}
		err := survey.AskOne(prompt, &newMessage, survey.WithValidator(survey.MinLength(1)))
		if err != nil {
			return "", "", err
		}
		return action, newMessage, nil
	}
	return "", "", fmt.Errorf("invalid input")
}
