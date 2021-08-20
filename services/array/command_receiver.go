package array

import (
	"context"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	pb "github.com/kubemq-io/protobuf/go"
)

type CommandReceiver interface {
	SubscribeToCommands(ctx context.Context, subReq *pb.Subscribe, rx chan *pb.Request) error
}

type CommandReceiverFunc func(ctx context.Context, subReq *pb.Subscribe, rx chan *pb.Request) error

func (crf CommandReceiverFunc) SubscribeToCommands(ctx context.Context, subReq *pb.Subscribe, rx chan *pb.Request) error {
	return crf(ctx, subReq, rx)
}

type CommandReceiverMiddleware func(CommandReceiver) CommandReceiver

func CommandReceiverLogging(l *logging.Logger) CommandReceiverMiddleware {
	return func(cr CommandReceiver) CommandReceiver {
		return CommandReceiverFunc(func(ctx context.Context, subReq *pb.Subscribe, rx chan *pb.Request) error {
			l.Debugw("subscribe to a commands rpc requests", "channel", subReq.Channel, "group", subReq.Group, "client_id", subReq.ClientID)
			err := cr.SubscribeToCommands(ctx, subReq, rx)
			if err != nil {
				l.Errorw("subscribe to a commands rpc requests error", "channel", subReq.Channel, "group", subReq.Group, "client_id", subReq.ClientID, "error", err.Error())
			} else {
				l.Debugw("subscribe to a commands rpc requests completed", "channel", subReq.Channel, "group", subReq.Group, "client_id", subReq.ClientID)
			}
			return err
		})
	}
}

func ChainCommandReceiver(cr CommandReceiver, crm ...CommandReceiverMiddleware) CommandReceiver {
	receiver := cr
	for _, middleware := range crm {
		receiver = middleware(receiver)
	}
	return receiver
}
