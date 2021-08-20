package array

import (
	"context"

	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/pkg/monitor"
	pb "github.com/kubemq-io/protobuf/go"
)

type CommandSender interface {
	SendCommand(ctx context.Context, req *pb.Request) (*pb.Response, error)
}

type CommandSenderFunc func(ctx context.Context, req *pb.Request) (*pb.Response, error)

func (csf CommandSenderFunc) SendCommand(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	return csf(ctx, req)
}

type CommandSenderMiddleware func(CommandSender) CommandSender

func CommandSenderLogging(l *logging.Logger) CommandSenderMiddleware {
	return func(cs CommandSender) CommandSender {
		return CommandSenderFunc(func(ctx context.Context, req *pb.Request) (*pb.Response, error) {
			l.Debugw("send command rpc request", "channel", req.Channel, "client_id", req.ClientID, "request_id", req.RequestID, "metadata", req.Metadata)
			res, err := cs.SendCommand(ctx, req)
			if err != nil {
				l.Errorw("send command rpc request error", "channel", req.Channel, "client_id", req.ClientID, "request_id", req.RequestID, "error", err.Error())
			} else {
				l.Debugw("send command rpc request result", "channel", req.Channel, "request_client_id", req.ClientID, "response_client_id", res.ClientID, "request_id", res.RequestID, "executed", res.Executed)
			}
			return res, err
		})
	}
}

func CommandSenderMonitor(mm *monitor.Middleware) CommandSenderMiddleware {
	return func(cs CommandSender) CommandSender {
		return CommandSenderFunc(func(ctx context.Context, req *pb.Request) (*pb.Response, error) {
			res, err := cs.SendCommand(ctx, req)
			mm.CheckAndSendCommand(ctx, req, res, err)
			return res, err
		})
	}
}

func ChainCommandSender(cs CommandSender, csm ...CommandSenderMiddleware) CommandSender {
	sender := cs
	for _, middleware := range csm {
		sender = middleware(sender)
	}
	return sender
}
