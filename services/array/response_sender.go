package array

import (
	"context"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	pb "github.com/kubemq-io/protobuf/go"
)

type ResponseSender interface {
	SendResponse(ctx context.Context, res *pb.Response) error
}

type ResponseSenderFunc func(ctx context.Context, res *pb.Response) error

func (rsf ResponseSenderFunc) SendResponse(ctx context.Context, res *pb.Response) error {
	return rsf(ctx, res)
}

type ResponseSenderMiddleware func(ResponseSender) ResponseSender

func ResponseSenderLogging(l *logging.Logger) ResponseSenderMiddleware {
	return func(rs ResponseSender) ResponseSender {
		return ResponseSenderFunc(func(ctx context.Context, res *pb.Response) error {
			l.Debugw("send response for a rpc request", "client_id", res.ClientID, "request_id", res.RequestID, "metadata", res.Metadata)
			err := rs.SendResponse(ctx, res)
			if err != nil {
				l.Debugw("send response for a rpc request error", "client_id", res.ClientID, "request_id", res.RequestID, "error", err.Error())
			}
			return err
		})
	}
}

func ChainResponseSender(rs ResponseSender, rsm ...ResponseSenderMiddleware) ResponseSender {
	sender := rs
	for _, middleware := range rsm {
		sender = middleware(sender)
	}
	return sender
}
