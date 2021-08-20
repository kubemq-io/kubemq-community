package array

import (
	"context"
	pb "github.com/kubemq-io/protobuf/go"

	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/pkg/monitor"
)

type QuerySender interface {
	SendQuery(ctx context.Context, req *pb.Request) (*pb.Response, error)
}

type QuerySenderFunc func(ctx context.Context, req *pb.Request) (*pb.Response, error)

func (qsf QuerySenderFunc) SendQuery(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	return qsf(ctx, req)
}

type QuerySenderMiddleware func(QuerySender) QuerySender


func QuerySenderLogging(l *logging.Logger) QuerySenderMiddleware {
	return func(qs QuerySender) QuerySender {
		return QuerySenderFunc(func(ctx context.Context, req *pb.Request) (*pb.Response, error) {
			l.Debugw("send query a rpc request", "channel", req.Channel, "client_id", req.ClientID, "request_id", req.RequestID, "metadata", req.Metadata)
			res, err := qs.SendQuery(ctx, req)
			if err != nil {
				l.Errorw("send query a rpc request error", "channel", req.Channel, "client_id", req.ClientID, "request_id", req.RequestID, "error", err.Error())
			} else {
				l.Debugw("send query a rpc request result", "channel", req.Channel, "request_client_id", req.ClientID, "response_client_id", res.ClientID, "request_id", res.RequestID, "executed", res.Executed)
			}
			return res, err
		})
	}
}

func QuerySenderMonitor(mm *monitor.Middleware) QuerySenderMiddleware {
	return func(qs QuerySender) QuerySender {
		return QuerySenderFunc(func(ctx context.Context, req *pb.Request) (*pb.Response, error) {
			res, err := qs.SendQuery(ctx, req)
			mm.CheckAndSendQuery(ctx, req, res, err)
			return res, err
		})
	}
}

func ChainQuerySender(qs QuerySender, qsm ...QuerySenderMiddleware) QuerySender {
	sender := qs
	for _, middleware := range qsm {
		sender = middleware(sender)
	}
	return sender
}
