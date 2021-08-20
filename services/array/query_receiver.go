package array

import (
	"context"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	pb "github.com/kubemq-io/protobuf/go"
)

type QueryReceiver interface {
	SubscribeToQueries(ctx context.Context, subReq *pb.Subscribe, rx chan *pb.Request) error
}

type QueryReceiverFunc func(ctx context.Context, subReq *pb.Subscribe, rx chan *pb.Request) error

func (qsf QueryReceiverFunc) SubscribeToQueries(ctx context.Context, subReq *pb.Subscribe, rx chan *pb.Request) error {
	return qsf(ctx, subReq, rx)
}

type QueryReceiverMiddleware func(QueryReceiver) QueryReceiver

func QueryReceiverLogging(l *logging.Logger) QueryReceiverMiddleware {
	return func(qr QueryReceiver) QueryReceiver {
		return QueryReceiverFunc(func(ctx context.Context, subReq *pb.Subscribe, rx chan *pb.Request) error {
			l.Debugw("subscribe to a pb.Subscribe_Queries rpc requests", "channel", subReq.Channel, "group", subReq.Group, "client_id", subReq.ClientID)
			err := qr.SubscribeToQueries(ctx, subReq, rx)
			if err != nil {
				l.Errorw("subscribe to a pb.Subscribe_Queries rpc requests error", "channel", subReq.Channel, "group", subReq.Group, "client_id", subReq.ClientID, "error", err.Error())
			} else {
				l.Debugw("subscribe to a pb.Subscribe_Queries rpc requests completed", "channel", subReq.Channel, "group", subReq.Group, "client_id", subReq.ClientID)
			}
			return err
		})
	}
}

func ChainQueryReceiver(qr QueryReceiver, qrm ...QueryReceiverMiddleware) QueryReceiver {
	receiver := qr
	for _, middleware := range qrm {
		receiver = middleware(receiver)
	}
	return receiver
}
