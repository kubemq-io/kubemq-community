package array

import (
	"context"
	"github.com/kubemq-io/kubemq-community/pkg/logging"

	pb "github.com/kubemq-io/protobuf/go"
)

type EventsSender interface {
	SendEvents(ctx context.Context, msg *pb.Event) (*pb.Result, error)
}

type EventsSenderFunc func(ctx context.Context, msg *pb.Event) (*pb.Result, error)

func (esf EventsSenderFunc) SendEvents(ctx context.Context, msg *pb.Event) (*pb.Result, error) {
	return esf(ctx, msg)
}

type EventsSenderMiddleware func(EventsSender) EventsSender

func EventsSenderLogging(l *logging.Logger) EventsSenderMiddleware {
	return func(es EventsSender) EventsSender {
		return EventsSenderFunc(func(ctx context.Context, msg *pb.Event) (*pb.Result, error) {
			md, err := es.SendEvents(ctx, msg)
			if err != nil {
				l.Errorw("publish pub/sub event error", "channel", msg.Channel, "client_id", msg.ClientID, "event_id", msg.EventID, "error", err.Error())
			}
			return md, err
		})
	}
}

func ChainEventsSenders(es EventsSender, esm ...EventsSenderMiddleware) EventsSender {
	sender := es
	for _, middleware := range esm {
		sender = middleware(sender)
	}
	return sender
}
