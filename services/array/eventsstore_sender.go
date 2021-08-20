package array

import (
	"context"

	"github.com/kubemq-io/kubemq-community/pkg/logging"

	pb "github.com/kubemq-io/protobuf/go"
)

type EventsStoreSender interface {
	SendEventsStore(ctx context.Context, msg *pb.Event) (*pb.Result, error)
}

type EventsStoreSenderFunc func(ctx context.Context, msg *pb.Event) (*pb.Result, error)

func (essf EventsStoreSenderFunc) SendEventsStore(ctx context.Context, msg *pb.Event) (*pb.Result, error) {
	return essf(ctx, msg)
}

type EventsStoreSenderMiddleware func(EventsStoreSender) EventsStoreSender

func EventsStoreSenderLogging(l *logging.Logger) EventsStoreSenderMiddleware {
	return func(ess EventsStoreSender) EventsStoreSender {
		return EventsStoreSenderFunc(func(ctx context.Context, msg *pb.Event) (*pb.Result, error) {
			md, err := ess.SendEventsStore(ctx, msg)
			if err != nil {
				l.Errorw("publish pub/sub event store error", "channel", msg.Channel, "client_id", msg.ClientID, "event_id", msg.EventID, "error", err.Error())
			}
			return md, err
		})
	}
}


func ChainEventsStoreSenders(ess EventsStoreSender, essm ...EventsStoreSenderMiddleware) EventsStoreSender {
	sender := ess
	for _, middleware := range essm {
		sender = middleware(sender)
	}
	return sender
}
