package array

import (
	"context"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	pb "github.com/kubemq-io/protobuf/go"
)

type EventsReceiver interface {
	SubscribeToEvents(ctx context.Context, subReq *pb.Subscribe, msgCh chan *pb.EventReceive) error
}

type EventsReceiverFunc func(ctx context.Context, subReq *pb.Subscribe, msgCh chan *pb.EventReceive) error

func (qs EventsReceiverFunc) SubscribeToEvents(ctx context.Context, subReq *pb.Subscribe, msgCh chan *pb.EventReceive) error {
	return qs(ctx, subReq, msgCh)
}

type EventsReceiverMiddleware func(EventsReceiver) EventsReceiver

func EventsReceiverLogging(l *logging.Logger) EventsReceiverMiddleware {
	return func(er EventsReceiver) EventsReceiver {
		return EventsReceiverFunc(func(ctx context.Context, subReq *pb.Subscribe, msgCh chan *pb.EventReceive) error {
			l.Infow("subscribe to a pub/sub events", "channel", subReq.Channel, "group", subReq.Group, "client_id", subReq.ClientID)
			err := er.SubscribeToEvents(ctx, subReq, msgCh)
			if err != nil {
				l.Errorw("subscribe to a pub/sub events error", "channel", subReq.Channel, "group", subReq.Group, "client_id", subReq.ClientID, "error", err.Error())
			} else {
				l.Debugw("subscribe to a pub/sub events completed", "channel", subReq.Channel, "group", subReq.Group, "client_id", subReq.ClientID)
			}
			return err
		})
	}
}

func ChainEventsReceiver(ev EventsReceiver, erm ...EventsReceiverMiddleware) EventsReceiver {
	receiver := ev
	for _, middleware := range erm {
		receiver = middleware(receiver)
	}
	return receiver
}
