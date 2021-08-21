package array

import (
	"context"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	pb "github.com/kubemq-io/protobuf/go"
)

type EventsStoreReceiver interface {
	SubscribeToEventsStore(ctx context.Context, subReq *pb.Subscribe, msgCh chan *pb.EventReceive) error
}

type EventsStoreReceiverFunc func(ctx context.Context, subReq *pb.Subscribe, msgCh chan *pb.EventReceive) error

func (qs EventsStoreReceiverFunc) SubscribeToEventsStore(ctx context.Context, subReq *pb.Subscribe, msgCh chan *pb.EventReceive) error {
	return qs(ctx, subReq, msgCh)
}

type EventsStoreReceiverMiddleware func(EventsStoreReceiver) EventsStoreReceiver

func EventsStoreReceiverLogging(l *logging.Logger) EventsStoreReceiverMiddleware {
	return func(qs EventsStoreReceiver) EventsStoreReceiver {
		return EventsStoreReceiverFunc(func(ctx context.Context, subReq *pb.Subscribe, msgCh chan *pb.EventReceive) error {
			l.Infow("subscribe to a pub/sub events store", "channel", subReq.Channel, "group", subReq.Group, "client_id", subReq.ClientID, "type", subReq.EventsStoreTypeData, "type_value", subReq.EventsStoreTypeValue)
			err := qs.SubscribeToEventsStore(ctx, subReq, msgCh)
			if err != nil {
				l.Errorw("subscribe to a pub/sub events store error", "channel", subReq.Channel, "group", subReq.Group, "client_id", subReq.ClientID, "error", err.Error())
			} else {
				l.Debugw("subscribe to a pub/sub events store completed", "channel", subReq.Channel, "group", subReq.Group, "client_id", subReq.ClientID)
			}
			return err
		})
	}
}

func ChainEventsStoreReceiver(esr EventsStoreReceiver, esrm ...EventsStoreReceiverMiddleware) EventsStoreReceiver {
	receiver := esr
	for _, middleware := range esrm {
		receiver = middleware(receiver)
	}
	return receiver
}
