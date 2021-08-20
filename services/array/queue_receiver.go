package array

import (
	"context"
	pb "github.com/kubemq-io/protobuf/go"

	"github.com/kubemq-io/kubemq-community/pkg/logging"
)

type QueueReceiver interface {
	ReceiveQueueMessages(ctx context.Context, req *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error)
}

type QueueReceiverFunc func(ctx context.Context, req *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error)

func (qr QueueReceiverFunc) ReceiveQueueMessages(ctx context.Context, req *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error) {
	return qr(ctx, req)
}

type QueueReceiverMiddleware func(QueueReceiver) QueueReceiver

func QueueReceiverLogging(l *logging.Logger) QueueReceiverMiddleware {
	return func(qr QueueReceiver) QueueReceiver {
		return QueueReceiverFunc(func(ctx context.Context, req *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error) {
			l.Debugw("receive messages from a queue", "queue", req.Channel, "client_id", req.ClientID, "request_id", req.RequestID, "count", req.MaxNumberOfMessages, "peek", req.IsPeak)
			md, err := qr.ReceiveQueueMessages(ctx, req)
			if err != nil {
				l.Errorw("receive messages from a queue error", "queue", req.Channel, "client_id", req.ClientID, "request_id", req.RequestID, "error", err.Error())
			} else {
				l.Debugw("receive messages from a queue result", "queue", req.Channel, "client_id", req.ClientID, "request_id", req.RequestID, "messages", md.MessagesReceived, "expired", md.MessagesExpired, "is_error", md.IsError, "error", md.Error)
			}
			return md, err
		})
	}
}

func ChainQueueReceivers(qr QueueReceiver, qrm ...QueueReceiverMiddleware) QueueReceiver {
	sender := qr
	for _, middleware := range qrm {
		sender = middleware(sender)
	}
	return sender
}
