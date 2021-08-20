package array

import (
	"context"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	pb "github.com/kubemq-io/protobuf/go"
)

type QueueSender interface {
	SendQueueMessage(ctx context.Context, msg *pb.QueueMessage) *pb.SendQueueMessageResult
}

type QueueSenderFunc func(ctx context.Context, msg *pb.QueueMessage) *pb.SendQueueMessageResult

func (qs QueueSenderFunc) SendQueueMessage(ctx context.Context, msg *pb.QueueMessage) *pb.SendQueueMessageResult {
	return qs(ctx, msg)
}

type QueueSenderMiddleware func(QueueSender) QueueSender

func QueueSenderLogging(l *logging.Logger) QueueSenderMiddleware {
	return func(qs QueueSender) QueueSender {
		return QueueSenderFunc(func(ctx context.Context, msg *pb.QueueMessage) *pb.SendQueueMessageResult {
			l.Debugw("publish message to a queue", "queue", msg.Channel, "client_id", msg.ClientID, "message_id", msg.MessageID, "metadata", msg.Metadata)
			md := qs.SendQueueMessage(ctx, msg)
			l.Debugw("publish message to queue result", "queue", msg.Channel, "client_id", msg.ClientID, "message_id", msg.MessageID, "metadata", msg.Metadata, "sent", md.SentAt, "is_error", md.IsError, "error", md.Error)
			return md
		})
	}
}

func ChainQueueSenders(qs QueueSender, qsm ...QueueSenderMiddleware) QueueSender {
	sender := qs
	for _, middleware := range qsm {
		sender = middleware(sender)
	}
	return sender
}
