package array

import (
	"context"
	pb "github.com/kubemq-io/protobuf/go"

	"github.com/kubemq-io/kubemq-community/pkg/logging"
)

type QueueBatchSender interface {
	SendQueueMessagesBatch(ctx context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error)
}

type QueueBatchSenderFunc func(ctx context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error)

func (qbsf QueueBatchSenderFunc) SendQueueMessagesBatch(ctx context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error) {
	return qbsf(ctx, req)
}

type QueueBatchSenderMiddleware func(QueueBatchSender) QueueBatchSender

func QueueBatchSenderLogging(l *logging.Logger) QueueBatchSenderMiddleware {
	return func(qbs QueueBatchSender) QueueBatchSender {
		return QueueBatchSenderFunc(func(ctx context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error) {
			for _, msg := range req.Messages {
				l.Debugw("batch publish message to queue", "queue", msg.Channel, "client_id", msg.ClientID, "message_id", msg.MessageID, "metadata", msg.Metadata)
			}
			md, err := qbs.SendQueueMessagesBatch(ctx, req)
			if err != nil {
				l.Errorw("batch publish message to queue", "error", err.Error())
				return nil, err
			}
			for _, result := range md.Results {
				l.Debugw("batch publish message to queue result", "message_id", result.MessageID, "sent", result.SentAt, "is_error", result.IsError, "error", result.Error)
			}
			return md, err
		})
	}
}

func ChainQueueBatchSenders(qbs QueueBatchSender, qbsm ...QueueBatchSenderMiddleware) QueueBatchSender {
	sender := qbs
	for _, middleware := range qbsm {
		sender = middleware(sender)
	}
	return sender
}
