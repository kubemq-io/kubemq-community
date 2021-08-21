package array

import (
	"context"
	pb "github.com/kubemq-io/protobuf/go"

	"github.com/kubemq-io/kubemq-community/pkg/logging"
)

type AckAllQueueMessages interface {
	AckAllQueueMessages(ctx context.Context, req *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error)
}

type AckAllQueueMessagesFunc func(ctx context.Context, req *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error)

func (aqf AckAllQueueMessagesFunc) AckAllQueueMessages(ctx context.Context, req *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error) {
	return aqf(ctx, req)
}

type AckAllQueueMessagesMiddleware func(AckAllQueueMessages) AckAllQueueMessages

func AckAllQueueMessagesLogging(l *logging.Logger) AckAllQueueMessagesMiddleware {
	return func(aq AckAllQueueMessages) AckAllQueueMessages {
		return AckAllQueueMessagesFunc(func(ctx context.Context, req *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error) {
			l.Infow("request ack all messages in a queue", "queue", req.Channel, "client_id", req.ClientID, "request_id", req.RequestID)
			md, err := aq.AckAllQueueMessages(ctx, req)
			if err != nil {
				l.Errorw("request ack all messages in a queue error", "queue", req.Channel, "client_id", req.ClientID, "request_id", "error", err.Error())
			} else {
				l.Infow("request ack all messages in a queue result", "queue", req.Channel, "client_id", req.ClientID, "request_id", req.RequestID, "affected", md.AffectedMessages, "is_error", md.IsError, "error", md.Error)
			}
			return md, err
		})
	}
}

func ChainAckAllQueueMessages(aq AckAllQueueMessages, aqm ...AckAllQueueMessagesMiddleware) AckAllQueueMessages {
	sender := aq
	for _, middleware := range aqm {
		sender = middleware(sender)
	}
	return sender
}
