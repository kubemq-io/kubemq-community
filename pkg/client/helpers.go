package client

import (
	"fmt"
	"github.com/kubemq-io/broker/client/stan"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	pb "github.com/kubemq-io/protobuf/go"
	"time"
)

func UnmarshalToEventReceive(data []byte) (*pb.EventReceive, error) {
	msg := &pb.Event{}
	err := msg.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	qm := &pb.EventReceive{
		EventID:              msg.EventID,
		Channel:              msg.Channel,
		Metadata:             msg.Metadata,
		Body:                 msg.Body,
		Timestamp:            0,
		Sequence:             0,
		Tags:                 msg.Tags,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}

	return qm, err
}

func ParseSubscriptionRequest(req *pb.Subscribe) []stan.SubscriptionOption {
	var list = []stan.SubscriptionOption{stan.DurableName(fmt.Sprintf("%s-%s", req.Channel, req.Group))}
	switch req.EventsStoreTypeData {

	case pb.Subscribe_StartFromLast:
		list = append(list, stan.StartWithLastReceived())
	case pb.Subscribe_StartNewOnly:
		list = append(list, stan.StartAt(0))
	case pb.Subscribe_StartFromFirst:
		list = append(list, stan.DeliverAllAvailable())
	case pb.Subscribe_StartAtSequence:
		list = append(list, stan.StartAtSequence(uint64(req.EventsStoreTypeValue)))
	case pb.Subscribe_StartAtTime:
		list = append(list, stan.StartAtTimeDelta(time.Since(time.Unix(0, req.EventsStoreTypeValue))))
	case pb.Subscribe_StartAtTimeDelta:
		list = append(list, stan.StartAtTimeDelta(time.Duration(req.EventsStoreTypeValue)*time.Second))

	}
	return list
}

func validateOptions(opts *Options, forQueue bool) error {
	if opts.ClientID == "" {
		return entities.ErrInvalidClientID
	}
	return nil
}

func unmarshalToEventReceive(data []byte) (*pb.EventReceive, error) {
	msg := &pb.Event{}
	err := msg.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	qm := &pb.EventReceive{
		EventID:              msg.EventID,
		Channel:              msg.Channel,
		Metadata:             msg.Metadata,
		Body:                 msg.Body,
		Timestamp:            0,
		Sequence:             0,
		Tags:                 msg.Tags,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}

	return qm, err
}
