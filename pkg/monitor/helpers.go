package monitor

import (
	pb "github.com/kubemq-io/protobuf/go"
)

func responseToMessage(r *pb.Response, channel string) *pb.Event {
	data, _ := r.Marshal()
	return &pb.Event{
		ClientID: "monitor-client-id",
		Channel:  channel,
		Metadata: "response",
		Body:     data}
}
