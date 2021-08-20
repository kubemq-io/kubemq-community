package monitor

import (
	"encoding/json"
	pb "github.com/kubemq-io/protobuf/go"
)

type ResponseError struct {
	Kind      string `json:"kind"`
	RequestID string `json:"request_id"`
	Error     string `json:"error"`
}

func NewResponseErrorFromRequest(v *pb.Request, err error) *ResponseError {
	return &ResponseError{
		Kind:      "error",
		RequestID: v.RequestID,
		Error:     err.Error(),
	}
}
func (re *ResponseError) ToMessage(channel string) *pb.Event {
	data, _ := json.Marshal(re)
	return &pb.Event{
		ClientID: "internal-response-err",
		Channel:  channel,
		Metadata: "response_error",
		Body:     data,
	}
}

func (re *ResponseError) JsonString() string {
	data, _ := json.Marshal(re)
	return string(data)
}
func NewTransportFromResponseError(v *ResponseError) *Transport {
	t := NewTransport()
	t.Kind = "response_error"
	t.BodySize = float32(len(v.Error)) / 1e3
	return t
}
