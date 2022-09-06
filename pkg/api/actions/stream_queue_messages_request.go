package actions

import (
	"encoding/json"
	"fmt"
)

// create RequestTYpe consts

type StreamQueueMessagesRequestType string

const (
	PollQueueMessagesRequestType   StreamQueueMessagesRequestType = "stream_queue_messages"
	AckQueueMessagesRequestType    StreamQueueMessagesRequestType = "ack_queue_messages"
	RejectQueueMessagesRequestType StreamQueueMessagesRequestType = "reject_queue_messages"
)

type StreamQueueMessagesRequest struct {
	RequestType       StreamQueueMessagesRequestType `json:"requestType"`
	Channel           string                         `json:"channel"`
	VisibilitySeconds int                            `json:"visibilitySeconds"`
	WaitSeconds       int                            `json:"waitSeconds"`
	RefSequence       int64                          `json:"refSequence"`
}

func NewStreamQueueMessagesRequest() *StreamQueueMessagesRequest {
	return &StreamQueueMessagesRequest{}
}

func (r *StreamQueueMessagesRequest) ParseRequest(data []byte) error {
	err := json.Unmarshal([]byte(data), r)
	if err != nil {
		return fmt.Errorf("error unmarshalling request: %s", err.Error())
	}
	return r.Validate()
}

func (r *StreamQueueMessagesRequest) Validate() error {
	switch r.RequestType {
	case PollQueueMessagesRequestType:
		if r.Channel == "" {
			return fmt.Errorf("channel not set")
		}
		if r.VisibilitySeconds <= 0 {
			return fmt.Errorf("visibilitySeconds not set")
		}
		if r.WaitSeconds <= 0 {
			return fmt.Errorf("waitSeconds not set")
		}
	case AckQueueMessagesRequestType, RejectQueueMessagesRequestType:
		if r.RefSequence <= 0 {
			return fmt.Errorf("refSequence not set")
		}
	default:
		return fmt.Errorf("invalid request type")
	}
	return nil
}
