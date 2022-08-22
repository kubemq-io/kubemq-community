package actions

import (
	"encoding/json"
	"fmt"
)

type ReceiveQueueMessagesRequest struct {
	Channel string `json:"channel"`
	IsPeek  bool   `json:"isPeek"`
	Count   int    `json:"count"`
}

func NewReceiveQueueMessagesRequest() *ReceiveQueueMessagesRequest {
	return &ReceiveQueueMessagesRequest{}
}

func (r *ReceiveQueueMessagesRequest) ParseRequest(req *Request) error {
	if req.Type != "receive_queue_messages" {
		return fmt.Errorf("request type %s must be %s", req.Type, "receive_queue_messages")
	}

	if req.Data == nil {
		return fmt.Errorf("receive queue messages request data not found")
	}

	err := json.Unmarshal(req.Data, r)
	if err != nil {
		return fmt.Errorf("error unmarshalling receive queue messages request data: %s", err.Error())
	}

	return r.Validate()
}

func (r *ReceiveQueueMessagesRequest) Validate() error {
	if r.Channel == "" {
		return fmt.Errorf("channel not set")
	}
	if r.Count < 0 {
		return fmt.Errorf("count must be greater than 0")
	}
	return nil
}
