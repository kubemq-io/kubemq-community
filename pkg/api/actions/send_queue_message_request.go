package actions

import (
	"encoding/json"
	"fmt"
	"strings"
)

type SendQueueMessageRequest struct {
	MessageId       string `json:"messageId,omitempty"`
	Channel         string `json:"channel"`
	ClientId        string `json:"clientId,omitempty"`
	Metadata        string `json:"metadata,omitempty"`
	Body            any    `json:"body"`
	Tags            string `json:"tags,omitempty"`
	MaxReceiveCount int    `json:"maxReceiveCount,omitempty"`
	MaxReceiveQueue string `json:"maxReceiveQueue,omitempty"`
	ExpirationAt    int    `json:"expirationAt,omitempty"`
	DelayedTo       int    `json:"delayedTo,omitempty"`
	tagsKeyValue    map[string]string
}

func (r *SendQueueMessageRequest) TagsKeyValue() map[string]string {
	return r.tagsKeyValue
}

func NewSendQueueMessageRequest() *SendQueueMessageRequest {
	return &SendQueueMessageRequest{
		tagsKeyValue: make(map[string]string),
	}
}

func (r *SendQueueMessageRequest) ParseRequest(req *Request) error {
	if req.Type != "send_queue_message" {
		return fmt.Errorf("request type %s must be %s", req.Type, "send_queue_message")
	}

	if req.Data == nil {
		return fmt.Errorf("send queue message request data not found")
	}

	err := json.Unmarshal(req.Data, r)
	if err != nil {
		return fmt.Errorf("error unmarshalling send queue message request data: %s", err.Error())
	}

	return r.Validate()
}

func (r *SendQueueMessageRequest) Validate() error {
	if r.Channel == "" {
		return fmt.Errorf("channel not set")
	}
	if r.Body == nil {
		return fmt.Errorf("body not set")
	}
	if r.Tags != "" {
		parts := strings.Split(r.Tags, ",")
		for _, part := range parts {
			keyValue := strings.Split(part, ":")
			if len(keyValue) != 2 {
				return fmt.Errorf("invalid tag %s", part)
			}
			r.tagsKeyValue[keyValue[0]] = keyValue[1]
		}
	}
	return nil
}
