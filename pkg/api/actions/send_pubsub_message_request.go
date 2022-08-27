package actions

import (
	"fmt"
	"strings"
)

type SendPubSubMessageRequest struct {
	MessageId    string `json:"messageId,omitempty"`
	Channel      string `json:"channel"`
	ClientId     string `json:"clientId,omitempty"`
	Metadata     string `json:"metadata,omitempty"`
	Body         any    `json:"body"`
	Tags         string `json:"tags,omitempty"`
	IsEvents     bool   `json:"isEvents,omitempty"`
	tagsKeyValue map[string]string
}

func (r *SendPubSubMessageRequest) TagsKeyValue() map[string]string {
	return r.tagsKeyValue
}

func NewSendPubSubMessageRequest() *SendPubSubMessageRequest {
	return &SendPubSubMessageRequest{
		tagsKeyValue: make(map[string]string),
	}
}

func (r *SendPubSubMessageRequest) ParseRequest(req *Request) error {
	if req.Type != "send_pubsub_message" {
		return fmt.Errorf("request type %s must be %s", req.Type, "send_pubsub_message")
	}

	if req.Data == nil {
		return fmt.Errorf("send pubssub message request data not found")
	}

	err := transformJsonMapToStruct(req.Data, r)
	if err != nil {
		return fmt.Errorf("error unmarshalling send pubssub message request data: %s", err.Error())
	}

	return r.Validate()
}

func (r *SendPubSubMessageRequest) Validate() error {
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
