package actions

import (
	"fmt"
	"strings"
)

type SendCQRSMessageRequest struct {
	RequestId    string `json:"requestId"`
	Channel      string `json:"channel"`
	Metadata     string `json:"metadata,omitempty"`
	Body         any    `json:"body"`
	Tags         string `json:"tags,omitempty"`
	IsCommands   bool   `json:"isCommands"`
	Timeout      int    `json:"timeout"`
	tagsKeyValue map[string]string
}

func (r *SendCQRSMessageRequest) TagsKeyValue() map[string]string {
	return r.tagsKeyValue
}

func NewSendCQRSMessageRequest() *SendCQRSMessageRequest {
	return &SendCQRSMessageRequest{
		tagsKeyValue: make(map[string]string),
	}
}

func (r *SendCQRSMessageRequest) ParseRequest(req *Request) error {
	if req.Type != "send_cqrs_message_request" {
		return fmt.Errorf("request type %s must be %s", req.Type, "send_cqrs_message_request")
	}

	if req.Data == nil {
		return fmt.Errorf("send cqrs message request data not found")
	}

	err := transformJsonMapToStruct(req.Data, r)
	if err != nil {
		return fmt.Errorf("error unmarshalling send cqrs message request data: %s", err.Error())
	}

	return r.Validate()
}

func (r *SendCQRSMessageRequest) Validate() error {
	if r.Channel == "" {
		return fmt.Errorf("channel not set")
	}
	if r.Body == nil {
		return fmt.Errorf("body not set")
	}
	if r.Tags != "" {
		parts := strings.Split(r.Tags, ",")
		for _, part := range parts {
			keyValue := strings.Split(part, "=")
			if len(keyValue) != 2 {
				return fmt.Errorf("invalid tag %s", part)
			}
			r.tagsKeyValue[keyValue[0]] = keyValue[1]
		}
	}
	if r.Timeout <= 0 {
		return fmt.Errorf("timeout must be greater than 0")
	}
	return nil
}
