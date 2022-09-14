package actions

import (
	"fmt"
	"strings"
)

type SendCQRSMessageResponse struct {
	RequestId    string `json:"requestId"`
	ReplyChannel string `json:"replyChannel"`
	Metadata     string `json:"metadata,omitempty"`
	Body         any    `json:"body"`
	Tags         string `json:"tags,omitempty"`
	Executed     bool   `json:"executed"`
	Error        string `json:"error,omitempty"`
	tagsKeyValue map[string]string
}

func (r *SendCQRSMessageResponse) TagsKeyValue() map[string]string {
	return r.tagsKeyValue
}
func (r *SendCQRSMessageResponse) GetError() error {
	if r.Error != "" {
		return fmt.Errorf(r.Error)
	}
	return nil
}
func NewSendCQRSMessageResponse() *SendCQRSMessageResponse {
	return &SendCQRSMessageResponse{
		tagsKeyValue: make(map[string]string),
	}
}

func (r *SendCQRSMessageResponse) ParseRequest(req *Request) error {
	if req.Type != "send_cqrs_message_response" {
		return fmt.Errorf("request type %s must be %s", req.Type, "send_cqrs_message_response")
	}

	if req.Data == nil {
		return fmt.Errorf("send cqrs message response data not found")
	}

	err := transformJsonMapToStruct(req.Data, r)
	if err != nil {
		return fmt.Errorf("error unmarshalling send cqrs message response data: %s", err.Error())
	}

	return r.Validate()
}

func (r *SendCQRSMessageResponse) Validate() error {
	if r.ReplyChannel == "" {
		return fmt.Errorf("reply channel not set")
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
