package actions

import (
	"time"
)

type SendQueueMessageResponse struct {
	MessageId string `json:"messageId,omitempty"`
	SentAt    string `json:"sentAt,omitempty"`
	ExpiresAt string `json:"expiresAt,omitempty"`
	DelayedTo string `json:"delayedTo,omitempty"`
}

func NewSendQueueMessageResponse() *SendQueueMessageResponse {
	return &SendQueueMessageResponse{}
}

func (r *SendQueueMessageResponse) SetMessageId(messageId string) *SendQueueMessageResponse {
	r.MessageId = messageId
	return r
}

func (r *SendQueueMessageResponse) SetSentAt(sentAt int64) *SendQueueMessageResponse {
	r.SentAt = time.Unix(0, sentAt).Format(time.RFC3339)
	return r
}

func (r *SendQueueMessageResponse) SetExpiresAt(expiresAt int64) *SendQueueMessageResponse {
	if expiresAt == 0 {
		return r
	}
	r.ExpiresAt = time.Unix(0, expiresAt).Format(time.RFC3339)
	return r
}

func (r *SendQueueMessageResponse) SetDelayedTo(delayedTo int64) *SendQueueMessageResponse {
	if delayedTo == 0 {
		return r
	}
	r.DelayedTo = time.Unix(0, delayedTo).Format(time.RFC3339)
	return r
}
