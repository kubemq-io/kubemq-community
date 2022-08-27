package actions

import (
	"time"
)

type SubscribePubSubMessage struct {
	MessageId string `json:"messageId"`
	Metadata  string `json:"metadata"`
	Body      any    `json:"body"`
	Timestamp string `json:"timestamp"`
	Tags      string `json:"tags,omitempty"`
	Sequence  int64  `json:"sequence,omitempty"`
}

func NewSubscribePubSubMessage() *SubscribePubSubMessage {
	return &SubscribePubSubMessage{}
}

func (m *SubscribePubSubMessage) SetMessageId(messageId string) *SubscribePubSubMessage {
	m.MessageId = messageId
	return m
}

func (m *SubscribePubSubMessage) SetMetadata(metadata string) *SubscribePubSubMessage {
	m.Metadata = metadata
	return m
}

func (m *SubscribePubSubMessage) SetBody(body any) *SubscribePubSubMessage {
	m.Body = body
	return m
}

func (m *SubscribePubSubMessage) SetTimestamp(value time.Time) *SubscribePubSubMessage {
	m.Timestamp = value.Format("2006-01-02 15:04:05")
	return m
}

func (m *SubscribePubSubMessage) SetTags(tags string) *SubscribePubSubMessage {
	m.Tags = tags
	return m
}

func (m *SubscribePubSubMessage) SetSequence(sequence int64) *SubscribePubSubMessage {
	m.Sequence = sequence
	return m
}
