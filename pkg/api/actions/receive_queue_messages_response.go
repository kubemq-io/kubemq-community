package actions

import "time"

type ReceiveQueueMessageResponse struct {
	MessageId     string `json:"messageId"`
	ClientId      string `json:"clientId"`
	Metadata      string `json:"metadata"`
	Body          any    `json:"body"`
	Timestamp     int64  `json:"timestamp"`
	Sequence      int64  `json:"sequence"`
	Tags          string `json:"tags"`
	ReceivedCount int64  `json:"receivedCount"`
	ReRoutedFrom  string `json:"reRoutedFrom"`
	ExpirationAt  int64  `json:"expirationAt"`
	DelayedTo     int64  `json:"delayedTo"`
}

func NewReceiveQueueMessageResponse() *ReceiveQueueMessageResponse {
	return &ReceiveQueueMessageResponse{}
}

func (m *ReceiveQueueMessageResponse) SetMessageId(messageId string) *ReceiveQueueMessageResponse {
	m.MessageId = messageId
	return m
}

func (m *ReceiveQueueMessageResponse) SetClientId(clientId string) *ReceiveQueueMessageResponse {
	m.ClientId = clientId
	return m
}

func (m *ReceiveQueueMessageResponse) SetMetadata(metadata string) *ReceiveQueueMessageResponse {
	m.Metadata = metadata
	return m
}

func (m *ReceiveQueueMessageResponse) SetBody(body any) *ReceiveQueueMessageResponse {
	m.Body = body
	return m
}

func (m *ReceiveQueueMessageResponse) SetTimestamp(timestamp int64) *ReceiveQueueMessageResponse {
	t := time.Unix(0, timestamp)
	m.Timestamp = t.UnixMilli()
	return m
}

func (m *ReceiveQueueMessageResponse) SetSequence(sequence int64) *ReceiveQueueMessageResponse {
	m.Sequence = sequence
	return m
}

func (m *ReceiveQueueMessageResponse) SetTags(tags string) *ReceiveQueueMessageResponse {
	m.Tags = tags
	return m
}

func (m *ReceiveQueueMessageResponse) SetReceivedCount(receivedCount int64) *ReceiveQueueMessageResponse {
	m.ReceivedCount = receivedCount
	return m
}

func (m *ReceiveQueueMessageResponse) SetReRoutedFrom(reRoutedFrom string) *ReceiveQueueMessageResponse {
	m.ReRoutedFrom = reRoutedFrom
	return m
}

func (m *ReceiveQueueMessageResponse) SetExpirationAt(expirationAt int64) *ReceiveQueueMessageResponse {
	t := time.Unix(0, expirationAt)
	m.ExpirationAt = t.UnixMilli()
	return m
}

func (m *ReceiveQueueMessageResponse) SetDelayedTo(delayedTo int64) *ReceiveQueueMessageResponse {
	t := time.Unix(0, delayedTo)
	m.DelayedTo = t.UnixMilli()
	return m
}
