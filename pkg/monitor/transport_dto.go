package monitor

import (
	"encoding/json"
	pb "github.com/kubemq-io/protobuf/go"
	"time"
)

type TransportQueueMessageDto struct {
	MessageId     string `json:"messageId"`
	Metadata      string `json:"metadata"`
	Body          any    `json:"body"`
	Timestamp     string `json:"timestamp"`
	Sequence      int64  `json:"sequence"`
	Tags          string `json:"tags"`
	ReceivedCount int32  `json:"receivedCount"`
	ReRoutedFrom  string `json:"reRoutedFrom"`
	ExpirationAt  string `json:"expirationAt"`
	DelayedTo     string `json:"delayedTo"`
}

func NewTransportQueueMessageDto(tr *Transport) *TransportQueueMessageDto {
	dto := &TransportQueueMessageDto{}
	message := &pb.QueueMessage{}
	err := tr.Unmarshal(message)
	if err != nil {
		return nil
	}
	dto.MessageId = message.MessageID
	if message.Metadata != "" {
		dto.Metadata = message.Metadata
	} else {
		dto.Metadata = "N/A"
	}
	if message.Attributes.Timestamp > 0 {
		ts := time.Unix(0, message.Attributes.Timestamp)
		dto.Timestamp = ts.Format("2006-01-02 15:04:05")
	} else {
		dto.Timestamp = "N/A"
	}
	dto.Sequence = int64(message.Attributes.Sequence)
	if len(message.Tags) > 0 {
		data, _ := json.Marshal(message.Tags)
		dto.Tags = string(data)
	} else {
		dto.Tags = "N/A"
	}
	dto.ReceivedCount = message.Attributes.ReceiveCount
	dto.ReRoutedFrom = message.Attributes.ReRoutedFromQueue
	if message.Attributes.ExpirationAt > 0 {
		ts := time.Unix(0, message.Attributes.ExpirationAt)
		dto.ExpirationAt = ts.Format("2006-01-02 15:04:05")
	} else {
		dto.ExpirationAt = "N/A"
	}
	if message.Attributes.DelayedTo > 0 {
		ts := time.Unix(0, message.Attributes.DelayedTo)
		dto.DelayedTo = ts.Format("2006-01-02 15:04:05")
	} else {
		dto.DelayedTo = "N/A"
	}
	dto.Body = detectAndConvertToAny(message.Body)
	return dto
}

func (t *TransportQueueMessageDto) ToJson() string {
	b, _ := json.MarshalIndent(t, "", "  ")
	return string(b)
}
