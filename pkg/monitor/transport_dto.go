package monitor

import (
	"encoding/json"
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
	"time"
)

func TransformToDtoString(tr *Transport) (string, error) {
	var t any
	switch tr.Kind {
	case "event_receive":
		t = NewTransportPubSubMessageDto(tr)
	case "command", "query":
		t = NewTransportRequestMessageDto(tr)

	case "response":
		t = NewTransportResponseMessageDto(tr)

	case "queue":
		t = NewTransportQueueMessageDto(tr)
	default:
		return "", fmt.Errorf("unknown transport kind %s", tr.Kind)
	}
	b, _ := json.MarshalIndent(t, "", "  ")
	return string(b), nil

}

type TransportQueueMessageDto struct {
	MessageId     string `json:"messageId"`
	Metadata      string `json:"metadata,omitempty"`
	Body          any    `json:"body,omitempty"`
	Timestamp     string `json:"timestamp,omitempty"`
	Sequence      int64  `json:"sequence,omitempty"`
	Tags          string `json:"tags,omitempty"`
	ReceivedCount int32  `json:"receivedCount,omitempty"`
	ReRoutedFrom  string `json:"reRoutedFrom,omitempty"`
	ExpirationAt  string `json:"expirationAt,omitempty"`
	DelayedTo     string `json:"delayedTo,omitempty"`
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
	}
	if message.Attributes.Timestamp > 0 {
		ts := time.Unix(0, message.Attributes.Timestamp)
		dto.Timestamp = ts.Format("2006-01-02 15:04:05")
	}
	dto.Sequence = int64(message.Attributes.Sequence)
	if len(message.Tags) > 0 {
		data, _ := json.Marshal(message.Tags)
		dto.Tags = string(data)
	}
	dto.ReceivedCount = message.Attributes.ReceiveCount
	dto.ReRoutedFrom = message.Attributes.ReRoutedFromQueue
	if message.Attributes.ExpirationAt > 0 {
		ts := time.Unix(0, message.Attributes.ExpirationAt)
		dto.ExpirationAt = ts.Format("2006-01-02 15:04:05")
	}
	if message.Attributes.DelayedTo > 0 {
		ts := time.Unix(0, message.Attributes.DelayedTo)
		dto.DelayedTo = ts.Format("2006-01-02 15:04:05")
	}
	dto.Body = detectAndConvertToAny(message.Body)
	return dto
}

type TransportPubSubMessageDto struct {
	MessageId string `json:"messageId"`
	Metadata  string `json:"metadata,omitempty"`
	Body      any    `json:"body,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
	Sequence  int64  `json:"sequence,omitempty"`
	Tags      string `json:"tags,omitempty"`
}

func NewTransportPubSubMessageDto(tr *Transport) *TransportPubSubMessageDto {
	dto := &TransportPubSubMessageDto{}
	message := &pb.EventReceive{}
	err := tr.Unmarshal(message)
	if err != nil {
		return nil
	}
	dto.MessageId = message.EventID
	if message.Metadata != "" {
		dto.Metadata = message.Metadata
	}
	if message.Timestamp > 0 {
		ts := time.Unix(0, message.Timestamp)
		dto.Timestamp = ts.Format("2006-01-02 15:04:05")
	}
	dto.Sequence = int64(message.Sequence)
	if len(message.Tags) > 0 {
		data, _ := json.Marshal(message.Tags)
		dto.Tags = string(data)
	}
	dto.Body = detectAndConvertToAny(message.Body)
	return dto
}

type TransportRequestMessageDto struct {
	RequestId string `json:"requestId"`
	Metadata  string `json:"metadata,omitempty"`
	Body      any    `json:"body,omitempty"`
	Timeout   int32  `json:"timeout,omitempty"`
	Tags      string `json:"tags,omitempty"`
}

func NewTransportRequestMessageDto(tr *Transport) *TransportRequestMessageDto {
	dto := &TransportRequestMessageDto{}
	message := &pb.Request{}
	err := tr.Unmarshal(message)
	if err != nil {
		return nil
	}
	dto.RequestId = message.RequestID
	if message.Metadata != "" {
		dto.Metadata = message.Metadata
	}
	dto.Timeout = message.Timeout
	if len(message.Tags) > 0 {
		data, _ := json.Marshal(message.Tags)
		dto.Tags = string(data)
	}
	dto.Body = detectAndConvertToAny(message.Body)
	return dto
}

type TransportResponseMessageDto struct {
	RequestId string `json:"requestId"`
	Metadata  string `json:"metadata,omitempty"`
	Body      any    `json:"body,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
	Tags      string `json:"tags,omitempty"`
	Error     string `json:"error,omitempty"`
	Executed  bool   `json:"executed,omitempty"`
}

func NewTransportResponseMessageDto(tr *Transport) *TransportResponseMessageDto {
	dto := &TransportResponseMessageDto{}
	message := &pb.Response{}
	err := tr.Unmarshal(message)
	if err != nil {
		return nil
	}
	dto.RequestId = message.RequestID
	if message.Metadata != "" {
		dto.Metadata = message.Metadata
	}
	if message.Timestamp > 0 {
		ts := time.Unix(message.Timestamp, 0)
		dto.Timestamp = ts.Format("2006-01-02 15:04:05")
	}
	if len(message.Tags) > 0 {
		data, _ := json.Marshal(message.Tags)
		dto.Tags = string(data)
	}
	if message.Body != nil {
		dto.Body = detectAndConvertToAny(message.Body)
	}
	dto.Error = message.Error
	dto.Executed = message.Executed
	return dto
}
