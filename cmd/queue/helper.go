package queue

import (
	b64 "encoding/base64"
	"encoding/json"
	"fmt"

	kubemq "github.com/kubemq-io/kubemq-go"
	"time"
)

type queueMessageObject struct {
	Id                        string            `json:"id"`
	Channel                   string            `json:"channel"`
	ClientId                  string            `json:"client_id"`
	Timestamp                 string            `json:"timestamp,omitempty"`
	Sequence                  uint64            `json:"sequence,omitempty"`
	Metadata                  string            `json:"metadata,omitempty"`
	DelayTo                   string            `json:"delay_to,omitempty"`
	ExpireAt                  string            `json:"expire_at,omitempty"`
	MaxDeadLetterQueueRetires int32             `json:"max_dead_letter_queue_retires,omitempty"`
	DeadLetterQueue           string            `json:"dead_letter_queue,omitempty"`
	Tags                      map[string]string `json:"tags,omitempty"`
	BodyJson                  json.RawMessage   `json:"body_json,omitempty"`
	BodyString                string            `json:"body_string,omitempty"`
}

func newQueueMessageObject(msg *kubemq.QueueMessage) *queueMessageObject {
	obj := &queueMessageObject{
		Id:         msg.MessageID,
		Channel:    msg.Channel,
		ClientId:   msg.ClientID,
		Timestamp:  "",
		Sequence:   0,
		Metadata:   msg.Metadata,
		DelayTo:    "",
		ExpireAt:   "",
		Tags:       msg.Tags,
		BodyJson:   json.RawMessage{},
		BodyString: "",
	}
	if msg.Policy != nil {
		if msg.Policy.DelaySeconds > 0 {
			obj.DelayTo = (time.Duration(msg.Policy.DelaySeconds) * time.Second).String()
		}
		if msg.Policy.ExpirationSeconds > 0 {
			obj.ExpireAt = (time.Duration(msg.Policy.ExpirationSeconds) * time.Second).String()
		}
		obj.MaxDeadLetterQueueRetires = msg.Policy.MaxReceiveCount
		obj.DeadLetterQueue = msg.Policy.MaxReceiveQueue
	}
	if msg.Attributes != nil {
		obj.Timestamp = time.Unix(0, msg.Attributes.Timestamp).Format("2006-01-02 15:04:05.999")
		obj.Sequence = msg.Attributes.Sequence
		if msg.Attributes.DelayedTo > 0 {
			obj.DelayTo = time.Unix(0, msg.Attributes.DelayedTo).Format("2006-01-02 15:04:05.999")
		}
		if msg.Attributes.ExpirationAt > 0 {
			obj.ExpireAt = time.Unix(0, msg.Attributes.ExpirationAt).Format("2006-01-02 15:04:05.999")
		}
	}
	var js json.RawMessage
	if err := json.Unmarshal(msg.Body, &js); err == nil {
		obj.BodyJson = js
	} else {
		sDec, err := b64.StdEncoding.DecodeString(string(msg.Body))
		if err != nil {
			obj.BodyString = string(msg.Body)
		} else {
			obj.BodyString = string(sDec)
		}
	}

	return obj
}

func (o *queueMessageObject) String() string {
	data, _ := json.MarshalIndent(o, "", "    ")
	return string(data)
}

func printItems(items []*kubemq.QueueMessage) {
	for _, item := range items {
		fmt.Println(newQueueMessageObject(item))
	}
}
func printQueueMessage(msg *kubemq.QueueMessage) {
	fmt.Println(newQueueMessageObject(msg))
}

type resultObj struct {
	MessageID    string `json:"message_id"`
	SentAt       string `json:"sent_at,omitempty"`
	ExpirationAt string `json:"expiration_at,omitempty"`
	DelayedTo    string `json:"delayed_to,omitempty"`
	IsError      bool   `json:"is_error,omitempty"`
	Error        string `json:"error,omitempty"`
}

func printQueueMessageResult(res *kubemq.SendQueueMessageResult) {
	obj := &resultObj{
		MessageID: res.MessageID,
		SentAt:    time.Unix(0, res.SentAt).Format("2006-01-02 15:04:05.999"),
		IsError:   res.IsError,
		Error:     res.Error,
	}
	if res.ExpirationAt > 0 {
		obj.ExpirationAt = time.Unix(0, res.ExpirationAt).Format("2006-01-02 15:04:05.999")
	}
	if res.DelayedTo > 0 {
		obj.DelayedTo = time.Unix(0, res.DelayedTo).Format("2006-01-02 15:04:05.999")
	}
	data, _ := json.MarshalIndent(obj, "", "    ")
	fmt.Println(string(data))
}
