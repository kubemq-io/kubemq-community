package events

import (
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	kubemq "github.com/kubemq-io/kubemq-go"
)

type object struct {
	Id         string            `json:"id"`
	Channel    string            `json:"channel,omitempty"`
	ClientId   string            `json:"client_id,omitempty"`
	Metadata   string            `json:"metadata,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
	BodyJson   json.RawMessage   `json:"body_json,omitempty"`
	BodyString string            `json:"body_string,omitempty"`
}

func newObjectWithEvent(event *kubemq.Event) *object {
	obj := &object{
		Id:         event.Id,
		Channel:    event.Channel,
		ClientId:   event.ClientId,
		Metadata:   event.Metadata,
		Tags:       event.Tags,
		BodyJson:   json.RawMessage{},
		BodyString: "",
	}
	var js json.RawMessage
	if err := json.Unmarshal(event.Body, &js); err == nil {
		obj.BodyJson = js
	} else {
		sDec, err := b64.StdEncoding.DecodeString(string(event.Body))
		if err != nil {
			obj.BodyString = string(event.Body)
		} else {
			obj.BodyString = string(sDec)
		}
	}
	return obj
}

func (o *object) String() string {
	data, _ := json.MarshalIndent(o, "", "    ")
	return string(data)
}

func printEvent(event *kubemq.Event) {
	fmt.Println(newObjectWithEvent(event))
}
