package commands

import (
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	kubemq "github.com/kubemq-io/kubemq-go"
	"strconv"
)

type object struct {
	Id         string            `json:"id"`
	Channel    string            `json:"channel,omitempty"`
	ClientId   string            `json:"client_id,omitempty"`
	Metadata   string            `json:"metadata,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
	Executed   string            `json:"executed,omitempty"`
	Timeout    string            `json:"timeout,omitempty"`
	ExecutedAt string            `json:"executed_at,omitempty"`
	Error      string            `json:"error,omitempty"`
	BodyJson   json.RawMessage   `json:"body_json,omitempty"`
	BodyString string            `json:"body_string,omitempty"`
}

func newObjectWithCommandReceive(cmd *kubemq.CommandReceive) *object {
	obj := &object{
		Id:         cmd.Id,
		Channel:    cmd.Channel,
		ClientId:   cmd.ClientId,
		Metadata:   cmd.Metadata,
		Tags:       cmd.Tags,
		BodyJson:   json.RawMessage{},
		BodyString: "",
		Executed:   "",
		ExecutedAt: "",
		Error:      "",
		Timeout:    "",
	}

	var js json.RawMessage
	if err := json.Unmarshal(cmd.Body, &js); err == nil {
		obj.BodyJson = js
	} else {
		sDec, err := b64.StdEncoding.DecodeString(string(cmd.Body))
		if err != nil {
			obj.BodyString = string(cmd.Body)
		} else {
			obj.BodyString = string(sDec)
		}
	}
	return obj
}
func newObjectWithCommandResponse(response *kubemq.CommandResponse) *object {
	obj := &object{
		Id:         response.CommandId,
		Channel:    "",
		ClientId:   response.ResponseClientId,
		Metadata:   "",
		Tags:       response.Tags,
		Executed:   strconv.FormatBool(response.Executed),
		Timeout:    "",
		ExecutedAt: response.ExecutedAt.Format("2006-01-02 15:04:05.999"),
		Error:      response.Error,
		BodyJson:   nil,
		BodyString: "",
	}
	if !response.Executed {
		obj.ExecutedAt = ""
	}
	return obj
}
func newObjectWithCommand(cmd *kubemq.Command) *object {

	obj := &object{
		Id:         cmd.Id,
		Channel:    cmd.Channel,
		ClientId:   cmd.ClientId,
		Metadata:   cmd.Metadata,
		Tags:       cmd.Tags,
		BodyJson:   json.RawMessage{},
		BodyString: "",
		Executed:   "",
		ExecutedAt: "",
		Error:      "",
		Timeout:    cmd.Timeout.String(),
	}
	var js json.RawMessage
	if err := json.Unmarshal(cmd.Body, &js); err == nil {
		obj.BodyJson = js
	} else {
		sDec, err := b64.StdEncoding.DecodeString(string(cmd.Body))
		if err != nil {
			obj.BodyString = string(cmd.Body)
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

func printCommandReceive(command *kubemq.CommandReceive) {
	fmt.Println(newObjectWithCommandReceive(command))
}

func printCommandResponse(response *kubemq.CommandResponse) {
	fmt.Println(newObjectWithCommandResponse(response))
}
func printCommand(cmd *kubemq.Command) {
	fmt.Println(newObjectWithCommand(cmd))
}
