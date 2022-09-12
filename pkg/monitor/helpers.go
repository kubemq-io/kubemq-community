package monitor

import (
	"encoding/json"
	pb "github.com/kubemq-io/protobuf/go"
)

func responseToMessage(r *pb.Response, channel string) *pb.Event {
	data, _ := r.Marshal()
	return &pb.Event{
		ClientID: "monitor-client-id",
		Channel:  channel,
		Metadata: "response",
		Body:     data}
}

func detectAndConvertToAny(data []byte) any {
	jsonObject := make(map[string]interface{})
	err := json.Unmarshal(data, &jsonObject)
	if err == nil {
		return jsonObject
	}
	jsonArray := make([]map[string]interface{}, 0)
	err = json.Unmarshal(data, &jsonArray)
	if err == nil {

		return jsonArray
	}
	return string(data)
}
