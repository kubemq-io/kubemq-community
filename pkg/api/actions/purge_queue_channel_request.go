package actions

import (
	"fmt"
)

type PurgeQueueChannelRequest struct {
	Channel string `json:"channel"`
}

func NewPurgeQueueChannelRequest() *PurgeQueueChannelRequest {
	return &PurgeQueueChannelRequest{
		Channel: "",
	}
}

func (r *PurgeQueueChannelRequest) ParseRequest(req *Request) error {
	if req.Type != "purge_queue_channel" {
		return fmt.Errorf("request type %s must be %s", req.Type, "purge_queue_channel")
	}

	if req.Data == nil {
		return fmt.Errorf("purge queue channel request data not found")
	}

	err := transformJsonMapToStruct(req.Data, r)
	if err != nil {
		return fmt.Errorf("error unmarshalling purge queue request data: %s", err.Error())
	}

	return r.Validate()
}

func (r *PurgeQueueChannelRequest) Validate() error {
	if r.Channel == "" {
		return fmt.Errorf("channel not set")
	}
	return nil
}
