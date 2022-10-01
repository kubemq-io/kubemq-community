package actions

import (
	"fmt"
)

type CreateChannelRequest struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

func NewCreateChannelRequest() *CreateChannelRequest {
	return &CreateChannelRequest{
		Type: "",
		Name: "",
	}
}

func (r *CreateChannelRequest) ParseRequest(req *Request) error {
	if req.Type != "create_channel" {
		return fmt.Errorf("request type %s must be %s", req.Type, "create_channel")
	}

	if req.Data == nil {
		return fmt.Errorf("create channel request data not found")
	}

	err := transformJsonMapToStruct(req.Data, r)
	if err != nil {
		return fmt.Errorf("error unmarshalling create channel request data: %s", err.Error())
	}

	return r.Validate()
}

func (r *CreateChannelRequest) Validate() error {
	if r.Type == "" {
		return fmt.Errorf("type not set")
	}
	if r.Name == "" {
		return fmt.Errorf("name not set")
	}
	return nil
}
