package actions

import "encoding/json"

type Request struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func NewRequest() *Request {
	return &Request{
		Type: "",
		Data: nil,
	}
}

func (r *Request) SetType(type_ string) *Request {
	r.Type = type_
	return r
}

func (r *Request) SetData(data json.RawMessage) *Request {
	r.Data = data
	return r
}
