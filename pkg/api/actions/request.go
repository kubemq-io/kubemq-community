package actions

type Request struct {
	Type string                 `json:"type"`
	Data map[string]interface{} `json:"data"`
}

func NewRequest() *Request {
	return &Request{
		Type: "",
		Data: make(map[string]interface{}),
	}
}

func (r *Request) SetType(type_ string) *Request {
	r.Type = type_
	return r
}

func (r *Request) SetData(data map[string]interface{}) *Request {
	r.Data = data
	return r
}
