package actions

type Response struct {
	Error string `json:"error,omitempty"`
	Data  any    `json:"data"`
}

func NewResponse() *Response {
	return &Response{
		Error: "",
		Data:  nil,
	}
}

func (r *Response) SetError(err error) *Response {
	r.Error = err.Error()
	return r
}

func (r *Response) SetData(data any) *Response {
	r.Data = data
	return r
}
