package rest

import (
	"encoding/json"

	"github.com/kubemq-io/kubemq-community/config"

	"runtime"

	"github.com/labstack/echo/v4"
)

var hostname = config.GetHostname()
var goRuntime = runtime.Version()
var kubeversion string

type Response struct {
	IsError  bool            `json:"is_error"`
	Message  string          `json:"message"`
	Data     json.RawMessage `json:"data"`
	httpCode int
	c        echo.Context
	data     interface{}
}

func NewResponse(c echo.Context) *Response {
	res := &Response{
		c:        c,
		httpCode: 200,
	}
	res.setResponseHeaders()
	return res
}
func (res *Response) setResponseHeaders() *Response {
	res.c.Response().Header().Set("Broker", hostname)
	res.c.Response().Header().Set("X-Runtime", goRuntime)
	res.c.Response().Header().Set("X-Powered-By", kubeversion)
	return res
}

func (res *Response) SetError(err error) *Response {
	res.IsError = true
	res.Message = err.Error()
	return res
}

func (res *Response) SetErrorWithText(errText string) *Response {
	res.IsError = true
	res.Message = errText
	return res
}

func (res *Response) SetResponseBody(data interface{}) *Response {
	res.data = data
	return res
}
func (res *Response) SetHttpCode(value int) *Response {
	res.httpCode = value
	return res
}
func (res *Response) Send() error {
	buffer, err := json.Marshal(res.data)
	if err != nil {
		res.SetError(err)
		return res.c.JSONPretty(res.httpCode, res, "\t")
	}
	res.Data = buffer
	if !res.IsError {
		res.Message = "OK"
	}
	return res.c.JSONPretty(res.httpCode, res, "\t")
}

func (res *Response) Marshal() []byte {
	buffer, _ := json.Marshal(res)
	return buffer
}

func (res *Response) Unmarshal(v interface{}) error {
	err := json.Unmarshal(res.Data, v)
	return err
}
