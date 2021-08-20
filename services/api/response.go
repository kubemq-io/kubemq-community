package api

import (
	"encoding/json"

	"github.com/labstack/echo/v4"
)

type Response struct {
	Error       bool            `json:"error"`
	ErrorString string          `json:"error_string"`
	Data        json.RawMessage `json:"data"`
	httpCode    int
	c           echo.Context
	data        interface{}
}

func NewResponse(c echo.Context) *Response {
	return &Response{
		c:        c,
		httpCode: 200,
	}
}

func (res *Response) SetError(err error) *Response {
	res.Error = true
	res.ErrorString = err.Error()
	return res
}

func (res *Response) SetErrorWithText(errText string) *Response {
	res.Error = true
	res.ErrorString = errText
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
	res.c.Set("result", res)
	var err error
	res.Data, err = json.Marshal(res.data)
	if err != nil {
		res.SetError(err)
		return res.c.JSONPretty(res.httpCode, res, "\t")
	}
	return res.c.JSONPretty(res.httpCode, res, "\t")
}

func (res *Response) SendData(data []byte) error {
	res.c.Set("result", res)
	res.Data = data
	return res.c.JSONPretty(res.httpCode, res, "\t")
}
func (res *Response) Unmarshal(v interface{}) error {
	err := json.Unmarshal(res.Data, v)
	return err
}
