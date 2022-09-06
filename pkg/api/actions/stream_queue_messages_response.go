package actions

import "encoding/json"

type StreamQueueMessagesResponse struct {
	RequestType StreamQueueMessagesRequestType `json:"requestType"`
	Message     *ReceiveQueueMessageResponse   `json:"message"`
	Error       string                         `json:"error"`
	IsError     bool                           `json:"isError"`
}

func NewStreamQueueMessageResponse() *StreamQueueMessagesResponse {
	return &StreamQueueMessagesResponse{}
}
func (r *StreamQueueMessagesResponse) SetMessage(message *ReceiveQueueMessageResponse) *StreamQueueMessagesResponse {
	r.Message = message
	return r
}
func (r *StreamQueueMessagesResponse) SetError(error error) *StreamQueueMessagesResponse {
	r.Error = error.Error()
	r.IsError = true
	return r
}
func (r *StreamQueueMessagesResponse) SetRequestType(requestType StreamQueueMessagesRequestType) *StreamQueueMessagesResponse {
	r.RequestType = requestType
	return r
}

func (r *StreamQueueMessagesResponse) Marshal() (string, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
