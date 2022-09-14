package actions

import "time"

type ReceiveCQRSResponse struct {
	Metadata  string `json:"metadata,omitempty"`
	Body      any    `json:"body,omitempty"`
	Tags      string `json:"tags,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
	Executed  bool   `json:"executed"`
	Error     string `json:"error,omitempty"`
}

func NewReceiveCQRSResponse() *ReceiveCQRSResponse {
	return &ReceiveCQRSResponse{}
}

func (m *ReceiveCQRSResponse) SetMetadata(metadata string) *ReceiveCQRSResponse {
	m.Metadata = metadata
	return m
}

func (m *ReceiveCQRSResponse) SetBody(body any) *ReceiveCQRSResponse {
	m.Body = body
	return m
}

func (m *ReceiveCQRSResponse) SetTimestamp(timestamp int64) *ReceiveCQRSResponse {
	t := time.Unix(0, timestamp)
	m.Timestamp = t.UnixMilli()
	return m
}

func (m *ReceiveCQRSResponse) SetTags(tags string) *ReceiveCQRSResponse {
	m.Tags = tags
	return m
}

func (m *ReceiveCQRSResponse) SetExecuted(executed bool) *ReceiveCQRSResponse {
	m.Executed = executed
	return m
}

func (m *ReceiveCQRSResponse) SetError(error string) *ReceiveCQRSResponse {
	m.Error = error
	return m
}
