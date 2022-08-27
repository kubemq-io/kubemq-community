package actions

import "time"

type SubscribeCommandQueriesMessage struct {
	RequestId    string `json:"requestId"`
	Metadata     string `json:"metadata"`
	Body         any    `json:"body"`
	Timestamp    string `json:"timestamp"`
	ReplyChannel string `json:"replyChannel"`
	ReplyBy      string `json:"replyBy"`
}

func NewSubscribeCommandQueriesMessage() *SubscribeCommandQueriesMessage {
	return &SubscribeCommandQueriesMessage{}
}

func (m *SubscribeCommandQueriesMessage) SetRequestId(requestId string) *SubscribeCommandQueriesMessage {
	m.RequestId = requestId
	return m
}

func (m *SubscribeCommandQueriesMessage) SetMetadata(metadata string) *SubscribeCommandQueriesMessage {
	m.Metadata = metadata
	return m
}

func (m *SubscribeCommandQueriesMessage) SetBody(body any) *SubscribeCommandQueriesMessage {
	m.Body = body
	return m
}

func (m *SubscribeCommandQueriesMessage) SetTimestamp(value time.Time) *SubscribeCommandQueriesMessage {
	m.Timestamp = value.Format(time.RFC3339)
	return m
}

func (m *SubscribeCommandQueriesMessage) SetReplyChannel(replyChannel string) *SubscribeCommandQueriesMessage {
	m.ReplyChannel = replyChannel
	return m
}

func (m *SubscribeCommandQueriesMessage) SetReplyBy(value time.Time) *SubscribeCommandQueriesMessage {
	m.ReplyBy = value.Format(time.RFC3339)
	return m
}
