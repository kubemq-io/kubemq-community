package actions

type SubscribeCQRSRequestMessage struct {
	RequestId    string `json:"requestId"`
	Metadata     string `json:"metadata"`
	Body         any    `json:"body"`
	Timestamp    int64  `json:"timestamp"`
	ReplyChannel string `json:"replyChannel"`
	Tags         string `json:"tags"`
	IsCommand    bool   `json:"isCommand"`
}

func NewSubscribeCQRSRequestMessage() *SubscribeCQRSRequestMessage {
	return &SubscribeCQRSRequestMessage{}
}

func (m *SubscribeCQRSRequestMessage) SetRequestId(requestId string) *SubscribeCQRSRequestMessage {
	m.RequestId = requestId
	return m
}

func (m *SubscribeCQRSRequestMessage) SetMetadata(metadata string) *SubscribeCQRSRequestMessage {
	m.Metadata = metadata
	return m
}

func (m *SubscribeCQRSRequestMessage) SetBody(body any) *SubscribeCQRSRequestMessage {
	m.Body = body
	return m
}

func (m *SubscribeCQRSRequestMessage) SetTimestamp(value int64) *SubscribeCQRSRequestMessage {
	m.Timestamp = value
	return m
}

func (m *SubscribeCQRSRequestMessage) SetReplyChannel(replyChannel string) *SubscribeCQRSRequestMessage {
	m.ReplyChannel = replyChannel
	return m
}

func (m *SubscribeCQRSRequestMessage) SetTags(tags string) *SubscribeCQRSRequestMessage {
	m.Tags = tags
	return m
}

func (m *SubscribeCQRSRequestMessage) SetIsCommand(isCommand bool) *SubscribeCQRSRequestMessage {
	m.IsCommand = isCommand
	return m
}
