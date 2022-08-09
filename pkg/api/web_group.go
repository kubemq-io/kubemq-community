package api

type WebGroupBase struct {
	IncomingMessages int64 `json:"incoming_messages"`
	OutgoingMessages int64 `json:"outgoing_messages"`
	IncomingVolume   int64 `json:"incoming_volume"`
	OutgoingVolume   int64 `json:"outgoing_volume"`
	Errors           int64 `json:"errors"`
	Waiting          int64 `json:"waiting"`
}

func NewWebGroupBase() WebGroupBase {
	return WebGroupBase{}
}

func (b WebGroupBase) SetIncomingMessages(value int64) WebGroupBase {
	b.IncomingMessages = value
	return b
}

func (b WebGroupBase) SetOutgoingMessages(value int64) WebGroupBase {
	b.OutgoingMessages = value
	return b
}

func (b WebGroupBase) SetIncomingVolume(value int64) WebGroupBase {
	b.IncomingVolume = value
	return b
}

func (b WebGroupBase) SetOutgoingVolume(value int64) WebGroupBase {
	b.OutgoingVolume = value
	return b
}

func (b WebGroupBase) SetErrors(value int64) WebGroupBase {
	b.Errors = value
	return b
}

func (b WebGroupBase) SetWaiting(value int64) WebGroupBase {
	b.Waiting = value
	return b
}

type WebGroupChannel struct {
	Type string `json:"type,omitempty"`
	Name string `json:"name"`
	WebGroupBase
}

func NewWebGroupChannel() WebGroupChannel {
	return WebGroupChannel{
		WebGroupBase: NewWebGroupBase(),
	}
}

func (b WebGroupChannel) SetType(value string) WebGroupChannel {
	b.Type = value
	return b
}

func (b WebGroupChannel) SetName(value string) WebGroupChannel {
	b.Name = value
	return b
}

func (b WebGroupChannel) SetIncomingMessages(value int64) WebGroupChannel {
	b.IncomingMessages = value
	return b
}

func (b WebGroupChannel) SetOutgoingMessages(value int64) WebGroupChannel {
	b.OutgoingMessages = value
	return b
}

func (b WebGroupChannel) SetIncomingVolume(value int64) WebGroupChannel {
	b.IncomingVolume = value
	return b
}

func (b WebGroupChannel) SetOutgoingVolume(value int64) WebGroupChannel {
	b.OutgoingVolume = value
	return b
}

func (b WebGroupChannel) SetErrors(value int64) WebGroupChannel {
	b.Errors = value
	return b
}

func (b WebGroupChannel) SetWaiting(value int64) WebGroupChannel {
	b.Waiting = value
	return b
}

type WebGroupTotals struct {
	Channels int64 `json:"channels"`
	WebGroupBase
}

func NewWebGroupTotals() WebGroupTotals {
	return WebGroupTotals{
		WebGroupBase: NewWebGroupBase(),
	}
}

func (b WebGroupTotals) SetChannels(value int64) WebGroupTotals {
	b.Channels = value
	return b
}

func (b WebGroupTotals) SetIncomingMessages(value int64) WebGroupTotals {
	b.IncomingMessages = value
	return b
}

func (b WebGroupTotals) SetOutgoingMessages(value int64) WebGroupTotals {
	b.OutgoingMessages = value
	return b
}

func (b WebGroupTotals) SetIncomingVolume(value int64) WebGroupTotals {
	b.IncomingVolume = value
	return b
}

func (b WebGroupTotals) SetOutgoingVolume(value int64) WebGroupTotals {
	b.OutgoingVolume = value
	return b
}

func (b WebGroupTotals) SetErrors(value int64) WebGroupTotals {
	b.Errors = value
	return b
}

func (b WebGroupTotals) SetWaiting(value int64) WebGroupTotals {
	b.Waiting = value
	return b
}

type WebGroup struct {
	Type     string            `json:"type"`
	Totals   WebGroupTotals    `json:"totals"`
	Channels []WebGroupChannel `json:"channels"`
}

func NewWebGroup() WebGroup {
	return WebGroup{
		Totals:   NewWebGroupTotals(),
		Channels: []WebGroupChannel{},
	}
}

func (b WebGroup) SetType(value string) WebGroup {
	b.Type = value
	return b
}

func (b WebGroup) SetTotals(value WebGroupTotals) WebGroup {
	b.Totals = value
	return b
}

func (b WebGroup) AddChannel(value WebGroupChannel) WebGroup {
	b.Channels = append(b.Channels, value)
	return b
}
func (b WebGroup) SetChannels(value []WebGroupChannel) WebGroup {
	b.Channels = value
	return b
}
