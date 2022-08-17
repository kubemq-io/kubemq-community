package api

import (
	"github.com/dustin/go-humanize"
	"time"
)

type ChannelDTO struct {
	Name              string         `json:"name"`
	Type              string         `json:"type"`
	LastActivity      int64          `json:"lastActivity"`
	LastActivityHuman string         `json:"lastActivityHuman"`
	Total             *BaseValuesDTO `json:"total"`
	Incoming          *BaseValuesDTO `json:"incoming"`
	Outgoing          *BaseValuesDTO `json:"outgoing"`
}

// create a new ChannelDTO from Entity

func NewChannelDTO(_type, name string, entity *Entity) *ChannelDTO {
	return &ChannelDTO{
		Name:              name,
		Type:              _type,
		Total:             NewBaseValuesDTOFromBaseValues(entity.In.CombineWIth(entity.Out)),
		Incoming:          NewBaseValuesDTOFromBaseValues(entity.In),
		Outgoing:          NewBaseValuesDTOFromBaseValues(entity.Out),
		LastActivity:      entity.LastSeen,
		LastActivityHuman: humanize.Time(time.UnixMilli(entity.LastSeen)),
	}
}
