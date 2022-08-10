package api

import (
	"github.com/dustin/go-humanize"
	"time"
)

type ChannelDTO struct {
	Name              string         `json:"name"`
	Family            string         `json:"family"`
	LastActivity      int64          `json:"last_activity"`
	LastActivityHuman string         `json:"last_activity_human"`
	IsActive          bool           `json:"is_active"`
	Total             *BaseValuesDTO `json:"total"`
	Incoming          *BaseValuesDTO `json:"incoming"`
	Outgoing          *BaseValuesDTO `json:"outgoing"`
}

// create a new ChannelDTO from Entity

func NewChannelDTO(family, name string, entity *Entity) *ChannelDTO {
	return &ChannelDTO{
		Name:              name,
		Family:            family,
		Total:             NewBaseValuesDTOFromBaseValues(entity.In.CombineWIth(entity.Out)),
		Incoming:          NewBaseValuesDTOFromBaseValues(entity.In),
		Outgoing:          NewBaseValuesDTOFromBaseValues(entity.Out),
		LastActivity:      entity.LastSeen,
		LastActivityHuman: humanize.Time(time.UnixMilli(entity.LastSeen)),
		IsActive:          entity.IsActive(),
	}
}
