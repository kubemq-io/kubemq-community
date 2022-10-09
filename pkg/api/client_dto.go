package api

import (
	"github.com/dustin/go-humanize"
	"time"
)

type ClientDTO struct {
	Name              string         `json:"name"`
	LastActivity      int64          `json:"lastActivity"`
	LastActivityHuman string         `json:"lastActivityHuman"`
	Total             *BaseValuesDTO `json:"total"`
	Incoming          *BaseValuesDTO `json:"incoming"`
	Outgoing          *BaseValuesDTO `json:"outgoing"`
	IsActive          bool           `json:"isActive"`
}

func NewClientDTO(name string, entity *Entity) *ClientDTO {
	c := &ClientDTO{
		Name:              name,
		Total:             NewBaseValuesDTOFromBaseValues(entity.In.CombineWIth(entity.Out)),
		Incoming:          NewBaseValuesDTOFromBaseValues(entity.In),
		Outgoing:          NewBaseValuesDTOFromBaseValues(entity.Out),
		LastActivity:      entity.LastSeen,
		LastActivityHuman: humanize.Time(time.UnixMilli(entity.LastSeen)),
		IsActive:          entity.IsActive(),
	}
	return c
}
