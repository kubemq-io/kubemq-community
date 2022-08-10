package api

import (
	"github.com/dustin/go-humanize"
	"time"
)

type GroupDTO struct {
	System            *System               `json:"system"`
	LastActivity      int64                 `json:"last_activity"`
	LastActivityHuman string                `json:"last_activity_human"`
	Total             *BaseValuesDTO        `json:"total"`
	Incoming          *BaseValuesDTO        `json:"incoming"`
	Outgoing          *BaseValuesDTO        `json:"outgoing"`
	Channels          int64                 `json:"channels"`
	ChannelsHuman     string                `json:"channels_human"`
	Clients           int64                 `json:"clients"`
	ClientsHuman      string                `json:"clients_human"`
	ActiveChannels    int64                 `json:"active_channels"`
	Families          map[string]*FamilyDTO `json:"families"`
	inBaseValues      *BaseValues
	outBaseValues     *BaseValues
}

func newGroupDTO(system *System) *GroupDTO {
	return &GroupDTO{
		System:        system,
		Families:      make(map[string]*FamilyDTO),
		inBaseValues:  NewBaseValues(),
		outBaseValues: NewBaseValues(),
	}
}
func NewGroupDTO(system *System, entitiesGroup *EntitiesGroup) *GroupDTO {
	group := newGroupDTO(system)

	for name, family := range entitiesGroup.Families {
		familyDTO := NewFamilyDTO(family)
		group.Families[name] = familyDTO
		group.inBaseValues.Add(familyDTO.inBaseValues)
		group.outBaseValues.Add(familyDTO.outBaseValues)
		group.Channels += familyDTO.Channels
		group.ActiveChannels += familyDTO.ActiveChannels
		group.Clients += familyDTO.Clients
		if group.LastActivity < familyDTO.LastActivity {
			group.LastActivity = familyDTO.LastActivity
		}
	}
	group.Incoming = NewBaseValuesDTOFromBaseValues(group.inBaseValues)
	group.Outgoing = NewBaseValuesDTOFromBaseValues(group.outBaseValues)
	group.Total = NewBaseValuesDTOFromBaseValues(group.inBaseValues.CombineWIth(group.outBaseValues))
	group.LastActivityHuman = humanize.Time(time.UnixMilli(group.LastActivity))
	group.ChannelsHuman = humanize.Comma(group.Channels)
	group.ClientsHuman = humanize.Comma(group.Clients)
	return group
}
