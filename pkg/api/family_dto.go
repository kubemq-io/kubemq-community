package api

import (
	"github.com/dustin/go-humanize"
	"sort"
	"time"
)

type FamilyDTO struct {
	Name              string         `json:"name"`
	LastActivity      int64          `json:"last_activity"`
	LastActivityHuman string         `json:"last_activity_human"`
	Total             *BaseValuesDTO `json:"total"`
	Incoming          *BaseValuesDTO `json:"incoming"`
	Outgoing          *BaseValuesDTO `json:"outgoing"`
	ChannelsList      []*ChannelDTO  `json:"channels_list"`
	Channels          int64          `json:"channels"`
	ChannelsHuman     string         `json:"channels_human"`
	Clients           int64          `json:"clients"`
	ClientsHuman      string         `json:"clients_human"`
	ActiveChannels    int64          `json:"active_channels"`
	inBaseValues      *BaseValues
	outBaseValues     *BaseValues
}

func newFamilyDTO(name string) *FamilyDTO {
	return &FamilyDTO{
		Name:          name,
		inBaseValues:  NewBaseValues(),
		outBaseValues: NewBaseValues(),
	}
}
func NewFamilyDTO(family *EntitiesFamily) *FamilyDTO {
	f := newFamilyDTO(family.Name)

	for _, channel := range family.Entities {
		channelDTO := NewChannelDTO(family.Name, channel.Name, channel)
		f.inBaseValues.Add(channel.In)
		f.outBaseValues.Add(channel.Out)
		if channelDTO.LastActivity > f.LastActivity {
			f.LastActivity = channelDTO.LastActivity
		}
		f.Channels++
		f.Clients += channelDTO.Total.Clients
		f.ChannelsList = append(f.ChannelsList, channelDTO)
		if channel.IsActive() {
			f.ActiveChannels++
		}
	}
	sort.Slice(f.ChannelsList, func(i, j int) bool {
		return f.ChannelsList[i].LastActivity > f.ChannelsList[j].LastActivity
	})
	f.Incoming = NewBaseValuesDTOFromBaseValues(f.inBaseValues)
	f.Outgoing = NewBaseValuesDTOFromBaseValues(f.outBaseValues)
	f.Total = NewBaseValuesDTOFromBaseValues(f.inBaseValues.CombineWIth(f.outBaseValues))
	f.ChannelsHuman = humanize.Comma(f.Channels)
	f.ClientsHuman = humanize.Comma(f.Clients)
	f.LastActivityHuman = humanize.Time(time.UnixMilli(f.LastActivity))
	return f
}
