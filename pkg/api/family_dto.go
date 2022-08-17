package api

import (
	"github.com/dustin/go-humanize"
	"sort"
	"time"
)

type FamilyDTO struct {
	Name                string         `json:"name"`
	LastActivity        int64          `json:"last_activity"`
	LastActivityHuman   string         `json:"last_activity_human"`
	Total               *BaseValuesDTO `json:"total"`
	Incoming            *BaseValuesDTO `json:"incoming"`
	Outgoing            *BaseValuesDTO `json:"outgoing"`
	ChannelsList        []*ChannelDTO  `json:"channels_list"`
	Channels            int64          `json:"channels"`
	ChannelsHuman       string         `json:"channels_human"`
	Clients             int64          `json:"clients"`
	ClientsHuman        string         `json:"clients_human"`
	ActiveChannels      int64          `json:"active_channels"`
	ActiveChannelsHuman string         `json:"active_channels_human"`
	inBaseValues        *BaseValues
	outBaseValues       *BaseValues
}

func newFamilyDTO(name string) *FamilyDTO {
	return &FamilyDTO{
		Name:              name,
		LastActivity:      0,
		LastActivityHuman: humanize.Time(time.UnixMilli(0)),
		Total:             NewBaseValuesDTO(),
		Incoming:          NewBaseValuesDTO(),
		Outgoing:          NewBaseValuesDTO(),
		ChannelsList:      make([]*ChannelDTO, 0),
		Channels:          0,
		ChannelsHuman:     humanize.Comma(0),
		Clients:           0,
		ClientsHuman:      humanize.Comma(0),
		ActiveChannels:    0,
		inBaseValues:      NewBaseValues(),
		outBaseValues:     NewBaseValues(),
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
	f.ActiveChannelsHuman = humanize.Comma(f.ActiveChannels)
	return f
}

func (f *FamilyDTO) Add(family *FamilyDTO) {
	if family == nil {
		return
	}
	f.Incoming = NewBaseValuesDTOFromBaseValues(f.inBaseValues.Add(family.inBaseValues))
	f.Outgoing = NewBaseValuesDTOFromBaseValues(f.outBaseValues.Add(family.outBaseValues))
	f.Total = NewBaseValuesDTOFromBaseValues(f.inBaseValues.CombineWIth(f.outBaseValues))
	f.Clients += family.Clients
	f.ChannelsList = append(f.ChannelsList, family.ChannelsList...)
	f.ActiveChannels += family.ActiveChannels
	sort.Slice(f.ChannelsList, func(i, j int) bool {
		return f.ChannelsList[i].LastActivity > f.ChannelsList[j].LastActivity
	})
	f.Channels = int64(len(f.ChannelsList))
	f.ChannelsHuman = humanize.Comma(f.Channels)
	f.ClientsHuman = humanize.Comma(f.Clients)
	if family.LastActivity > f.LastActivity {
		f.LastActivity = family.LastActivity
	}
	f.LastActivityHuman = humanize.Time(time.UnixMilli(f.LastActivity))
	f.ActiveChannelsHuman = humanize.Comma(f.ActiveChannels)
}
