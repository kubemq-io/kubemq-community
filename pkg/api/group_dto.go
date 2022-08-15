package api

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"time"
)

type GroupDTO struct {
	HostsInfo         []*HostInfoDTO `json:"hostsInfo"`
	StatsCards        *StatCardDTOs  `json:"statsCards"`
	LastActivity      int64          `json:"lastActivity"`
	LastActivityHuman string         `json:"lastActivityHuman"`
	Total             *BaseValuesDTO `json:"total"`
	Incoming          *BaseValuesDTO `json:"incoming"`
	Outgoing          *BaseValuesDTO `json:"outgoing"`
	Channels          int64          `json:"channels"`
	ChannelsHuman     string         `json:"channelsHuman"`
	Clients           int64          `json:"clients"`
	ClientsHuman      string         `json:"clientsHuman"`
	ActiveChannels    int64          `json:"activeChannels"`
	Queues            *FamilyDTO     `json:"queues"`
	Pubsub            *FamilyDTO     `json:"pubsub"`
	RequestReply      *FamilyDTO     `json:"requestReply"`
	inBaseValues      *BaseValues
	outBaseValues     *BaseValues
}

func newGroupDTO(system *System) *GroupDTO {
	return &GroupDTO{
		HostsInfo:     []*HostInfoDTO{NewHostInfoDTO(system)},
		StatsCards:    NewStatCardDTOs(),
		Queues:        newFamilyDTO("queues"),
		Pubsub:        newFamilyDTO("pubsub"),
		RequestReply:  newFamilyDTO("request_reply"),
		inBaseValues:  NewBaseValues(),
		outBaseValues: NewBaseValues(),
	}
}
func NewGroupDTO(system *System, entitiesGroup *EntitiesGroup) *GroupDTO {
	group := newGroupDTO(system)
	queueFamily, ok := entitiesGroup.GetFamily("queues")
	if ok {
		group.Queues = NewFamilyDTO(queueFamily)
		group.inBaseValues.Add(group.Queues.inBaseValues)
		group.outBaseValues.Add(group.Queues.outBaseValues)
		group.Channels += group.Queues.Channels
		group.ActiveChannels += group.Queues.ActiveChannels
		group.Clients += group.Queues.Clients
		if group.LastActivity < group.Queues.LastActivity {
			group.LastActivity = group.Queues.LastActivity
		}
	}

	eventsFamily, ok := entitiesGroup.GetFamily("events")
	if ok {
		group.Pubsub = NewFamilyDTO(eventsFamily)
		group.inBaseValues.Add(group.Pubsub.inBaseValues)
		group.outBaseValues.Add(group.Pubsub.outBaseValues)
		group.ActiveChannels += group.Pubsub.ActiveChannels
		group.Clients += group.Pubsub.Clients
		if group.LastActivity < group.Pubsub.LastActivity {
			group.LastActivity = group.Pubsub.LastActivity
		}
	}
	eventsStoreFamily, ok := entitiesGroup.GetFamily("events_store")
	if ok {
		group.Pubsub.Add(NewFamilyDTO(eventsStoreFamily))
		group.inBaseValues.Add(group.Pubsub.inBaseValues)
		group.outBaseValues.Add(group.Pubsub.outBaseValues)
		group.ActiveChannels += group.Pubsub.ActiveChannels
		group.Clients += group.Pubsub.Clients
		if group.LastActivity < group.Pubsub.LastActivity {
			group.LastActivity = group.Pubsub.LastActivity
		}
	}
	group.Channels += int64(len(group.Pubsub.ChannelsList))
	group.Pubsub.Name = "pubsub"
	commandsFamily, ok := entitiesGroup.GetFamily("commands")
	if ok {
		group.RequestReply = NewFamilyDTO(commandsFamily)
		group.inBaseValues.Add(group.RequestReply.inBaseValues)
		group.outBaseValues.Add(group.RequestReply.outBaseValues)
		group.ActiveChannels += group.RequestReply.ActiveChannels
		group.Clients += group.RequestReply.Clients
		if group.LastActivity < group.RequestReply.LastActivity {
			group.LastActivity = group.RequestReply.LastActivity
		}
	}

	queriesFamily, ok := entitiesGroup.GetFamily("queries")
	if ok {
		group.RequestReply.Add(NewFamilyDTO(queriesFamily))
		group.inBaseValues.Add(group.RequestReply.inBaseValues)
		group.outBaseValues.Add(group.RequestReply.outBaseValues)
		group.ActiveChannels += group.RequestReply.ActiveChannels
		group.Clients += group.RequestReply.Clients
		if group.LastActivity < group.RequestReply.LastActivity {
			group.LastActivity = group.RequestReply.LastActivity
		}
	}
	group.Channels += int64(len(group.RequestReply.ChannelsList))
	group.RequestReply.Name = "request_reply"

	group.Incoming = NewBaseValuesDTOFromBaseValues(group.inBaseValues)
	group.Outgoing = NewBaseValuesDTOFromBaseValues(group.outBaseValues)
	group.Total = NewBaseValuesDTOFromBaseValues(group.inBaseValues.CombineWIth(group.outBaseValues))
	group.LastActivityHuman = humanize.Time(time.UnixMilli(group.LastActivity))
	group.ChannelsHuman = humanize.Comma(group.Channels)
	group.ClientsHuman = humanize.Comma(group.Clients)
	group.UpdateStatCards()
	return group
}

func (g *GroupDTO) UpdateStatCards() *GroupDTO {
	g.StatsCards.AddChannels(
		fmt.Sprintf("%d", g.ActiveChannels),
		fmt.Sprintf("%d", g.Channels),
		g.Queues,
		g.Pubsub,
		g.RequestReply,
	)
	g.StatsCards.AddClients(
		fmt.Sprintf("%d", g.Clients),
		g.Queues,
		g.Pubsub,
		g.RequestReply,
	)
	g.StatsCards.AddIncoming(
		g.Incoming.MessagesHumanized,
		g.Incoming.VolumeHumanized,
		g.Queues,
		g.Pubsub,
		g.RequestReply)

	g.StatsCards.AddOutgoing(
		g.Outgoing.MessagesHumanized,
		g.Outgoing.VolumeHumanized,
		g.Queues,
		g.Pubsub,
		g.RequestReply)
	return g
}
