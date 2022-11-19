package api

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/kubemq-io/kubemq-community/config"
	"sort"
	"time"
)

type SnapshotDTO struct {
	HostsInfo         []*HostInfoDTO   `json:"hostsInfo"`
	StatsCards        *StatCardDTOs    `json:"statsCards"`
	TopChannels       []*TopChannelDTO `json:"topChannels"`
	LastActivity      int64            `json:"lastActivity"`
	LastActivityHuman string           `json:"lastActivityHuman"`
	Total             *BaseValuesDTO   `json:"total"`
	Incoming          *BaseValuesDTO   `json:"incoming"`
	Outgoing          *BaseValuesDTO   `json:"outgoing"`
	Channels          int64            `json:"channels"`
	ChannelsHuman     string           `json:"channelsHuman"`
	Clients           int64            `json:"clients"`
	ClientsHuman      string           `json:"clientsHuman"`
	ActiveChannels    int64            `json:"activeChannels"`
	Queues            *FamilyDTO       `json:"queues"`
	Pubsub            *FamilyDTO       `json:"pubsub"`
	CommandsQueries   *FamilyDTO       `json:"commandsQueries"`
	IsCluster         bool             `json:"isCluster"`
	Node              string           `json:"node"`
	inBaseValues      *BaseValues
	outBaseValues     *BaseValues
}

func newSnapshot(systems []*System) *SnapshotDTO {
	var hostsInfo []*HostInfoDTO
	for _, system := range systems {
		hostsInfo = append(hostsInfo, NewHostInfoDTO(system))
	}
	sort.Slice(hostsInfo, func(i, j int) bool {
		return hostsInfo[i].Host < hostsInfo[j].Host
	})
	return &SnapshotDTO{
		HostsInfo:       hostsInfo,
		StatsCards:      NewStatCardDTOs(),
		Queues:          newFamilyDTO("queues"),
		Pubsub:          newFamilyDTO("pubsub"),
		CommandsQueries: newFamilyDTO("commandsQueries"),
		inBaseValues:    NewBaseValues(),
		outBaseValues:   NewBaseValues(),
		IsCluster:       false,
		Node:            config.GetHostname(),
	}
}
func NewSnapshotDTO(systems []*System, channelsEntitiesGroup *EntitiesGroup, clientsEntitiesGroup *EntitiesGroup) *SnapshotDTO {
	group := newSnapshot(systems)
	queueFamily, ok := channelsEntitiesGroup.GetFamily("queues")
	if ok {
		group.Queues = NewFamilyDTO(queueFamily, clientsEntitiesGroup)
		group.inBaseValues.Add(group.Queues.inBaseValues)
		group.outBaseValues.Add(group.Queues.outBaseValues)
		group.Channels += group.Queues.Channels
		group.ActiveChannels += group.Queues.ActiveChannels
		group.Clients += group.Queues.Clients
		if group.LastActivity < group.Queues.LastActivity {
			group.LastActivity = group.Queues.LastActivity
		}
	}

	eventsFamily, ok := channelsEntitiesGroup.GetFamily("events")
	if ok {
		group.Pubsub = NewFamilyDTO(eventsFamily, clientsEntitiesGroup)

	}
	eventsStoreFamily, ok := channelsEntitiesGroup.GetFamily("events_store")
	if ok {
		group.Pubsub.Add(NewFamilyDTO(eventsStoreFamily, clientsEntitiesGroup))
	}
	group.inBaseValues.Add(group.Pubsub.inBaseValues)
	group.outBaseValues.Add(group.Pubsub.outBaseValues)
	group.Clients += group.Pubsub.Clients
	group.ActiveChannels += group.Pubsub.ActiveChannels
	group.Channels += int64(len(group.Pubsub.ChannelsList))
	if group.LastActivity < group.Pubsub.LastActivity {
		group.LastActivity = group.Pubsub.LastActivity
	}
	group.Pubsub.Name = "pubsub"

	commandsFamily, ok := channelsEntitiesGroup.GetFamily("commands")
	if ok {
		group.CommandsQueries = NewFamilyDTO(commandsFamily, clientsEntitiesGroup)
	}

	queriesFamily, ok := channelsEntitiesGroup.GetFamily("queries")
	if ok {
		group.CommandsQueries.Add(NewFamilyDTO(queriesFamily, clientsEntitiesGroup))

	}
	group.inBaseValues.Add(group.CommandsQueries.inBaseValues)
	group.outBaseValues.Add(group.CommandsQueries.outBaseValues)
	group.Clients += group.CommandsQueries.Clients
	group.ActiveChannels += group.CommandsQueries.ActiveChannels
	if group.LastActivity < group.CommandsQueries.LastActivity {
		group.LastActivity = group.CommandsQueries.LastActivity
	}
	group.Channels += int64(len(group.CommandsQueries.ChannelsList))
	group.CommandsQueries.Name = "request_reply"

	group.Incoming = NewBaseValuesDTOFromBaseValues(group.inBaseValues)
	group.Outgoing = NewBaseValuesDTOFromBaseValues(group.outBaseValues)
	group.Total = NewBaseValuesDTOFromBaseValues(group.inBaseValues.CombineWIth(group.outBaseValues))
	group.LastActivityHuman = humanize.Time(time.UnixMilli(group.LastActivity))
	group.ChannelsHuman = humanize.Comma(group.Channels)
	group.ClientsHuman = humanize.Comma(group.Clients)
	group.UpdateStatCards()
	group.CreateTopChannels()
	return group
}

func (g *SnapshotDTO) UpdateStatCards() *SnapshotDTO {
	g.StatsCards.AddChannels(
		fmt.Sprintf("%d", g.ActiveChannels),
		fmt.Sprintf("%d", g.Channels),
		g.Queues,
		g.Pubsub,
		g.CommandsQueries,
	)
	g.StatsCards.AddClients(
		fmt.Sprintf("%d", g.Clients),
		g.Queues,
		g.Pubsub,
		g.CommandsQueries,
	)
	g.StatsCards.AddIncoming(
		g.Incoming.MessagesHumanized,
		g.Incoming.VolumeHumanized,
		g.Queues,
		g.Pubsub,
		g.CommandsQueries)

	g.StatsCards.AddOutgoing(
		g.Outgoing.MessagesHumanized,
		g.Outgoing.VolumeHumanized,
		g.Queues,
		g.Pubsub,
		g.CommandsQueries)
	return g
}

func (g *SnapshotDTO) CreateTopChannels() *SnapshotDTO {
	g.TopChannels = make([]*TopChannelDTO, 0)
	var channelList []*ChannelDTO
	channelList = append(channelList, g.Queues.ChannelsList...)
	channelList = append(channelList, g.Pubsub.ChannelsList...)
	channelList = append(channelList, g.CommandsQueries.ChannelsList...)
	sort.Slice(channelList, func(i, j int) bool {
		return channelList[i].LastActivity > channelList[j].LastActivity
	})
	if len(channelList) > 10 {
		channelList = channelList[:10]
	}
	for _, channel := range channelList {
		g.TopChannels = append(g.TopChannels, NewTopChannelDTO(channel))
	}
	return g
}
