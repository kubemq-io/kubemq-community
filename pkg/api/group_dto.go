package api

type GroupDTO struct {
	ChannelsStats     *StatCardDTO  `json:"channelsStats"`
	IncomingStats     *StatCardDTO  `json:"incomingStats"`
	OutgoingStats     *StatCardDTO  `json:"outgoingStats"`
	Channels          []*ChannelDTO `json:"channels"`
	LastActivityHuman string        `json:"lastActivityHuman"`
}

func NewGroupDTO(family *FamilyDTO) *GroupDTO {
	g := &GroupDTO{}
	g.ChannelsStats = NewStatCardDTO("Active", family.ActiveChannelsHuman, "Total", family.ChannelsHuman)
	g.IncomingStats = NewStatCardDTO("Messages", family.Incoming.MessagesHumanized, "Volume", family.Incoming.VolumeHumanized)
	g.OutgoingStats = NewStatCardDTO("Messages", family.Outgoing.MessagesHumanized, "Volume", family.Outgoing.VolumeHumanized)
	g.Channels = family.ChannelsList
	g.LastActivityHuman = family.LastActivityHuman
	return g
}
