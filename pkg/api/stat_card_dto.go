package api

import "fmt"

type StatCardSectionDTO struct {
	Title   string `json:"title"`
	Caption string `json:"caption"`
	Value   string `json:"value"`
}

func NewStatCardSectionDTO() *StatCardSectionDTO {
	return &StatCardSectionDTO{
		Title:   "",
		Caption: "",
		Value:   "",
	}
}
func (s *StatCardSectionDTO) SetTitle(value string) *StatCardSectionDTO {
	s.Title = value
	return s
}
func (s *StatCardSectionDTO) SetCaption(value string) *StatCardSectionDTO {
	s.Caption = value
	return s
}
func (s *StatCardSectionDTO) SetValue(value string) *StatCardSectionDTO {
	s.Value = value
	return s
}

type StatCardDTO struct {
	PrimaryItemCaption                string              `json:"primaryItemCaption"`
	PrimaryItemValue                  string              `json:"primaryItemValue"`
	SecondaryItemCaption              string              `json:"secondaryItemCaption,omitempty"`
	SecondaryItemValue                string              `json:"secondaryItemValue,omitempty"`
	Queues                            *StatCardSectionDTO `json:"queues,omitempty"`
	PubSub                            *StatCardSectionDTO `json:"pubsub,omitempty"`
	CommandsQueries                   *StatCardSectionDTO `json:"commandsQueries,omitempty"`
	primaryItemValue                  int64
	secondaryItemValue                int64
	queuesPrimaryItemValue            int64
	queuesSecondaryItemValue          int64
	pubsubPrimaryItemValue            int64
	pubsubSecondaryItemValue          int64
	commandsQueriesPrimaryItemValue   int64
	commandsQueriesSecondaryItemValue int64
}

func NewStatCardDTO(primaryCaption, primaryValue, secondaryCaption, secondaryValue string) *StatCardDTO {
	return &StatCardDTO{
		PrimaryItemCaption:   primaryCaption,
		PrimaryItemValue:     primaryValue,
		SecondaryItemCaption: secondaryCaption,
		SecondaryItemValue:   secondaryValue,
	}
}
func (s *StatCardDTO) AddQueuesFamily(primaryValue, secondaryValue int64) *StatCardDTO {
	s.Queues = NewStatCardSectionDTO()
	s.Queues.SetTitle("Queues")
	s.primaryItemValue += primaryValue
	s.secondaryItemValue += secondaryValue
	s.queuesPrimaryItemValue = primaryValue
	s.queuesSecondaryItemValue = secondaryValue
	return s
}
func (s *StatCardDTO) AddPubSubFamily(primaryValue, secondaryValue int64) *StatCardDTO {
	s.PubSub = NewStatCardSectionDTO()
	s.PubSub.SetTitle("PubSub")
	s.primaryItemValue += primaryValue
	s.secondaryItemValue += secondaryValue
	s.pubsubPrimaryItemValue = primaryValue
	s.pubsubSecondaryItemValue = secondaryValue
	return s
}

func (s *StatCardDTO) AddCommandsQueriesFamily(primaryValue, secondaryValue int64) *StatCardDTO {
	s.CommandsQueries = NewStatCardSectionDTO()
	s.CommandsQueries.SetTitle("Commands & Queries")
	s.primaryItemValue += primaryValue
	s.secondaryItemValue += secondaryValue
	s.commandsQueriesPrimaryItemValue = primaryValue
	s.commandsQueriesSecondaryItemValue = secondaryValue
	return s
}

type StatCardDTOs struct {
	Channels *StatCardDTO `json:"channels"`
	Outgoing *StatCardDTO `json:"outgoing"`
	Incoming *StatCardDTO `json:"incoming"`
	Clients  *StatCardDTO `json:"clients"`
}

func NewStatCardDTOs() *StatCardDTOs {
	return &StatCardDTOs{}
}
func (s *StatCardDTOs) AddChannels(active, total string, queues, pubsub, commandsQueries *FamilyDTO) *StatCardDTOs {
	sc := NewStatCardDTO("Active", active, "Total", total)
	sc.AddQueuesFamily(queues.ActiveChannels, queues.Channels)
	sc.AddPubSubFamily(pubsub.ActiveChannels, pubsub.Channels)
	sc.AddCommandsQueriesFamily(commandsQueries.ActiveChannels, commandsQueries.Channels)

	if sc.secondaryItemValue > 0 {
		sc.Queues.SetCaption(fmt.Sprintf("(%d/%d)", sc.queuesPrimaryItemValue, sc.queuesSecondaryItemValue))
		sc.PubSub.SetCaption(fmt.Sprintf("(%d/%d)", sc.pubsubPrimaryItemValue, sc.pubsubSecondaryItemValue))
		sc.CommandsQueries.SetCaption(fmt.Sprintf("(%d/%d)", sc.commandsQueriesPrimaryItemValue, sc.commandsQueriesSecondaryItemValue))

	} else {
		sc.Queues.SetCaption("(0/0)")
		sc.PubSub.SetCaption("(0/0)")
		sc.CommandsQueries.SetCaption("(0/0)")
	}

	if sc.secondaryItemValue > 0 {
		queuesRation := float64(sc.queuesSecondaryItemValue) / float64(sc.secondaryItemValue) * 100
		pubsubRation := float64(sc.pubsubSecondaryItemValue) / float64(sc.secondaryItemValue) * 100
		commandsQueriesRation := float64(sc.commandsQueriesSecondaryItemValue) / float64(sc.secondaryItemValue) * 100

		sc.Queues.SetValue(fmt.Sprintf("%.1f", queuesRation) + "%")
		sc.PubSub.SetValue(fmt.Sprintf("%.1f", pubsubRation) + "%")
		sc.CommandsQueries.SetValue(fmt.Sprintf("%.1f", commandsQueriesRation) + "%")
	} else {
		sc.Queues.SetValue("0%")
		sc.PubSub.SetValue("0%")
		sc.CommandsQueries.SetValue("0%")
	}
	s.Channels = sc
	return s
}
func (s *StatCardDTOs) AddOutgoing(messages, volume string, queues, pubsub, commandsQueries *FamilyDTO) *StatCardDTOs {
	sc := NewStatCardDTO("Messages", messages, "Volume", volume)
	sc.AddQueuesFamily(queues.Outgoing.Messages, queues.Outgoing.Volume)
	sc.AddPubSubFamily(pubsub.Outgoing.Messages, pubsub.Outgoing.Volume)
	sc.AddCommandsQueriesFamily(commandsQueries.Outgoing.Messages, commandsQueries.Outgoing.Volume)

	if queues.Outgoing.Messages > 0 {
		sc.Queues.SetCaption(fmt.Sprintf("(%s/%s)", queues.Outgoing.MessagesHumanized, queues.Outgoing.VolumeHumanized))
	} else {
		sc.Queues.SetCaption("(0/0)")
	}
	if pubsub.Outgoing.Messages > 0 {
		sc.PubSub.SetCaption(fmt.Sprintf("(%s/%s)", pubsub.Outgoing.MessagesHumanized, pubsub.Outgoing.VolumeHumanized))
	} else {
		sc.PubSub.SetCaption("(0/0)")
	}
	if commandsQueries.Outgoing.Messages > 0 {
		sc.CommandsQueries.SetCaption(fmt.Sprintf("(%s/%s)", commandsQueries.Outgoing.MessagesHumanized, commandsQueries.Outgoing.VolumeHumanized))
	} else {
		sc.CommandsQueries.SetCaption("(0/0)")
	}

	if sc.secondaryItemValue > 0 {
		queuesRation := float64(sc.queuesSecondaryItemValue) / float64(sc.secondaryItemValue) * 100
		pubsubRation := float64(sc.pubsubSecondaryItemValue) / float64(sc.secondaryItemValue) * 100
		commandsQueriesRation := float64(sc.commandsQueriesSecondaryItemValue) / float64(sc.secondaryItemValue) * 100
		sc.Queues.SetValue(fmt.Sprintf("%.1f", queuesRation) + "%")
		sc.PubSub.SetValue(fmt.Sprintf("%.1f", pubsubRation) + "%")
		sc.CommandsQueries.SetValue(fmt.Sprintf("%.1f", commandsQueriesRation) + "%")
	} else {
		sc.Queues.SetValue("0%")
		sc.PubSub.SetValue("0%")
		sc.CommandsQueries.SetValue("0%")
	}
	s.Outgoing = sc

	return s
}
func (s *StatCardDTOs) AddIncoming(messages, volume string, queues, pubsub, commandsQueries *FamilyDTO) *StatCardDTOs {
	sc := NewStatCardDTO("Messages", messages, "Volume", volume)
	sc.AddQueuesFamily(queues.Incoming.Messages, queues.Incoming.Volume)
	sc.AddPubSubFamily(pubsub.Incoming.Messages, pubsub.Incoming.Volume)
	sc.AddCommandsQueriesFamily(commandsQueries.Incoming.Messages, commandsQueries.Incoming.Volume)

	if queues.Incoming.Messages > 0 {
		sc.Queues.SetCaption(fmt.Sprintf("(%s/%s)", queues.Incoming.MessagesHumanized, queues.Incoming.VolumeHumanized))
	} else {
		sc.Queues.SetCaption("(0/0)")
	}
	if pubsub.Incoming.Messages > 0 {
		sc.PubSub.SetCaption(fmt.Sprintf("(%s/%s)", pubsub.Incoming.MessagesHumanized, pubsub.Incoming.VolumeHumanized))
	} else {
		sc.PubSub.SetCaption("(0/0)")
	}
	if commandsQueries.Incoming.Messages > 0 {
		sc.CommandsQueries.SetCaption(fmt.Sprintf("(%s/%s)", commandsQueries.Incoming.MessagesHumanized, commandsQueries.Incoming.VolumeHumanized))
	} else {
		sc.CommandsQueries.SetCaption("(0/0)")
	}

	if sc.secondaryItemValue > 0 {
		queuesRation := float64(sc.queuesSecondaryItemValue) / float64(sc.secondaryItemValue) * 100
		pubsubRation := float64(sc.pubsubSecondaryItemValue) / float64(sc.secondaryItemValue) * 100
		commandsQueriesRation := float64(sc.commandsQueriesSecondaryItemValue) / float64(sc.secondaryItemValue) * 100
		sc.Queues.SetValue(fmt.Sprintf("%.1f", queuesRation) + "%")
		sc.PubSub.SetValue(fmt.Sprintf("%.1f", pubsubRation) + "%")
		sc.CommandsQueries.SetValue(fmt.Sprintf("%.1f", commandsQueriesRation) + "%")
	} else {
		sc.Queues.SetValue("0%")
		sc.PubSub.SetValue("0%")
		sc.CommandsQueries.SetValue("0%")
	}

	s.Incoming = sc

	return s
}
func (s *StatCardDTOs) AddClients(total string, queues, pubsub, commandsQueries *FamilyDTO) *StatCardDTOs {
	sc := NewStatCardDTO("Total", total, "", "")
	sc.AddQueuesFamily(queues.Clients, 0)
	sc.AddPubSubFamily(pubsub.Clients, 0)
	sc.AddCommandsQueriesFamily(commandsQueries.Clients, 0)

	sc.Queues.SetCaption(fmt.Sprintf("(%d)", sc.queuesPrimaryItemValue))
	sc.PubSub.SetCaption(fmt.Sprintf("(%d)", sc.pubsubPrimaryItemValue))
	sc.CommandsQueries.SetCaption(fmt.Sprintf("(%d)", sc.commandsQueriesPrimaryItemValue))

	if sc.primaryItemValue > 0 {
		queuesRation := float64(sc.queuesPrimaryItemValue) / float64(sc.primaryItemValue) * 100
		pubsubRation := float64(sc.pubsubPrimaryItemValue) / float64(sc.primaryItemValue) * 100
		commandsQueriesRation := float64(sc.commandsQueriesPrimaryItemValue) / float64(sc.primaryItemValue) * 100
		sc.Queues.SetValue(fmt.Sprintf("%.1f", queuesRation) + "%")
		sc.PubSub.SetValue(fmt.Sprintf("%.1f", pubsubRation) + "%")
		sc.CommandsQueries.SetValue(fmt.Sprintf("%.1f", commandsQueriesRation) + "%")
	} else {
		sc.Queues.SetValue("0%")
		sc.PubSub.SetValue("0%")
		sc.CommandsQueries.SetValue("0%")
	}

	s.Clients = sc

	return s
}
