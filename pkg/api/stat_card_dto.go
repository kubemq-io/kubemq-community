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
  Queues                            *StatCardSectionDTO `json:"queues"`
  PubSub                            *StatCardSectionDTO `json:"pubsub"`
  CommandsQueries                   *StatCardSectionDTO `json:"commands_queries"`
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
    Queues:               NewStatCardSectionDTO(),
    PubSub:               NewStatCardSectionDTO(),
    CommandsQueries:      NewStatCardSectionDTO(),
  }
}
func (s *StatCardDTO) AddQueuesFamily(primaryValue, secondaryValue int64) *StatCardDTO {
  s.Queues.SetTitle("Queues")
  s.primaryItemValue += primaryValue
  s.secondaryItemValue += secondaryValue
  s.queuesPrimaryItemValue = primaryValue
  s.queuesSecondaryItemValue = secondaryValue
  return s
}
func (s *StatCardDTO) AddPubSunFamily(primaryValue, secondaryValue int64) *StatCardDTO {
  s.PubSub.SetTitle("PubSub")
  s.primaryItemValue += primaryValue
  s.secondaryItemValue += secondaryValue
  s.pubsubPrimaryItemValue = primaryValue
  s.pubsubSecondaryItemValue = secondaryValue
  return s
}

func (s *StatCardDTO) AddCommandsQueriesFamily(primaryValue, secondaryValue int64) *StatCardDTO {
  s.CommandsQueries.SetTitle("Commands & Queries")
  s.primaryItemValue += primaryValue
  s.secondaryItemValue += secondaryValue
  s.commandsQueriesPrimaryItemValue = primaryValue
  s.commandsQueriesSecondaryItemValue = secondaryValue
  return s
}

type StatCardDTOs map[string]*StatCardDTO

func NewStatCardDTOs() *StatCardDTOs {
  return &StatCardDTOs{}
}
func (s *StatCardDTOs) AddChannels(active, total string, queues, pubsub, commandsQueries *FamilyDTO) *StatCardDTOs {
  sc := NewStatCardDTO("Active", active, "Total", total)
  sc.AddQueuesFamily(queues.ActiveChannels, queues.Channels)
  sc.AddPubSunFamily(pubsub.ActiveChannels, pubsub.Channels)
  sc.AddCommandsQueriesFamily(commandsQueries.ActiveChannels, commandsQueries.Channels)

  sc.Queues.SetCaption(fmt.Sprintf("(%d/%d)", sc.queuesPrimaryItemValue, sc.queuesSecondaryItemValue))
  sc.PubSub.SetCaption(fmt.Sprintf("(%d/%d)", sc.pubsubPrimaryItemValue, sc.pubsubSecondaryItemValue))
  sc.CommandsQueries.SetCaption(fmt.Sprintf("(%d/%d)", sc.commandsQueriesPrimaryItemValue, sc.commandsQueriesSecondaryItemValue))

  queuesRation := float64(sc.queuesSecondaryItemValue) / float64(sc.secondaryItemValue) * 100
  pubsubRation := float64(sc.pubsubSecondaryItemValue) / float64(sc.secondaryItemValue) * 100
  commandsQueriesRation := float64(sc.commandsQueriesSecondaryItemValue) / float64(sc.secondaryItemValue) * 100

  sc.Queues.SetValue(fmt.Sprintf("%.1f", queuesRation) + "%")
  sc.PubSub.SetValue(fmt.Sprintf("%.1f", pubsubRation) + "%")
  sc.CommandsQueries.SetValue(fmt.Sprintf("%.1f", commandsQueriesRation) + "%")
  (*s)["channels"] = sc
  return s
}
func (s *StatCardDTOs) AddOutgoing(messages, volume string, queues, pubsub, commandsQueries *FamilyDTO) *StatCardDTOs {
  sc := NewStatCardDTO("Messages", messages, "Volume", volume)
  sc.AddQueuesFamily(queues.Outgoing.Messages, queues.Outgoing.Volume)
  sc.AddPubSunFamily(pubsub.Outgoing.Messages, pubsub.Outgoing.Volume)
  sc.AddCommandsQueriesFamily(commandsQueries.Outgoing.Messages, commandsQueries.Outgoing.Volume)

  sc.Queues.SetCaption(fmt.Sprintf("(%s/%s)", queues.Outgoing.MessagesHumanized, queues.Outgoing.VolumeHumanized))
  sc.PubSub.SetCaption(fmt.Sprintf("(%s/%s)", pubsub.Outgoing.MessagesHumanized, pubsub.Outgoing.VolumeHumanized))
  sc.CommandsQueries.SetCaption(fmt.Sprintf("(%s/%s)", commandsQueries.Outgoing.MessagesHumanized, commandsQueries.Outgoing.VolumeHumanized))

  queuesRation := float64(queues.Outgoing.Volume) / float64(sc.secondaryItemValue) * 100
  pubsubRation := float64(pubsub.Outgoing.Volume) / float64(sc.secondaryItemValue) * 100
  commandsQueriesRation := float64(commandsQueries.Outgoing.Volume) / float64(sc.secondaryItemValue) * 100

  sc.Queues.SetValue(fmt.Sprintf("%.1f", queuesRation) + "%")
  sc.PubSub.SetValue(fmt.Sprintf("%.1f", pubsubRation) + "%")
  sc.CommandsQueries.SetValue(fmt.Sprintf("%.1f", commandsQueriesRation) + "%")

  (*s)["outgoing"] = sc
  return s
}
func (s *StatCardDTOs) AddIncoming(messages, volume string, queues, pubsub, commandsQueries *FamilyDTO) *StatCardDTOs {
  sc := NewStatCardDTO("Messages", messages, "Volume", volume)
  sc.AddQueuesFamily(queues.Incoming.Messages, queues.Incoming.Volume)
  sc.AddPubSunFamily(pubsub.Incoming.Messages, pubsub.Incoming.Volume)
  sc.AddCommandsQueriesFamily(commandsQueries.Incoming.Messages, commandsQueries.Incoming.Volume)

  sc.Queues.SetCaption(fmt.Sprintf("(%s/%s)", queues.Incoming.MessagesHumanized, queues.Incoming.VolumeHumanized))
  sc.PubSub.SetCaption(fmt.Sprintf("(%s/%s)", pubsub.Incoming.MessagesHumanized, pubsub.Incoming.VolumeHumanized))
  sc.CommandsQueries.SetCaption(fmt.Sprintf("(%s/%s)", commandsQueries.Incoming.MessagesHumanized, commandsQueries.Incoming.VolumeHumanized))

  queuesRation := float64(queues.Incoming.Volume) / float64(sc.secondaryItemValue) * 100
  pubsubRation := float64(pubsub.Incoming.Volume) / float64(sc.secondaryItemValue) * 100
  commandsQueriesRation := float64(commandsQueries.Incoming.Volume) / float64(sc.secondaryItemValue) * 100

  sc.Queues.SetValue(fmt.Sprintf("%.1f", queuesRation) + "%")
  sc.PubSub.SetValue(fmt.Sprintf("%.1f", pubsubRation) + "%")
  sc.CommandsQueries.SetValue(fmt.Sprintf("%.1f", commandsQueriesRation) + "%")

  (*s)["incoming"] = sc
  return s
}
func (s *StatCardDTOs) AddClients(total string, queues, pubsub, commandsQueries *FamilyDTO) *StatCardDTOs {
  sc := NewStatCardDTO("Total", total, "", "")
  sc.AddQueuesFamily(queues.Clients, 0)
  sc.AddPubSunFamily(pubsub.Clients, 0)
  sc.AddCommandsQueriesFamily(commandsQueries.Clients, 0)

  sc.Queues.SetCaption(fmt.Sprintf("(%d)", sc.queuesPrimaryItemValue))
  sc.PubSub.SetCaption(fmt.Sprintf("(%d)", sc.pubsubPrimaryItemValue))
  sc.CommandsQueries.SetCaption(fmt.Sprintf("(%d)", sc.commandsQueriesPrimaryItemValue))

  queuesRation := float64(sc.queuesPrimaryItemValue) / float64(sc.primaryItemValue) * 100
  pubsubRation := float64(sc.pubsubPrimaryItemValue) / float64(sc.primaryItemValue) * 100
  commandsQueriesRation := float64(sc.commandsQueriesPrimaryItemValue) / float64(sc.primaryItemValue) * 100

  sc.Queues.SetValue(fmt.Sprintf("%.1f", queuesRation) + "%")
  sc.PubSub.SetValue(fmt.Sprintf("%.1f", pubsubRation) + "%")
  sc.CommandsQueries.SetValue(fmt.Sprintf("%.1f", commandsQueriesRation) + "%")

  (*s)["clients"] = sc
  return s
}
