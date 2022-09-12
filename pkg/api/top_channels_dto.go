package api

import (
	"fmt"
	"github.com/dustin/go-humanize"
)

type TopChannelDTO struct {
	Type         string `json:"type"`
	Channel      string `json:"channel"`
	LastActivity string `json:"lastActivity"`
	Sent         string `json:"sent"`
	Delivered    string `json:"delivered"`
	Clients      string `json:"clients"`
	ChannelKey   string `json:"channelKey"`
}

func NewTopChannelDTO(channel *ChannelDTO) *TopChannelDTO {
	tc := &TopChannelDTO{
		Type:         transformType(channel.Type),
		Channel:      channel.Name,
		LastActivity: channel.LastActivityHuman,
		Sent:         fmt.Sprintf("%s/%s", channel.Incoming.MessagesHumanized, channel.Incoming.VolumeHumanized),
		Delivered:    fmt.Sprintf("%s/%s", channel.Outgoing.MessagesHumanized, channel.Outgoing.VolumeHumanized),
		Clients:      humanize.Comma(channel.Incoming.Clients + channel.Outgoing.Clients),
	}
	tc.ChannelKey = fmt.Sprintf("%s-%s", tc.Type, tc.Channel)
	return tc
}

func transformType(value string) string {
	switch value {
	case "queues":
		return "Queue"
	case "events":
		return "PubSub / Events"
	case "events_store":
		return "PubSub / Events-Store"
	case "commands":
		return "Commands"
	case "queries":
		return "Queries"
	default:
		return "Unknown"
	}
}
