package metrics

import (
	"sort"
)

type ChannelStats struct {
	Kind          string  `json:"kind"`
	Name          string  `json:"name"`
	TotalMessages float64 `json:"total_messages"`
	TotalVolume   float64 `json:"total_volume"`
	TotalErrors   float64 `json:"total_errors"`
}

type ChannelsSummery struct {
	Kind          string  `json:"kind"`
	TotalChannels int     `json:"total_channels"`
	TotalMessages float64 `json:"total_messages"`
	TotalVolume   float64 `json:"total_volume"`
	TotalErrors   float64 `json:"total_errors"`
}

func toChannelStats(stats []*Stats) []*ChannelStats {
	if len(stats) == 0 {
		return []*ChannelStats{}
	}

	m := map[string]*ChannelStats{}
	for _, stat := range stats {
		var cs *ChannelStats
		cs, ok := m[stat.Type+stat.Channel]
		if !ok {
			cs = &ChannelStats{
				Kind:          stat.Type,
				Name:          stat.Channel,
				TotalMessages: 0,
				TotalVolume:   0,
				TotalErrors:   0,
			}
		}

		switch stat.Kind() {
		case "messages_count":
			cs.TotalMessages += stat.Float64()
		case "messages_volume":
			cs.TotalVolume += stat.Float64()
		case "errors_count":
			cs.TotalErrors += stat.Float64()
		default:
			continue
		}
		m[stat.Type+stat.Channel] = cs
	}

	list := []*ChannelStats{}
	for _, cs := range m {
		list = append(list, cs)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].TotalMessages > list[j].TotalMessages
	})
	return list
}

func toChannelsSummery(stats []*ChannelStats) []*ChannelsSummery {
	if len(stats) == 0 {
		return []*ChannelsSummery{}
	}
	m := map[string]*ChannelsSummery{}
	for _, stat := range stats {
		var cs *ChannelsSummery
		cs, ok := m[stat.Kind]
		if !ok {
			cs = &ChannelsSummery{
				Kind:          stat.Kind,
				TotalChannels: 0,
				TotalMessages: 0,
				TotalVolume:   0,
				TotalErrors:   0,
			}
		}
		cs.TotalChannels++
		cs.TotalMessages += stat.TotalMessages
		cs.TotalErrors += stat.TotalErrors
		cs.TotalVolume += stat.TotalVolume
		m[stat.Kind] = cs
	}

	list := []*ChannelsSummery{}
	for _, cs := range m {
		list = append(list, cs)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].TotalMessages > list[j].TotalMessages
	})
	return list
}
