package metrics

import (
	"sort"
)

type ClientsStats struct {
	Name          string  `json:"name"`
	TotalMessages float64 `json:"total_messages"`
	TotalVolume   float64 `json:"total_volume"`
	TotalErrors   float64 `json:"total_errors"`
	TotalPending  float64 `json:"total_pending"`
}

func toClientStats(stats []*Stats) []*ClientsStats {
	if stats == nil {
		return []*ClientsStats{}
	}

	m := map[string]*ClientsStats{}
	for _, stat := range stats {
		if stat.ClientId == "" {
			continue
		}
		var cs *ClientsStats
		cs, ok := m[stat.ClientId]
		if !ok {
			cs = &ClientsStats{
				Name:          stat.ClientId,
				TotalMessages: 0,
				TotalVolume:   0,
				TotalErrors:   0,
				TotalPending:  0,
			}
		}

		switch stat.Kind() {
		case "messages_count":
			cs.TotalMessages += stat.Float64()
		case "messages_volume":
			cs.TotalVolume += stat.Float64()
		case "errors_count":
			cs.TotalErrors += stat.Float64()
		case "messages_pending":
			cs.TotalPending += stat.Float64()
		default:
			continue
		}
		m[stat.ClientId] = cs
	}

	list := []*ClientsStats{}
	for _, cs := range m {
		if !cs.Empty() {
			list = append(list, cs)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].TotalMessages > list[j].TotalMessages
	})
	return list
}

func (cs *ClientsStats) Empty() bool {
	return cs.TotalPending+cs.TotalErrors+cs.TotalVolume+cs.TotalMessages == 0
}

func getOnlineClients(stats []*Stats) int {
	cnt := 0
	for _, stat := range stats {
		if stat.Kind() == "clients_count" {
			cnt += int(stat.Float64())
		}

	}
	return cnt
}
