package metrics

import (
	"strconv"
	"strings"
)

type Stats struct {
	Name     string
	Node     string
	Type     string
	Side     string
	Channel  string
	ClientId string
	Value    string
}

func toStats(mf []*Family) []*Stats {
	var list []*Stats
	for _, family := range mf {
		if strings.Contains(family.Name, "kubemq") {
			for _, metric := range family.Metrics {
				switch v := metric.(type) {
				case Metric:
					st := &Stats{
						Name:     strings.Replace(family.Name, "kubemq_", "", -1),
						Node:     v.Labels["node"],
						Type:     v.Labels["type"],
						Side:     v.Labels["side"],
						Channel:  v.Labels["channel"],
						ClientId: v.Labels["client_id"],
						Value:    v.Value,
					}
					list = append(list, st)
				}
			}
		}
	}
	return list
}

func (s *Stats) Float64() float64 {
	v, err := strconv.ParseFloat(s.Value, 64)
	if err != nil {

		return 0
	}
	return v
}
func (s *Stats) Int64() int64 {
	v, err := strconv.ParseInt(s.Value, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func (s *Stats) Kind() string {
	if strings.Contains(s.Name, "messages_count") {
		return "messages_count"
	}
	if strings.Contains(s.Name, "messages_volume") {
		return "messages_volume"
	}
	if strings.Contains(s.Name, "errors_count") {
		return "errors_count"
	}
	if strings.Contains(s.Name, "clients_count") {
		return "clients_count"
	}
	if strings.Contains(s.Name, "messages_pending") {
		return "messages_pending"
	}
	return ""
}
