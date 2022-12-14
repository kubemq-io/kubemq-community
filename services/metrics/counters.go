package metrics

import (
	"strconv"
	"strings"
	"time"
)

type CountersController struct {
	lastReport     *Counters
	lastReportTime int64
	lastMessage    int64
}

func newCountersController() *CountersController {
	return &CountersController{
		lastReportTime: time.Now().UTC().Unix(),
		lastReport: &Counters{
			Hostname:    "",
			Uptime:      0,
			Messages:    0,
			Volume:      0,
			LastMessage: 0,
		},
	}
}

func (bc *CountersController) getCounters(mf []*Family) *Counters {
	b := toCounters(mf)
	result := &Counters{
		Hostname:    "",
		Uptime:      float64(time.Now().UTC().Unix() - bc.lastReportTime),
		Messages:    b.Messages - bc.lastReport.Messages,
		Volume:      b.Volume - bc.lastReport.Volume,
		LastMessage: 0,
	}
	if result.Messages > 0 {
		result.LastMessage = time.Now().UTC().Unix()
		bc.lastMessage = result.LastMessage
	} else {
		result.LastMessage = bc.lastMessage
	}
	bc.lastReport.Volume = b.Volume
	bc.lastReport.Messages = b.Messages
	bc.lastReport.Uptime = float64(time.Now().UTC().Unix() - bc.lastReportTime)
	bc.lastReportTime = time.Now().UTC().Unix()
	return result
}

type Counters struct {
	Hostname    string  `json:"hostname"`
	Uptime      float64 `json:"uptime"`
	Messages    int64   `json:"messages"`
	Volume      float64 `json:"volume"`
	LastMessage int64   `json:"last_message"`
}

func toFloat64(v string) float64 {
	val, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0
	}
	return val
}
func toInt64(v string) int64 {
	val, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0
	}
	return val
}
func toCounters(mf []*Family) *Counters {
	b := &Counters{}
	for _, family := range mf {
		if strings.Contains(family.Name, "kubemq") {
			for _, metric := range family.Metrics {
				switch v := metric.(type) {
				case Metric:
					if strings.Contains(family.Name, "messages_count") {
						b.Messages += toInt64(v.Value)
					}
					if strings.Contains(family.Name, "messages_volume") {
						b.Volume += toFloat64(v.Value)
					}
				}
			}
		}
	}
	return b
}
