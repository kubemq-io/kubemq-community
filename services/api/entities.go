package api

import (
	"github.com/kubemq-io/kubemq-community/services/metrics"
	"time"
)

type ChannelsResponse struct {
	Host       string
	LastUpdate time.Time
	Channels   []*metrics.ChannelStats
	Summery    []*metrics.ChannelsSummery
}
type ClientResponse struct {
	Host          string
	LastUpdate    time.Time
	TotalClients  int
	TotalMessages float64
	TotalVolume   float64
	TotalErrors   float64
	TotalOnline   int
	Clients       []*metrics.ClientsStats
}

func (cr *ClientResponse) calcSum() *ClientResponse {
	if len(cr.Clients) == 0 {
		return cr
	}
	for _, client := range cr.Clients {
		cr.TotalClients++
		cr.TotalMessages += client.TotalMessages
		cr.TotalErrors += client.TotalErrors
		cr.TotalVolume += client.TotalVolume
	}
	return cr
}
