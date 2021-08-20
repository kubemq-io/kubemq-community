package api

import (
	"context"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	"github.com/labstack/echo/v4"
)

func (s *Server) getQueueHandler(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)

	queues, err := s.broker.GetQueues(ctx)
	if err != nil {
		return res.SetError(err).Send()
	}

	return res.SetResponseBody(queues).Send()
}

func (s *Server) getEventsStoresHandler(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)

	eventsStores, err := s.broker.GetEventsStores(ctx)
	if err != nil {
		return res.SetError(err).Send()
	}

	return res.SetResponseBody(eventsStores).Send()
}
func (s *Server) getChannelsHandler(c echo.Context) error {
	res := NewResponse(c)
	channelsResponse := &ChannelsResponse{
		Host:       config.Host(),
		LastUpdate: metrics.LastUpdate(),
		Channels:   nil,
		Summery:    nil,
	}
	var err error
	channelsResponse.Channels, err = s.metricsExporter.Channels()
	if err != nil {
		return res.SetError(err).Send()
	}
	channelsResponse.Summery, err = s.metricsExporter.ChannelSummery(channelsResponse.Channels)
	if err != nil {
		return res.SetError(err).Send()
	}
	return res.SetResponseBody(channelsResponse).Send()
}

func (s *Server) getClientsHandler(c echo.Context) error {
	res := NewResponse(c)
	clientResponse := &ClientResponse{
		Host:       config.Host(),
		LastUpdate: metrics.LastUpdate(),
		Clients:    nil,
	}
	var err error
	clientResponse.Clients, clientResponse.TotalOnline, err = s.metricsExporter.Clients()
	if err != nil {
		return res.SetError(err).Send()
	}
	return res.SetResponseBody(clientResponse.calcSum()).Send()
}
