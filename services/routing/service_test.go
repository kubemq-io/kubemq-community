package routing

import (
	"context"
	"encoding/json"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/routing"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestService_CreateRouteMessages_Events(t *testing.T) {
	tests := []struct {
		name       string
		channel    string
		routeTable []*routing.RouteTableEntry
		wantErr    bool
	}{
		{
			name:       "no entry",
			channel:    "",
			routeTable: nil,
			wantErr:    true,
		},
		{
			name:       "one channel entry",
			channel:    "foo.bar",
			routeTable: nil,
			wantErr:    false,
		},
		{
			name:       "two channels entries",
			channel:    "foo.bar;events:foo.bar.1",
			routeTable: nil,
			wantErr:    false,
		},
		{
			name:       "one channel entry event store, should be error",
			channel:    "events_store:foo.bar",
			routeTable: nil,
			wantErr:    true,
		},
		{
			name:       "one channel entry event and one channel entry event store",
			channel:    "events_store:foo.bar;foo.bar",
			routeTable: nil,
			wantErr:    false,
		},
		{
			name:       "one channel entry event and one channel entry event store and one channel entry queues",
			channel:    "events_store:foo.bar;foo.bar;queues:foo.bar.queues",
			routeTable: nil,
			wantErr:    false,
		},
		{
			name:    "with routing",
			channel: "foo.bar.2;events:foo.bar.3;events_store:foo.bar.2;queues:foo.bar.2;route:event_route",
			routeTable: []*routing.RouteTableEntry{{
				Key:    "event_route",
				Routes: "events_store:foo.bar;foo.bar;queues:foo.bar.queues",
			}},
			wantErr: false,
		},
		{
			name:    "with bad routing",
			channel: "route:event_bad_route",
			routeTable: []*routing.RouteTableEntry{{
				Key:    "event_route",
				Routes: "events_store:foo.bar;foo.bar;queues:foo.bar.queues",
			}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appConfig := &config.Config{}
			if tt.routeTable != nil {
				data, err := json.Marshal(tt.routeTable)
				appConfig = &config.Config{Routing: &config.RoutingConfig{
					Enable:     true,
					Data:       string(data),
					FilePath:   "",
					Url:        "",
					AutoReload: 0,
				}}
				require.NoError(t, err)
			}
			service, err := CreateRoutingService(context.Background(), appConfig)
			require.NoError(t, err)
			require.NotNil(t, service)
			rm, err := service.CreateRouteMessages(tt.channel, baseEvent)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				trm := &testRouterMessage{
					RouteMessages: rm,
				}
				trm.testEventsRouteMessagesResult(t)
			}

		})
	}
}

func TestService_CreateRouteMessages_EventsStore(t *testing.T) {
	tests := []struct {
		name       string
		channel    string
		routeTable []*routing.RouteTableEntry
		wantErr    bool
	}{
		{
			name:       "no entry",
			channel:    "",
			routeTable: nil,
			wantErr:    true,
		},
		{
			name:       "one channel entry",
			channel:    "foo.bar",
			routeTable: nil,
			wantErr:    false,
		},
		{
			name:       "two channels entries",
			channel:    "foo.bar;events_store:foo.bar.1",
			routeTable: nil,
			wantErr:    false,
		},
		{
			name:       "one channel entry events, should be an error",
			channel:    "events:foo.bar",
			routeTable: nil,
			wantErr:    true,
		},
		{
			name:       "one channel entry event and one channel entry event store",
			channel:    "events:foo.bar;foo.bar",
			routeTable: nil,
			wantErr:    false,
		},
		{
			name:       "one channel entry event and one channel entry event store and one channel entry queues",
			channel:    "events:foo.bar;foo.bar;queues:foo.bar.queues",
			routeTable: nil,
			wantErr:    false,
		},
		{
			name:    "with routing",
			channel: "foo.bar.2;events_store:foo.bar.3;events:foo.bar.2;queues:foo.bar.2;route:event_route",
			routeTable: []*routing.RouteTableEntry{{
				Key:    "event_route",
				Routes: "events_store:foo.bar;foo.bar;queues:foo.bar.queues",
			}},
			wantErr: false,
		},
		{
			name:    "with bad routing",
			channel: "route:event_bad_route",
			routeTable: []*routing.RouteTableEntry{{
				Key:    "event_route",
				Routes: "events_store:foo.bar;foo.bar;queues:foo.bar.queues",
			}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appConfig := &config.Config{}
			if tt.routeTable != nil {
				data, err := json.Marshal(tt.routeTable)
				appConfig = &config.Config{Routing: &config.RoutingConfig{
					Enable:     true,
					Data:       string(data),
					FilePath:   "",
					Url:        "",
					AutoReload: 0,
				}}
				require.NoError(t, err)
			}
			service, err := CreateRoutingService(context.Background(), appConfig)
			require.NoError(t, err)
			require.NotNil(t, service)
			rm, err := service.CreateRouteMessages(tt.channel, baseEventStore)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				trm := &testRouterMessage{
					RouteMessages: rm,
				}
				trm.testEventsStoreRouteMessagesResult(t)
			}

		})
	}
}

func TestService_CreateRouteMessages_MessageQueue(t *testing.T) {
	tests := []struct {
		name       string
		channel    string
		routeTable []*routing.RouteTableEntry
		wantErr    bool
	}{
		{
			name:       "no entry",
			channel:    "",
			routeTable: nil,
			wantErr:    true,
		},
		{
			name:       "one channel entry",
			channel:    "foo.bar",
			routeTable: nil,
			wantErr:    false,
		},
		{
			name:       "two channels entries",
			channel:    "foo.bar;queues:foo.bar.1",
			routeTable: nil,
			wantErr:    false,
		},
		{
			name:       "one channel entry events, should be an error",
			channel:    "events:foo.bar",
			routeTable: nil,
			wantErr:    true,
		},
		{
			name:       "one channel entry event and one channel entry event store",
			channel:    "events:foo.bar;foo.bar",
			routeTable: nil,
			wantErr:    false,
		},
		{
			name:       "one channel entry event and one channel entry event store and one channel entry queues",
			channel:    "events:foo.bar;foo.bar;queues:foo.bar.queues",
			routeTable: nil,
			wantErr:    false,
		},
		{
			name:    "with routing",
			channel: "foo.bar.2;events_store:foo.bar.3;events:foo.bar.2;queues:foo.bar.2;route:event_route",
			routeTable: []*routing.RouteTableEntry{{
				Key:    "event_route",
				Routes: "events_store:foo.bar;foo.bar;queues:foo.bar.queues",
			}},
			wantErr: false,
		},
		{
			name:    "with bad routing",
			channel: "route:event_bad_route",
			routeTable: []*routing.RouteTableEntry{{
				Key:    "event_route",
				Routes: "events_store:foo.bar;foo.bar;queues:foo.bar.queues",
			}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appConfig := &config.Config{}
			if tt.routeTable != nil {
				data, err := json.Marshal(tt.routeTable)
				appConfig = &config.Config{Routing: &config.RoutingConfig{
					Enable:     true,
					Data:       string(data),
					FilePath:   "",
					Url:        "",
					AutoReload: 0,
				}}
				require.NoError(t, err)
			}
			service, err := CreateRoutingService(context.Background(), appConfig)
			require.NoError(t, err)
			require.NotNil(t, service)
			rm, err := service.CreateRouteMessages(tt.channel, baseQueueMessage)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				trm := &testRouterMessage{
					RouteMessages: rm,
				}
				trm.testQueueMessagesRouteMessagesResult(t)
			}

		})
	}
}
