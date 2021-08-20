package routing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRouteTable_GetRoutes(t *testing.T) {

	tests := []struct {
		name  string
		table []*RouteTableEntry
		key   string
		want  []string
	}{
		{
			name: "one route, any key",
			table: []*RouteTableEntry{
				&RouteTableEntry{
					Key:    ".*",
					Routes: "foo.bar",
				},
			},
			key:  "anykey",
			want: []string{"foo.bar"},
		},
		{
			name: "one key, multiple routes",
			table: []*RouteTableEntry{
				&RouteTableEntry{
					Key:    ".*",
					Routes: "foo.bar",
				},
				&RouteTableEntry{
					Key:    "onekey",
					Routes: "bar.foo",
				},
			},
			key:  "onekey",
			want: []string{"foo.bar", "bar.foo"},
		},
		{
			name: "no match",
			table: []*RouteTableEntry{
				&RouteTableEntry{
					Key:    "s",
					Routes: "foo.bar",
				},
			},
			key:  "onekey",
			want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := newRouteTable(tt.table)
			routes := rt.getRoutes(tt.key)
			assert.EqualValues(t, tt.want, routes)
		})
	}
}
func TestRouteTable_Parse(t *testing.T) {

	tests := []struct {
		name            string
		table           []*RouteTableEntry
		channel         string
		wantBlank       []string
		wantEvents      []string
		wantEventsStore []string
		wantQueues      []string
	}{
		{
			name: "one route, any key",
			table: []*RouteTableEntry{
				&RouteTableEntry{
					Key:    ".*",
					Routes: "foo.bar",
				},
			},
			channel:         "foo.bar;route:anykey",
			wantBlank:       []string{"foo.bar"},
			wantEvents:      []string{},
			wantEventsStore: []string{},
			wantQueues:      []string{},
		},
		{
			name: "events and routes",
			table: []*RouteTableEntry{
				&RouteTableEntry{
					Key:    ".*",
					Routes: "foo.bar",
				},
			},
			channel:         "events:foo.bar;route:anykey",
			wantBlank:       []string{"foo.bar"},
			wantEvents:      []string{"foo.bar"},
			wantEventsStore: []string{},
			wantQueues:      []string{},
		},
		{
			name: "several routes",
			table: []*RouteTableEntry{
				&RouteTableEntry{
					Key:    ".*",
					Routes: "foo.bar",
				},
			},
			channel:         "route:foo.bar;route:anykey",
			wantBlank:       []string{"foo.bar"},
			wantEvents:      []string{},
			wantEventsStore: []string{},
			wantQueues:      []string{},
		}, {
			name: "route with route",
			table: []*RouteTableEntry{
				&RouteTableEntry{
					Key:    "foo",
					Routes: "events:foo",
				},
			},
			channel:         "route:foo",
			wantBlank:       []string{},
			wantEvents:      []string{"foo"},
			wantEventsStore: []string{},
			wantQueues:      []string{},
		},
		{
			name: "route with route",
			table: []*RouteTableEntry{
				&RouteTableEntry{
					Key:    "foo",
					Routes: "events:foo",
				},
				&RouteTableEntry{
					Key:    "bar",
					Routes: "events:bar",
				},
			},
			channel:         "route:foo;route:bar",
			wantBlank:       []string{},
			wantEvents:      []string{"foo", "bar"},
			wantEventsStore: []string{},
			wantQueues:      []string{},
		},
		{
			name: "routes with many routes",
			table: []*RouteTableEntry{
				&RouteTableEntry{
					Key:    "foo",
					Routes: "events:foo;foo",
				},
				&RouteTableEntry{
					Key:    "bar",
					Routes: "events:bar;bar;events_store:bar;queues:bar",
				},
			},
			channel:         "route:foo;route:bar;foo;bar;",
			wantBlank:       []string{"foo", "bar"},
			wantEvents:      []string{"foo", "bar"},
			wantEventsStore: []string{"bar"},
			wantQueues:      []string{"bar"},
		},
		{
			name: "routes with template",
			table: []*RouteTableEntry{
				&RouteTableEntry{
					Key:    ".*",
					Routes: "events:{1}.{2};{2};queues:{2};queues:{1};events_store:{1}{2}",
				},
			},
			channel:         "route:foo.bar",
			wantBlank:       []string{"bar"},
			wantEvents:      []string{"foo.bar"},
			wantEventsStore: []string{"foobar"},
			wantQueues:      []string{"bar", "foo"},
		},
		{
			name: "routes with template2",
			table: []*RouteTableEntry{
				&RouteTableEntry{
					Key:    "foo..*.bar",
					Routes: "events:{1}.{2};{2};queues:{2};queues:{1};events_store:{1}{2};{1}.{2}.{3}",
				},
			},
			channel:         "route:foo.some.bar",
			wantBlank:       []string{"some", "foo.some.bar"},
			wantEvents:      []string{"foo.some"},
			wantEventsStore: []string{"foosome"},
			wantQueues:      []string{"some", "foo"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := newRouteTable(tt.table)
			r := NewRoute().Parse(tt.channel)
			rt.Parse(r)
			assert.EqualValues(t, tt.wantBlank, r.RoutesMap[""])
			assert.EqualValues(t, tt.wantEvents, r.RoutesMap["events"])
			assert.EqualValues(t, tt.wantEventsStore, r.RoutesMap["events_store"])
			assert.EqualValues(t, tt.wantQueues, r.RoutesMap["queues"])
		})
	}
}
