package routing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRoute_Parse(t *testing.T) {

	tests := []struct {
		name            string
		channel         string
		wantBlank       []string
		wantEvents      []string
		wantEventsStore []string
		wantQueues      []string
		wantRoutes      []string
	}{
		{
			name:            "single channel",
			channel:         "foo",
			wantBlank:       []string{"foo"},
			wantEvents:      []string{},
			wantEventsStore: []string{},
			wantQueues:      []string{},
			wantRoutes:      []string{},
		},
		{
			name:            "single channel with events prefix",
			channel:         "events:foo",
			wantBlank:       []string{},
			wantEvents:      []string{"foo"},
			wantEventsStore: []string{},
			wantQueues:      []string{},
			wantRoutes:      []string{},
		},
		{
			name:            "single channel with events_store prefix",
			channel:         "events_store:foo",
			wantBlank:       []string{},
			wantEvents:      []string{},
			wantEventsStore: []string{"foo"},
			wantQueues:      []string{},
			wantRoutes:      []string{},
		},
		{
			name:            "single channel with queues prefix",
			channel:         "queues:foo",
			wantBlank:       []string{},
			wantEvents:      []string{},
			wantEventsStore: []string{},
			wantQueues:      []string{"foo"},
			wantRoutes:      []string{},
		},
		{
			name:            "single channel with routes prefix",
			channel:         "route:foo",
			wantBlank:       []string{},
			wantEvents:      []string{},
			wantEventsStore: []string{},
			wantQueues:      []string{},
			wantRoutes:      []string{"foo"},
		},
		{
			name:            "multiple channels",
			channel:         "foo.bar;bar.foo;events:foo;events_store:bar;queues:bar;queues:foo;route:routes",
			wantBlank:       []string{"foo.bar", "bar.foo"},
			wantEvents:      []string{"foo"},
			wantEventsStore: []string{"bar"},
			wantQueues:      []string{"bar", "foo"},
			wantRoutes:      []string{"routes"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRoute().Parse(tt.channel)
			assert.EqualValues(t, tt.wantBlank, r.RoutesMap[""])
			assert.EqualValues(t, tt.wantEvents, r.RoutesMap["events"])
			assert.EqualValues(t, tt.wantEventsStore, r.RoutesMap["events_store"])
			assert.EqualValues(t, tt.wantQueues, r.RoutesMap["queues"])
			assert.EqualValues(t, tt.wantRoutes, r.RoutesMap["routes"])
		})
	}
}
func TestRoute_Default(t *testing.T) {

	tests := []struct {
		name        string
		channel     string
		wantDefault string
	}{
		{
			name:        "single channel",
			channel:     "foo",
			wantDefault: "foo",
		},
		{
			name:        "single channel with prefix",
			channel:     "events:foo",
			wantDefault: "foo",
		},
		{
			name:        "no match",
			channel:     "events_store:foo",
			wantDefault: "",
		},
		{
			name:        "match on multiple channels",
			channel:     "events_store:foo;bar",
			wantDefault: "bar",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewRoute().Parse(tt.channel).Default("events")
			assert.EqualValues(t, tt.wantDefault, d)
		})
	}
}
