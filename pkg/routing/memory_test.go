package routing

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestController_MemoryProvider(t *testing.T) {
	table := []*RouteTableEntry{
		{
			Key:    "routeKey",
			Routes: "foo.bar",
		},
	}
	route := NewRoute().Parse("route:routeKey")
	m := NewMemoryProvider(marshal(table))
	controller, err := NewController(context.Background(), m, 0)
	require.NoError(t, err)
	defer controller.Close()
	controller.RouteTable.Parse(route)
	require.Equal(t, 1, len(route.RoutesMap[""]))
	require.Equal(t, route.RoutesMap[""][0], "foo.bar")
}

func TestController_MemoryProviderBadAccessList(t *testing.T) {
	m := NewMemoryProvider([]byte("some-bad-data"))
	_, err := NewController(context.Background(), m, 0)
	require.Error(t, err)
}

func TestController_MemoryProviderValidationError(t *testing.T) {
	table := []*RouteTableEntry{
		{
			Key:    "",
			Routes: "foo.bar",
		},
	}
	m := NewMemoryProvider(marshal(table))
	_, err := NewController(context.Background(), m, 0)
	require.Error(t, err)

}
func TestController_MemoryProviderValidationBlankList(t *testing.T) {
	table := []*RouteTableEntry{}
	m := NewMemoryProvider(marshal(table))
	controller, err := NewController(context.Background(), m, 0)
	require.NoError(t, err)
	defer controller.Close()
	route := NewRoute().Parse("route:routeKey")
	controller.RouteTable.Parse(route)
	require.Equal(t, 0, len(route.RoutesMap[""]))

}
