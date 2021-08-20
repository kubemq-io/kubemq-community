package routing

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestController_GetRoutes(t *testing.T) {
	table := []*RouteTableEntry{
		{
			Key:    "routeKey",
			Routes: "foo.bar",
		},
	}

	m := NewMemoryProvider(marshal(table))
	controller, err := NewController(context.Background(), m, 0)
	require.NoError(t, err)
	defer controller.Close()
	route := controller.GetRoutes("route:routeKey")
	require.Equal(t, 1, len(route.RoutesMap[""]))
	require.Equal(t, route.RoutesMap[""][0], "foo.bar")
}
