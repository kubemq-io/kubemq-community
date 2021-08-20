package routing

import (
	"context"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestController_FileProvider(t *testing.T) {
	table := []*RouteTableEntry{
		{
			Key:    "routeKey",
			Routes: "foo.bar",
		},
	}

	route := NewRoute().Parse("route:routeKey")
	err := ioutil.WriteFile("route.json", marshal(table), 0600)
	require.NoError(t, err)
	defer os.Remove("route.json")
	f := NewFileProvider("route.json")
	controller, err := NewController(context.Background(), f, 0)
	require.NoError(t, err)
	defer controller.Close()
	controller.RouteTable.Parse(route)
	require.Equal(t, 1, len(route.RoutesMap[""]))
	require.Equal(t, route.RoutesMap[""][0], "foo.bar")

}

func TestController_FileProviderReload(t *testing.T) {

	table := []*RouteTableEntry{
		{
			Key:    "routeKey",
			Routes: "foo.bar",
		},
	}

	route := NewRoute().Parse("route:routeKey")
	err := ioutil.WriteFile("route.json", marshal(table), 0600)
	require.NoError(t, err)
	defer os.Remove("route.json")
	f := NewFileProvider("route.json")
	controller, err := NewController(context.Background(), f, 0)
	require.NoError(t, err)
	defer controller.Close()
	controller.RouteTable.Parse(route)
	require.Equal(t, 1, len(route.RoutesMap[""]))
	require.Equal(t, route.RoutesMap[""][0], "foo.bar")

	newtable := []*RouteTableEntry{
		{
			Key:    "routeKey",
			Routes: "bar.foo",
		},
	}

	newRoute := NewRoute().Parse("route:routeKey")
	err = ioutil.WriteFile("route.json", marshal(newtable), 0600)
	require.NoError(t, err)
	err = controller.ReloadTable()
	require.NoError(t, err)

	controller.RouteTable.Parse(newRoute)
	require.Equal(t, 1, len(newRoute.RoutesMap[""]))
	require.Equal(t, newRoute.RoutesMap[""][0], "bar.foo")

}

func TestController_FileProviderBadPath(t *testing.T) {
	f := NewFileProvider("/badPaths.csv")
	_, err := NewController(context.Background(), f, 0)
	require.Error(t, err)

}

func TestController_FileProviderBadFormat(t *testing.T) {
	policy := "p,alice*,queues/*"
	err := ioutil.WriteFile("/badformat.csv", []byte(policy), 0600)
	require.NoError(t, err)
	f := NewFileProvider("/badformat.csv")
	_, err = NewController(context.Background(), f, 0)
	require.Error(t, err)

}

func TestController_FileProviderValidationError(t *testing.T) {
	table := []*RouteTableEntry{
		{
			Key:    "",
			Routes: "foo.bar",
		},
	}
	err := ioutil.WriteFile("route.json", marshal(table), 0600)
	require.NoError(t, err)
	defer os.Remove("route.json")
	f := NewFileProvider("route.json")
	_, err = NewController(context.Background(), f, 0)
	require.Error(t, err)

}
