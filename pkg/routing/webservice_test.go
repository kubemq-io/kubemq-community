package routing

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func marshal(table []*RouteTableEntry) []byte {
	data, _ := json.Marshal(table)
	return data
}

func TestController_WebServiceProvider(t *testing.T) {
	table := []*RouteTableEntry{
		{
			Key:    "routeKey",
			Routes: "foo.bar",
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(marshal(table))
	}))
	defer srv.Close()

	route := NewRoute().Parse("route:routeKey")
	w := NewWebServiceProvider(srv.URL)

	controller, err := NewController(context.Background(), w, 0)
	require.NoError(t, err)
	defer controller.Close()
	controller.RouteTable.Parse(route)
	require.Equal(t, 1, len(route.RoutesMap[""]))
	require.Equal(t, route.RoutesMap[""][0], "foo.bar")

}

func TestController_WebServiceProviderInvalidResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("just bad response"))
	}))
	defer srv.Close()
	w := NewWebServiceProvider(srv.URL)
	_, err := NewController(context.Background(), w, 0)
	require.Error(t, err)

}
func TestController_WebServiceProviderErrorGet(t *testing.T) {
	w := NewWebServiceProvider("http://localhost:5555")
	_, err := NewController(context.Background(), w, 0)
	require.Error(t, err)
}
func TestController_WebServiceProviderReturnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		_, _ = w.Write([]byte("just bad response"))
	}))
	defer srv.Close()
	w := NewWebServiceProvider(srv.URL)
	_, err := NewController(context.Background(), w, 0)
	require.Error(t, err)

}

//
func TestController_WebServiceProviderReturnInvalid(t *testing.T) {
	table := []*RouteTableEntry{
		{
			Key:    "",
			Routes: "foo.bar",
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(marshal(table))
	}))
	defer srv.Close()

	w := NewWebServiceProvider(srv.URL)
	_, err := NewController(context.Background(), w, 0)
	require.Error(t, err)

}
