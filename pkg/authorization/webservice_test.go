package authorization

import (
	"context"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestController_WebServiceProvider(t *testing.T) {
	accessList := AccessControlList{
		{
			ClientID:    ".*",
			Events:      false,
			EventsStore: false,
			Commands:    false,
			Queries:     false,
			Queues:      true,
			Channel:     ".*",
			Read:        true,
			Write:       false,
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(accessList.Marshal())
	}))
	defer srv.Close()

	input := []interface{}{"queues", "alice", "somequeue", "read"}
	w := NewWebServiceProvider(srv.URL)

	acl, err := NewController(context.Background(), w, 0)
	require.NoError(t, err)
	acl.Close()
	res, err := acl.Enforce(input...)
	require.NoError(t, err)
	require.Equal(t, true, res)

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

func TestController_WebServiceProviderReturnInvalid(t *testing.T) {
	accessList := AccessControlList{
		{
			ClientID:    ".*",
			Events:      false,
			EventsStore: false,
			Commands:    false,
			Queries:     false,
			Queues:      true,
			Channel:     "*",
			Read:        true,
			Write:       false,
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(accessList.Marshal())
	}))
	defer srv.Close()

	w := NewWebServiceProvider(srv.URL)
	_, err := NewController(context.Background(), w, 0)
	require.Error(t, err)

}
