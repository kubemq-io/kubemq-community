package authorization

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestController_MemoryProvider(t *testing.T) {
	accessList := AccessControlList{
		{
			ClientID:    ".*",
			Events:      true,
			EventsStore: false,
			Commands:    false,
			Queries:     false,
			Queues:      false,
			Channel:     ".*",
			Read:        true,
			Write:       false,
		},
	}

	input := []interface{}{"events", "alice", "somequeue", "read"}
	m := NewMemoryProvider(accessList.Marshal())

	acl, err := NewController(context.Background(), m, 0)
	require.NoError(t, err)
	res, err := acl.Enforce(input...)
	require.NoError(t, err)
	require.Equal(t, true, res)
	acl.Close()
}

func TestController_MemoryProviderBadAccessList(t *testing.T) {
	m := NewMemoryProvider([]byte("some-bad-data"))
	_, err := NewController(context.Background(), m, 0)
	require.Error(t, err)
}

func TestController_MemoryProviderValidationError(t *testing.T) {
	accessList := AccessControlList{
		{
			ClientID:    "*",
			Events:      true,
			EventsStore: false,
			Commands:    false,
			Queries:     false,
			Queues:      false,
			Channel:     "*",
			Read:        true,
			Write:       false,
		},
	}
	m := NewMemoryProvider(accessList.Marshal())
	_, err := NewController(context.Background(), m, 0)
	require.Error(t, err)
}
func TestController_MemoryProviderValidationBlankList(t *testing.T) {
	accessList := AccessControlList{}
	m := NewMemoryProvider(accessList.Marshal())
	_, err := NewController(context.Background(), m, 0)
	require.Error(t, err)
}
func TestController_MemoryProviderValidationWithOneBadRecord(t *testing.T) {
	accessList := AccessControlList{{}}
	m := NewMemoryProvider(accessList.Marshal())
	_, err := NewController(context.Background(), m, 0)
	require.Error(t, err)
}

func TestController_MemoryProviderValidationErrorBadClientID(t *testing.T) {
	accessList := AccessControlList{
		{
			ClientID:    "",
			Events:      true,
			EventsStore: false,
			Commands:    false,
			Queries:     false,
			Queues:      false,
			Channel:     ".*",
			Read:        true,
			Write:       false,
		},
	}
	m := NewMemoryProvider(accessList.Marshal())
	_, err := NewController(context.Background(), m, 0)
	require.Error(t, err)
}
func TestController_MemoryProviderValidationErrorBadChannel(t *testing.T) {
	accessList := AccessControlList{
		{
			ClientID:    ".*",
			Events:      true,
			EventsStore: false,
			Commands:    false,
			Queries:     false,
			Queues:      false,
			Channel:     "",
			Read:        true,
			Write:       false,
		},
	}
	m := NewMemoryProvider(accessList.Marshal())
	_, err := NewController(context.Background(), m, 0)
	require.Error(t, err)
}
