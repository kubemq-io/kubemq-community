package authorization

import (
	"context"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestController_Enforce(t *testing.T) {
	defer leaktest.Check(t)()
	tests := []struct {
		name       string
		accessList AccessControlList
		input      []interface{}
		ok         bool
		isError    bool
	}{
		{
			name: "grant_read_access_full",
			accessList: AccessControlList{
				{
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					ClientID:    ".*",
					Channel:     ".*",
					Read:        true,
					Write:       false,
				},
			},
			input:   []interface{}{"queues", "alice", "somequeue", "read"},
			ok:      true,
			isError: false,
		},

		{
			name: "grant_read_write_access_full_with_Read",
			accessList: AccessControlList{
				{
					ClientID:    ".*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        true,
					Write:       true,
				},
			},
			input:   []interface{}{"queues", "alice", "somequeue", "read"},
			ok:      true,
			isError: false,
		},
		{
			name: "grant_read_write_access_full_with_write",
			accessList: AccessControlList{
				{
					ClientID:    ".*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        true,
					Write:       true,
				},
			},
			input:   []interface{}{"queues", "alice", "somequeue", "write"},
			ok:      true,
			isError: false,
		},
		{
			name: "deny_read_write_access_full_with_write",
			accessList: AccessControlList{
				{
					ClientID:    ".*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        false,
					Write:       false,
				},
			},
			input:   []interface{}{"queues", "alice", "somequeue", "write"},
			ok:      false,
			isError: false,
		},
		{
			name: "deny_read_write_access_full_with_write",
			accessList: AccessControlList{
				{
					ClientID:    ".*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        false,
					Write:       false,
				},
			},
			input:   []interface{}{"queues", "alice", "somequeue", "read"},
			ok:      false,
			isError: false,
		},
		{
			name: "grant_read_access_only_to_one_client_error",
			accessList: AccessControlList{
				{
					ClientID:    "client",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        true,
					Write:       false,
				},
			},
			input:   []interface{}{"queues", "alice", "somequeue", "read"},
			ok:      false,
			isError: false,
		},
		{
			name: "grant_read_access_only_to_one_client_ok",
			accessList: AccessControlList{
				{
					ClientID:    "client",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        true,
					Write:       false,
				},
			},

			input:   []interface{}{"queues", "client", "somequeue", "read"},
			ok:      true,
			isError: false,
		},
		{
			name: "grant_read_access_with_read_rule",
			accessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        true,
					Write:       false,
				},
			},
			input:   []interface{}{"queues", "alice", "somequeue", "read"},
			ok:      true,
			isError: false,
		},
		{
			name: "grant_write_access_with_write_rule",
			accessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        false,
					Write:       true,
				},
			},
			input:   []interface{}{"queues", "alice", "somequeue", "write"},
			ok:      true,
			isError: false,
		},
		{
			name: "allow_read_access_with_*_wildcards_1",
			accessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     "foo*",
					Read:        true,
					Write:       false,
				},
			},
			input:   []interface{}{"queues", "alice", "foo*", "read"},
			ok:      true,
			isError: false,
		},
		{
			name: "allow_read_access_with_*_wildcards_2",
			accessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        true,
					Write:       false,
				},
			},

			input:   []interface{}{"queues", "alice", ">", "read"},
			ok:      true,
			isError: false,
		},
		{
			name: "allow_read_access_with_client_regex",

			accessList: AccessControlList{
				{
					ClientID:    "(alice|tom)",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        true,
					Write:       false,
				},
			},
			input:   []interface{}{"queues", "tom", ">", "read"},
			ok:      true,
			isError: false,
		},
		{
			name: "deny_read_access",
			accessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     "foo*",
					Read:        true,
					Write:       false,
				},
			},
			input:   []interface{}{"queues", "alice", "bar", "read"},
			ok:      false,
			isError: false,
		},
		{
			name: "deny_access_bad_policy",
			accessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     "foo.bar/*",
					Read:        true,
					Write:       false,
				},
			},
			input:   []interface{}{"queues", "alice", "bar", "read"},
			ok:      false,
			isError: false,
		},
		{
			name: "deny_access_bad_clientid",
			accessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        true,
					Write:       false,
				},
			},
			input:   []interface{}{"queues", "tom", "bar", "read"},
			ok:      false,
			isError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acl, err := NewController(context.Background(), NewMemoryProvider(tt.accessList.Marshal()), 0)
			require.NoError(t, err)

			res, err := acl.Enforce(tt.input...)
			if tt.isError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.ok, res)
			acl.Close()
		})
	}
}

func TestController_ReloadPolicy(t *testing.T) {
	defer leaktest.Check(t)()
	tests := []struct {
		name          string
		accessList    AccessControlList
		newAccessList AccessControlList
		input         []interface{}
		ok1           bool
		ok2           bool
		isError       bool
	}{
		{
			name: "grant_after_reload",
			accessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        true,
					Write:       false,
				},
			},
			newAccessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        false,
					Write:       true,
				},
			},
			input:   []interface{}{"queues", "alice", "somequeue", "write"},
			ok1:     false,
			ok2:     true,
			isError: false,
		},
		{
			name: "deny_after_reload",
			accessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        true,
					Write:       false,
				},
			},

			newAccessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     false,
					Queues:      true,
					Channel:     ".*",
					Read:        false,
					Write:       true,
				},
			},
			input:   []interface{}{"queues", "alice", "somequeue", "read"},
			ok1:     true,
			ok2:     false,
			isError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMemoryProvider(tt.accessList.Marshal())
			acl, err := NewController(context.Background(), m, 0)
			require.NoError(t, err)
			acl.Close()
			res, err := acl.Enforce(tt.input...)
			require.NoError(t, err)
			require.Equal(t, tt.ok1, res)
			m.data = tt.newAccessList.Marshal()
			err = acl.ReloadPolicy()
			require.NoError(t, err)
			res, err = acl.Enforce(tt.input...)
			if tt.isError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.ok2, res)
			acl.Close()
		})
	}
}

func TestController_ReloadPolicyReloadInterval(t *testing.T) {
	defer leaktest.Check(t)()
	tests := []struct {
		name          string
		accessList    AccessControlList
		newAccessList AccessControlList
		input         []interface{}
		ok1           bool
		ok2           bool
		isError       bool
	}{
		{
			name: "grant_after_reload",
			accessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: true,
					Commands:    true,
					Queries:     false,
					Queues:      false,
					Channel:     ".*",
					Read:        true,
					Write:       false,
				},
			},

			newAccessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: true,
					Commands:    true,
					Queries:     false,
					Queues:      false,
					Channel:     ".*",
					Read:        false,
					Write:       true,
				},
			},
			input:   []interface{}{"commands", "alice", "somequeue", "write"},
			ok1:     false,
			ok2:     true,
			isError: false,
		},
		{
			name: "deny_after_reload",
			accessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     true,
					Queues:      false,
					Channel:     ".*",
					Read:        true,
					Write:       false,
				},
			},

			newAccessList: AccessControlList{
				{
					ClientID:    "alice*",
					Events:      false,
					EventsStore: false,
					Commands:    false,
					Queries:     true,
					Queues:      false,
					Channel:     ".*",
					Read:        false,
					Write:       true,
				},
			},
			input:   []interface{}{"queries", "alice", "somequeue", "read"},
			ok1:     true,
			ok2:     false,
			isError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMemoryProvider(tt.accessList.Marshal())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			acl, err := NewController(ctx, m, time.Second)
			require.NoError(t, err)
			acl.Close()
			res, err := acl.Enforce(tt.input...)
			require.NoError(t, err)
			require.Equal(t, tt.ok1, res)
			m.data = tt.newAccessList.Marshal()
			time.Sleep(2 * time.Second)
			res, err = acl.Enforce(tt.input...)
			if tt.isError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.ok2, res)

		})
	}
}
func TestController_BadEnforce(t *testing.T) {
	defer leaktest.Check(t)()
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
	m := NewMemoryProvider(accessList.Marshal())
	acl, err := NewController(context.Background(), m, 0)
	require.NoError(t, err)
	_, err = acl.Enforce(nil)
	require.Error(t, err)
}
func TestController_BadReload(t *testing.T) {
	defer leaktest.Check(t)()
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
	m := NewMemoryProvider(accessList.Marshal())
	acl, err := NewController(context.Background(), m, 0)
	require.NoError(t, err)
	m.data = nil
	err = acl.ReloadPolicy()
	require.Error(t, err)
}
