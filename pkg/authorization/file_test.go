package authorization

import (
	"context"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestController_FileProvider(t *testing.T) {
	accessList := AccessControlList{
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
	}

	input := []interface{}{"queues", "alice", "somequeue", "read"}
	err := ioutil.WriteFile("policyfile.json", accessList.Marshal(), 0600)
	require.NoError(t, err)
	f := NewFileProvider("policyfile.json")
	acl, err := NewController(context.Background(), f, 0)
	require.NoError(t, err)
	defer acl.Close()
	res, err := acl.Enforce(input...)
	require.NoError(t, err)

	require.Equal(t, true, res)

}

func TestController_FileProviderReload(t *testing.T) {
	accessList := AccessControlList{
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
	}
	input := []interface{}{"queues", "alice", "somequeue", "read"}
	err := ioutil.WriteFile("policyfile.json", accessList.Marshal(), 0600)
	require.NoError(t, err)
	defer os.Remove("policyfile.json")
	f := NewFileProvider("policyfile.json")
	acl, err := NewController(context.Background(), f, 0)
	require.NoError(t, err)
	defer acl.Close()
	res, err := acl.Enforce(input...)
	require.NoError(t, err)
	require.Equal(t, true, res)
	input = []interface{}{"queues", "alice", "somequeue", "write"}
	newAccessList := AccessControlList{
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
	}

	err = ioutil.WriteFile("policyfile.json", newAccessList.Marshal(), 0600)
	require.NoError(t, err)
	err = acl.ReloadPolicy()
	require.NoError(t, err)
	res, err = acl.Enforce(input...)
	require.NoError(t, err)
	require.Equal(t, true, res)
	acl.Close()
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
	accessList := AccessControlList{
		{
			ClientID:    "alice*",
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
	err := ioutil.WriteFile("validation_error.json", accessList.Marshal(), 0600)
	require.NoError(t, err)
	defer os.Remove("validation_error.json")
	f := NewFileProvider("validation_error.json")
	_, err = NewController(context.Background(), f, 0)
	require.Error(t, err)
}
