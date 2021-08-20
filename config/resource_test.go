package config

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func Test_ResourceGetFromData(t *testing.T) {
	data := "some-data"
	r := &ResourceConfig{
		Filename: "",
		Data:     data,
	}
	wantData, err := r.Get()
	require.NoError(t, err)
	require.EqualValues(t, []byte(data), wantData)
}

func Test_ResourceGetFromFile(t *testing.T) {
	data := "some-data"
	r := &ResourceConfig{
		Filename: "data",
		Data:     "",
	}
	err := ioutil.WriteFile("data", []byte(data), 0600)
	require.NoError(t, err)
	defer os.Remove("data")
	wantData, err := r.Get()
	require.NoError(t, err)
	require.EqualValues(t, []byte(data), wantData)
}

func Test_ResourceEmpty(t *testing.T) {

	r := &ResourceConfig{
		Filename: "data",
		Data:     "",
	}
	require.False(t, r.Empty())
	r.Filename = ""
	r.Data = "some-data"
	require.False(t, r.Empty())
	r.Data = ""
	require.True(t, r.Empty())
}
