package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// This specifically tests for loading values from the env
func Test_getConfigFromEnv(t *testing.T) {
	configFile = ""
	err := os.Setenv("LOG_LEVEL", "debug")

	require.NoError(t, err)
	defer os.Remove("config.yaml")
	c := GetAppConfig("./")
	require.Equal(t, "debug", string(c.Log.Level))
}

// Tests loading config file path from CONFIG env var
func Test_getConfigPathFromEnv(t *testing.T) {
	configFile = ""
	err := os.Setenv("CONFIG", "../config.yaml")
	require.NoError(t, err)
	c, err := getConfigFile()

	require.Contains(t, c, "config.yaml")
}
