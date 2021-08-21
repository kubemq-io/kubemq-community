package config

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func Test_getConfigFromEnv(t *testing.T) {
	*configFile = ""
	err := os.Setenv("LOG_LEVEL", "debug")

	require.NoError(t, err)
	defer os.Remove("config.yaml")
	c := GetAppConfig("./")
	require.Equal(t, "debug", string(c.Log.Level))
}
