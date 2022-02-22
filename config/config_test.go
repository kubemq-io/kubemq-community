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

	require.NoError(t, err)
	require.Contains(t, c, "config.yaml")
}

// Tests loading config data from CONFIG env var
func Test_getConfigDataFromEnv(t *testing.T) {
	configFile = ""
	err := os.Setenv("CONFIG", "YXBpOgogIHBvcnQ6IDgwODAKYXV0aGVudGljYXRpb246CiAgY29uZmlnOiAiIgogIGVuYWJsZTogZmFsc2UKICBqd3RDb25maWc6CiAgICBmaWxlUGF0aDogIiIKICAgIGtleTogIiIKICAgIHNpZ25hdHVyZVR5cGU6ICIiCiAgdHlwZTogIiIKYXV0aG9yaXphdGlvbjoKICBhdXRvUmVsb2FkOiAwCiAgZW5hYmxlOiBmYWxzZQogIGZpbGVQYXRoOiAiIgogIHBvbGljeURhdGE6ICIiCiAgdXJsOiAiIgpicm9rZXI6CiAgZGlza1N5bmNTZWNvbmRzOiA2MAogIG1heENvbm46IDAKICBtYXhQYXlsb2FkOiAxMDQ4NTc2MDAKICBwYXJhbGxlbFJlY292ZXJ5OiAyCiAgcmVhZEJ1ZmZlclNpemU6IDIKICBzbGljZU1heEFnZVNlY29uZHM6IDAKICBzbGljZU1heEJ5dGVzOiA2NAogIHNsaWNlTWF4TWVzc2FnZXM6IDAKICB3cml0ZUJ1ZmZlclNpemU6IDIKICB3cml0ZURlYWRsaW5lOiAyMDAwCmNsaWVudDoKICBhcGlfYWRkcmVzczogaHR0cDovL2xvY2FsaG9zdDo4MDgwCiAgYXV0aF90b2tlbjogIiIKICBjbGllbnRfaWQ6ICIiCiAgZ3JwY19ob3N0OiBsb2NhbGhvc3QKICBncnBjX3BvcnQ6IDUwMDAwCmdycGM6CiAgYm9keUxpbWl0OiAxMDQ4NTc2MDAKICBwb3J0OiA1MDAwMAogIHN1YkJ1ZmZTaXplOiAxMDAKbG9nOgogIGxldmVsOiBpbmZvCnF1ZXVlOgogIGRlZmF1bHRWaXNpYmlsaXR5U2Vjb25kczogNjAKICBkZWZhdWx0V2FpdFRpbWVvdXRTZWNvbmRzOiAxCiAgbWF4RGVsYXlTZWNvbmRzOiA0MzIwMAogIG1heEV4cGlyYXRpb25TZWNvbmRzOiA0MzIwMAogIG1heEluZmxpZ2h0OiAyMDQ4CiAgbWF4TnVtYmVyT2ZNZXNzYWdlczogMTAyNAogIG1heFJlY2VpdmVDb3VudDogMTAyNAogIG1heFZpc2liaWxpdHlTZWNvbmRzOiA0MzIwMAogIG1heFdhaXRUaW1lb3V0U2Vjb25kczogMzYwMAogIHB1YkFja1dhaXRTZWNvbmRzOiA2MApyZXN0OgogIGJvZHlMaW1pdDogIiIKICBjb3JzOgogICAgYWxsb3dDcmVkZW50aWFsczogZmFsc2UKICAgIGFsbG93SGVhZGVyczogW10KICAgIGFsbG93TWV0aG9kczoKICAgIC0gR0VUCiAgICAtIFBPU1QKICAgIGFsbG93T3JpZ2luczoKICAgIC0gJyonCiAgICBleHBvc2VIZWFkZXJzOiBbXQogICAgbWF4QWdlOiAwCiAgcG9ydDogOTA5MAogIHJlYWRUaW1lb3V0OiA2MAogIHN1YkJ1ZmZTaXplOiAxMDAKICB3cml0ZVRpbWVvdXQ6IDYwCnJvdXRpbmc6CiAgYXV0b1JlbG9hZDogMAogIGRhdGE6ICIiCiAgZW5hYmxlOiBmYWxzZQogIGZpbGVQYXRoOiAiIgogIHVybDogIiIKc2VjdXJpdHk6CiAgY2E6CiAgICBkYXRhOiAiIgogICAgZmlsZW5hbWU6ICIiCiAgY2VydDoKICAgIGRhdGE6ICIiCiAgICBmaWxlbmFtZTogIiIKICBrZXk6CiAgICBkYXRhOiAiIgogICAgZmlsZW5hbWU6ICIiCnN0b3JlOgogIGNsZWFuU3RvcmU6IGZhbHNlCiAgbWF4TWVzc2FnZXM6IDAKICBtYXhQdXJnZUluYWN0aXZlOiAxNDQwCiAgbWF4UXVldWVTaXplOiAwCiAgbWF4UXVldWVzOiAwCiAgbWF4UmV0ZW50aW9uOiAxNDQwCiAgbWF4U3Vic2NyaWJlcnM6IDAKICBzdG9yZVBhdGg6IC4vc3RvcmUK")
	require.NoError(t, err)
	c, err := getConfigFile()

	require.NoError(t, err)
	require.Contains(t, c, "config.yaml")
}

// Tests loading config file from cwd
func Test_getConfigPathFromCWD(t *testing.T) {
	configFile = "../config.yaml"
	c, err := getConfigFile()

	require.NoError(t, err)
	require.Contains(t, c, "config.yaml")
}
