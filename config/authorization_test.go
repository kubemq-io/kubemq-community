package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_AuthorizationLoadEnvironmentVars(t *testing.T) {
	authConfig := &AuthorizationConfig{
		Enable:     true,
		PolicyData: "some-env-data",
		FilePath:   "some-env-file-path",
		Url:        "some-env-url",
		AutoReload: 1000,
	}
	authEnv := map[string]string{
		"Authorization.Enable":     "true",
		"Authorization.PolicyData": "some-env-data",
		"Authorization.FilePath":   "some-env-file-path",
		"Authorization.Url":        "some-env-url",
		"Authorization.AutoReload": "1000",
	}
	setEnvValues(authEnv)
	appConfig := getConfigRecord()
	require.EqualValues(t, authConfig, appConfig.Authorization)
}

func TestAuthorizationConfig_Validate(t *testing.T) {

	tests := []struct {
		name    string
		cfg     *AuthorizationConfig
		wantErr bool
	}{
		{
			name:    "default",
			cfg:     defaultAuthorizationConfig(),
			wantErr: false,
		},
		{
			name: "proper_url",
			cfg: &AuthorizationConfig{
				Enable:     true,
				PolicyData: "",
				FilePath:   "",
				Url:        "https://localhost:3000",
				AutoReload: 0,
			},
			wantErr: false,
		},
		{
			name: "proper_policy",
			cfg: &AuthorizationConfig{
				Enable:     true,
				PolicyData: "some-policy",
				FilePath:   "",
				Url:        "",
				AutoReload: 0,
			},
			wantErr: false,
		},
		{
			name: "empty",
			cfg: &AuthorizationConfig{
				Enable:     true,
				PolicyData: "",
				FilePath:   "",
				Url:        "",
				AutoReload: 0,
			},
			wantErr: true,
		},
		{
			name: "bad_file",
			cfg: &AuthorizationConfig{
				Enable:     true,
				PolicyData: "",
				FilePath:   "as",
				Url:        "",
				AutoReload: 0,
			},
			wantErr: true,
		},
		{
			name: "bad_url",
			cfg: &AuthorizationConfig{
				Enable:     true,
				PolicyData: "",
				FilePath:   "",
				Url:        "heep://",
				AutoReload: 0,
			},
			wantErr: true,
		},
		{
			name: "bad_url_2",
			cfg: &AuthorizationConfig{
				Enable:     true,
				PolicyData: "",
				FilePath:   "",
				Url:        "localhost",
				AutoReload: 0,
			},
			wantErr: true,
		},

		{
			name: "bad_auto_reload",
			cfg: &AuthorizationConfig{
				Enable:     true,
				PolicyData: "a",
				FilePath:   "",
				Url:        "",
				AutoReload: -1,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if err := tt.cfg.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
