package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_RoutingLoadEnvironmentVars(t *testing.T) {
	authConfig := &RoutingConfig{
		Enable:     true,
		Data:       "some-env-data",
		FilePath:   "some-env-file-path",
		Url:        "some-env-url",
		AutoReload: 1000,
	}
	authEnv := map[string]string{
		"Routing.Enable":     "true",
		"Routing.Data":       "some-env-data",
		"Routing.FilePath":   "some-env-file-path",
		"Routing.Url":        "some-env-url",
		"Routing.AutoReload": "1000",
	}
	setEnvValues(authEnv)
	appConfig := getConfigRecord()
	require.EqualValues(t, authConfig, appConfig.Routing)
}

func TestRoutingConfig_Validate(t *testing.T) {

	tests := []struct {
		name    string
		cfg     *RoutingConfig
		wantErr bool
	}{
		{
			name:    "default",
			cfg:     defaultRoutingConfig(),
			wantErr: false,
		},
		{
			name: "proper_url",
			cfg: &RoutingConfig{
				Enable:     true,
				Data:       "",
				FilePath:   "",
				Url:        "https://localhost:3000",
				AutoReload: 0,
			},
			wantErr: false,
		},
		{
			name: "proper_policy",
			cfg: &RoutingConfig{
				Enable:     true,
				Data:       "some-policy",
				FilePath:   "",
				Url:        "",
				AutoReload: 0,
			},
			wantErr: false,
		},
		{
			name: "empty",
			cfg: &RoutingConfig{
				Enable:     true,
				Data:       "",
				FilePath:   "",
				Url:        "",
				AutoReload: 0,
			},
			wantErr: true,
		},
		{
			name: "bad_file",
			cfg: &RoutingConfig{
				Enable:     true,
				Data:       "",
				FilePath:   "as",
				Url:        "",
				AutoReload: 0,
			},
			wantErr: true,
		},
		{
			name: "bad_url",
			cfg: &RoutingConfig{
				Enable:     true,
				Data:       "",
				FilePath:   "",
				Url:        "heep://",
				AutoReload: 0,
			},
			wantErr: true,
		},
		{
			name: "bad_url_2",
			cfg: &RoutingConfig{
				Enable:     true,
				Data:       "",
				FilePath:   "",
				Url:        "localhost",
				AutoReload: 0,
			},
			wantErr: true,
		},

		{
			name: "bad_auto_reload",
			cfg: &RoutingConfig{
				Enable:     true,
				Data:       "a",
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
