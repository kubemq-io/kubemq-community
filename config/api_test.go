package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ApiLoadEnvironmentVars(t *testing.T) {
	apiConfig := &ApiConfig{
		Port: 8888,
	}
	apuEnv := map[string]string{
		"Api.Enable": "true",
		"Api.Port":   "8888",
	}
	setEnvValues(apuEnv)
	appConfig := getConfigRecord()
	require.EqualValues(t, apiConfig, appConfig.Api)

}

func TestApiConfig_Validate(t *testing.T) {

	tests := []struct {
		name    string
		cfg     *ApiConfig
		wantErr bool
	}{
		{
			name: "proper_config",
			cfg: &ApiConfig{
				Port: 8080,
			},
			wantErr: false,
		},
		{
			name: "bad port_config",
			cfg: &ApiConfig{
				Port: 0,
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
