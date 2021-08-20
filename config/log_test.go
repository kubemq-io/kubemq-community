package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)


func Test_LogVarsLoadEnvironmentVars(t *testing.T) {
	c := &LogConfig{
		Level: "trace",
	}
	cEnv := map[string]string{
		"Log.Level": "trace",
	}
	setEnvValues(cEnv)
	appConfig := getConfigRecord()
	require.EqualValues(t, c, appConfig.Log)

}

func Test_LogConfig_Validate(t *testing.T) {

	tests := []struct {
		name    string
		cfg     *LogConfig
		wantErr bool
	}{
		{
			name:    "default",
			cfg:     defaultLogConfig(),
			wantErr: false,
		},
		{
			name: "bad_level",
			cfg: &LogConfig{
				Level: "",
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
