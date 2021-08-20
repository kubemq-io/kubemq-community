package config

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_BrokerVarsLoadEnvironmentVars(t *testing.T) {
	c := &BrokerConfig{
		Port:               3000,
		MaxPayload:         2,
		WriteDeadline:      2,
		MaxConn:            2,
		MonitoringPort:     0,
		MemoryPipe:         nil,
		WriteBufferSize:    4,
		ReadBufferSize:     4,
		DiskSyncSeconds:    60,
		SliceMaxMessages:   0,
		SliceMaxBytes:      64,
		SliceMaxAgeSeconds: 0,
		ParallelRecovery:   2,
	}
	cEnv := map[string]string{
		"Broker.Port":               "3000",
		"Broker.MaxPayload":         "2",
		"Broker.WriteDeadline":      "2",
		"Broker.MaxConn":            "2",
		"Broker.WriteBufferSize":    "4",
		"Broker.ReadBufferSize":     "4",
		"Broker.DiskSyncSeconds":    "60",
		"Broker.SliceMaxMessages":   "0",
		"Broker.SliceMaxBytes":      "64",
		"Broker.SliceMaxAgeSeconds": "0",
		"Broker.ParallelRecovery":   "2",
	}
	setEnvValues(cEnv)
	appConfig := getConfigRecord()

	c.MonitoringPort = appConfig.Broker.MonitoringPort
	c.MemoryPipe = appConfig.Broker.MemoryPipe
	c.Port = appConfig.Broker.Port
	require.EqualValues(t, c, appConfig.Broker)

}

func TestBrokerConfig_Validate(t *testing.T) {

	tests := []struct {
		name    string
		cfg     *BrokerConfig
		wantErr bool
	}{
		{
			name:    "disable",
			cfg:     defaultBrokerConfig(),
			wantErr: false,
		},
		{
			name: "proper_config",
			cfg: &BrokerConfig{
				Port:               0,
				MaxPayload:         1,
				WriteDeadline:      1,
				MaxConn:            1,
				MonitoringPort:     1,
				MemoryPipe:         nil,
				WriteBufferSize:    2,
				ReadBufferSize:     2,
				DiskSyncSeconds:    60,
				SliceMaxMessages:   0,
				SliceMaxBytes:      64,
				SliceMaxAgeSeconds: 0,
				ParallelRecovery:   2,
			},
			wantErr: false,
		},
		{
			name: "bad_port",
			cfg: &BrokerConfig{

				MaxPayload:     1,
				WriteDeadline:  1,
				MaxConn:        1,
				MonitoringPort: 1,
			},
			wantErr: true,
		},
		{
			name: "bad_payload",
			cfg: &BrokerConfig{

				MaxPayload:     -1,
				WriteDeadline:  1,
				MaxConn:        1,
				MonitoringPort: 1,
			},
			wantErr: true,
		},
		{
			name: "bad_Deadline",
			cfg: &BrokerConfig{

				MaxPayload:     1,
				WriteDeadline:  -1,
				MaxConn:        1,
				MonitoringPort: 1,
			},
			wantErr: true,
		},
		{
			name: "bad_MaxCon",
			cfg: &BrokerConfig{

				MaxPayload:     1,
				WriteDeadline:  1,
				MaxConn:        -1,
				MonitoringPort: 1,
			},
			wantErr: true,
		},
		{
			name: "bad_MonitorPort",
			cfg: &BrokerConfig{

				MaxPayload:     1,
				WriteDeadline:  1,
				MaxConn:        1,
				MonitoringPort: -1,
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
