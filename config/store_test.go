package config

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_StoreVarsLoadEnvironmentVars(t *testing.T) {
	c := &StoreConfig{
		CleanStore:       true,
		StorePath:        "b",
		MaxQueues:        2,
		MaxSubscribers:   2,
		MaxMessages:      2,
		MaxQueueSize:     2,
		MaxRetention:     2,
		MaxPurgeInactive: 2,
	}
	cEnv := map[string]string{
		"Store.CleanStore":       "true",
		"Store.StorePath":        "b",
		"Store.MaxQueues":        "2",
		"Store.MaxSubscribers":   "2",
		"Store.MaxMessages":      "2",
		"Store.MaxQueueSize":     "2",
		"Store.MaxRetention":     "2",
		"Store.MaxPurgeInactive": "2",
	}
	setEnvValues(cEnv)
	appConfig := getConfigRecord()
	require.EqualValues(t, c, appConfig.Store)

}
func Test_StoreConfig_Validate(t *testing.T) {

	tests := []struct {
		name    string
		cfg     *StoreConfig
		wantErr bool
	}{
		{
			name:    "default",
			cfg:     defaultStoreConfig(),
			wantErr: false,
		},
		{
			name: "proper",
			cfg: &StoreConfig{
				CleanStore:       true,
				StorePath:        "/store",
				MaxQueues:        2,
				MaxSubscribers:   2,
				MaxMessages:      2,
				MaxQueueSize:     2,
				MaxRetention:     2,
				MaxPurgeInactive: 2,
			},
			wantErr: false,
		},
		{
			name: "bad_store",
			cfg: &StoreConfig{
				CleanStore:       true,
				StorePath:        "",
				MaxQueues:        2,
				MaxSubscribers:   2,
				MaxMessages:      2,
				MaxQueueSize:     2,
				MaxRetention:     2,
				MaxPurgeInactive: 2,
			},
			wantErr: true,
		},
		{
			name: "bad_max_queue",
			cfg: &StoreConfig{
				CleanStore:       true,
				StorePath:        "/store",
				MaxQueues:        -1,
				MaxSubscribers:   2,
				MaxMessages:      2,
				MaxQueueSize:     2,
				MaxRetention:     2,
				MaxPurgeInactive: 2,
			},
			wantErr: true,
		},
		{
			name: "bad_max_subs",
			cfg: &StoreConfig{
				CleanStore:       true,
				StorePath:        "/store",
				MaxQueues:        1,
				MaxSubscribers:   -1,
				MaxMessages:      2,
				MaxQueueSize:     2,
				MaxRetention:     2,
				MaxPurgeInactive: 2,
			},
			wantErr: true,
		},
		{
			name: "bad_max_msg",
			cfg: &StoreConfig{
				CleanStore:       true,
				StorePath:        "/store",
				MaxQueues:        1,
				MaxSubscribers:   1,
				MaxMessages:      -1,
				MaxQueueSize:     2,
				MaxRetention:     2,
				MaxPurgeInactive: 2,
			},
			wantErr: true,
		},
		{
			name: "bad_max_queue",
			cfg: &StoreConfig{
				CleanStore:       true,
				StorePath:        "/store",
				MaxQueues:        1,
				MaxSubscribers:   1,
				MaxMessages:      1,
				MaxQueueSize:     -1,
				MaxRetention:     2,
				MaxPurgeInactive: 2,
			},
			wantErr: true,
		},
		{
			name: "bad_max_retention",
			cfg: &StoreConfig{
				CleanStore:       true,
				StorePath:        "/store",
				MaxQueues:        1,
				MaxSubscribers:   1,
				MaxMessages:      1,
				MaxQueueSize:     1,
				MaxRetention:     -1,
				MaxPurgeInactive: 2,
			},
			wantErr: true,
		}, {
			name: "bad_max_inactive",
			cfg: &StoreConfig{
				CleanStore:       true,
				StorePath:        "/store",
				MaxQueues:        1,
				MaxSubscribers:   1,
				MaxMessages:      1,
				MaxQueueSize:     1,
				MaxRetention:     1,
				MaxPurgeInactive: -1,
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
