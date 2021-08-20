package config

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_QueueVarsLoadEnvironmentVars(t *testing.T) {
	c := &QueueConfig{
		MaxNumberOfMessages:       2,
		MaxWaitTimeoutSeconds:     2,
		MaxExpirationSeconds:      2,
		MaxDelaySeconds:           2,
		MaxReceiveCount:           2,
		MaxVisibilitySeconds:      2,
		DefaultVisibilitySeconds:  2,
		DefaultWaitTimeoutSeconds: 2,
		MaxInflight:               2048,
		PubAckWaitSeconds:         60,
	}
	cEnv := map[string]string{
		"Queue.MaxNumberOfMessages":       "2",
		"Queue.MaxWaitTimeoutSeconds":     "2",
		"Queue.MaxExpirationSeconds":      "2",
		"Queue.MaxDelaySeconds":           "2",
		"Queue.MaxReceiveCount":           "2",
		"Queue.MaxVisibilitySeconds":      "2",
		"Queue.DefaultVisibilitySeconds":  "2",
		"Queue.DefaultWaitTimeoutSeconds": "2",
		"Queue.MaxInflight":               "2048",
		"Queue.PubAckWaitSeconds":         "60",
	}
	setEnvValues(cEnv)
	appConfig := getConfigRecord()
	require.EqualValues(t, c, appConfig.Queue)

}
func Test_QueueConfig_Validate(t *testing.T) {

	tests := []struct {
		name    string
		cfg     *QueueConfig
		wantErr bool
	}{
		{
			name:    "default",
			cfg:     defaultQueueConfig(),
			wantErr: false,
		},
		{
			name: "bad_max_msg",
			cfg: &QueueConfig{
				MaxNumberOfMessages:       -2,
				MaxWaitTimeoutSeconds:     2,
				MaxExpirationSeconds:      2,
				MaxDelaySeconds:           2,
				MaxReceiveCount:           2,
				MaxVisibilitySeconds:      2,
				DefaultVisibilitySeconds:  2,
				DefaultWaitTimeoutSeconds: 2,
			},
			wantErr: true,
		},
		{
			name: "bad_max_wait",
			cfg: &QueueConfig{
				MaxNumberOfMessages:       2,
				MaxWaitTimeoutSeconds:     -2,
				MaxExpirationSeconds:      2,
				MaxDelaySeconds:           2,
				MaxReceiveCount:           2,
				MaxVisibilitySeconds:      2,
				DefaultVisibilitySeconds:  2,
				DefaultWaitTimeoutSeconds: 2,
			},
			wantErr: true,
		},
		{
			name: "bad_max_exp",
			cfg: &QueueConfig{
				MaxNumberOfMessages:       2,
				MaxWaitTimeoutSeconds:     2,
				MaxExpirationSeconds:      -2,
				MaxDelaySeconds:           2,
				MaxReceiveCount:           2,
				MaxVisibilitySeconds:      2,
				DefaultVisibilitySeconds:  2,
				DefaultWaitTimeoutSeconds: 2,
			},
			wantErr: true,
		},
		{
			name: "bad_max_dly",
			cfg: &QueueConfig{
				MaxNumberOfMessages:       2,
				MaxWaitTimeoutSeconds:     2,
				MaxExpirationSeconds:      2,
				MaxDelaySeconds:           -2,
				MaxReceiveCount:           2,
				MaxVisibilitySeconds:      2,
				DefaultVisibilitySeconds:  2,
				DefaultWaitTimeoutSeconds: 2,
			},
			wantErr: true,
		},
		{
			name: "bad_max_rx",
			cfg: &QueueConfig{
				MaxNumberOfMessages:       2,
				MaxWaitTimeoutSeconds:     2,
				MaxExpirationSeconds:      2,
				MaxDelaySeconds:           2,
				MaxReceiveCount:           -2,
				MaxVisibilitySeconds:      2,
				DefaultVisibilitySeconds:  2,
				DefaultWaitTimeoutSeconds: 2,
			},
			wantErr: true,
		},
		{
			name: "bad_max_vs",
			cfg: &QueueConfig{
				MaxNumberOfMessages:       2,
				MaxWaitTimeoutSeconds:     2,
				MaxExpirationSeconds:      2,
				MaxDelaySeconds:           2,
				MaxReceiveCount:           2,
				MaxVisibilitySeconds:      -2,
				DefaultVisibilitySeconds:  2,
				DefaultWaitTimeoutSeconds: 2,
			},
			wantErr: true,
		},
		{
			name: "bad_max_vis_def",
			cfg: &QueueConfig{
				MaxNumberOfMessages:       2,
				MaxWaitTimeoutSeconds:     2,
				MaxExpirationSeconds:      2,
				MaxDelaySeconds:           2,
				MaxReceiveCount:           2,
				MaxVisibilitySeconds:      2,
				DefaultVisibilitySeconds:  -2,
				DefaultWaitTimeoutSeconds: 2,
			},
			wantErr: true,
		},
		{
			name: "bad_max_wait_to",
			cfg: &QueueConfig{
				MaxNumberOfMessages:       2,
				MaxWaitTimeoutSeconds:     2,
				MaxExpirationSeconds:      2,
				MaxDelaySeconds:           2,
				MaxReceiveCount:           2,
				MaxVisibilitySeconds:      2,
				DefaultVisibilitySeconds:  2,
				DefaultWaitTimeoutSeconds: -2,
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
