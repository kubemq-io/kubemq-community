package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_GrpcVarsLoadEnvironmentVars(t *testing.T) {
	c := &GrpcConfig{
		Port:        3,
		SubBuffSize: 2,
		BodyLimit:   2,
	}

	cEnv := map[string]string{
		"Grpc.Port":        "3",
		"Grpc.SubBuffSize": "2",
		"Grpc.BodyLimit":   "2",
	}
	setEnvValues(cEnv)
	appConfig := getConfigRecord()
	require.EqualValues(t, c, appConfig.Grpc)
}

func TestGrpcConfig_Validate(t *testing.T) {

	tests := []struct {
		name    string
		cfg     *GrpcConfig
		wantErr bool
	}{
		{
			name:    "default",
			cfg:     defaultGRPCConfig(),
			wantErr: false,
		},
		{
			name: "bad_grpc_port",
			cfg: &GrpcConfig{
				Port:        -1,
				SubBuffSize: 0,
				BodyLimit:   0,
			},
			wantErr: true,
		},
		{
			name: "bad_grpc_port_2",
			cfg: &GrpcConfig{

				Port:        250000,
				SubBuffSize: 0,
				BodyLimit:   0,
			},
			wantErr: true,
		},
		{
			name: "bad_grpc_buf",
			cfg: &GrpcConfig{
				Port:        50000,
				SubBuffSize: -1,
				BodyLimit:   0,
			},
			wantErr: true,
		},
		{
			name: "bad_grpc_Body",
			cfg: &GrpcConfig{
				Port:        50000,
				SubBuffSize: 1,
				BodyLimit:   -1,
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
