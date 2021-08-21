package config

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_SecurityVarsLoadEnvironmentVars(t *testing.T) {
	c := &SecurityConfig{
		Cert: &ResourceConfig{
			Filename: "b",
			Data:     "b",
		},
		Key: &ResourceConfig{
			Filename: "b",
			Data:     "b",
		},
		Ca: &ResourceConfig{
			Filename: "b",
			Data:     "b",
		},
	}
	cEnv := map[string]string{
		"Security.Cert.Filename": "b",
		"Security.Cert.Data":     "b",
		"Security.Key.Filename":  "b",
		"Security.Key.Data":      "b",
		"Security.Ca.Filename":   "b",
		"Security.Ca.Data":       "b",
	}
	setEnvValues(cEnv)
	appConfig := getConfigRecord()
	require.EqualValues(t, c, appConfig.Security)

}

func Test_SecurityConfig_Validate(t *testing.T) {

	tests := []struct {
		name    string
		cfg     *SecurityConfig
		wantErr bool
	}{
		{
			name:    "default",
			cfg:     defaultSecurityConfig(),
			wantErr: false,
		},
		{
			name: "empty",
			cfg: &SecurityConfig{
				Cert: nil,
				Key:  nil,
				Ca:   nil,
			},
			wantErr: false,
		},
		{
			name: "good tls",
			cfg: &SecurityConfig{
				Cert: &ResourceConfig{
					Filename: "",
					Data:     "aaa",
				},
				Key: &ResourceConfig{
					Filename: "",
					Data:     "aaa",
				},
				Ca: nil,
			},
			wantErr: false,
		},
		{
			name: "bad tls",
			cfg: &SecurityConfig{
				Cert: &ResourceConfig{
					Filename: "aaa",
					Data:     "",
				},
				Key: &ResourceConfig{
					Filename: "aaa",
					Data:     "",
				},
				Ca: nil,
			},
			wantErr: true,
		},
		{
			name: "bad tls 2",
			cfg: &SecurityConfig{
				Cert: &ResourceConfig{
					Filename: "",
					Data:     "aaa",
				},
				Key: &ResourceConfig{
					Filename: "aaa",
					Data:     "",
				},
				Ca: nil,
			},
			wantErr: true,
		},
		{
			name: "good mtls",
			cfg: &SecurityConfig{
				Cert: &ResourceConfig{
					Filename: "",
					Data:     "aaa",
				},
				Key: &ResourceConfig{
					Filename: "",
					Data:     "aaa",
				},
				Ca: &ResourceConfig{
					Filename: "",
					Data:     "aaa",
				},
			},
			wantErr: false,
		},
		{
			name: "bad mtls 1",
			cfg: &SecurityConfig{
				Cert: &ResourceConfig{
					Filename: "aaa",
					Data:     "",
				},
				Key: &ResourceConfig{
					Filename: "aaa",
					Data:     "",
				},
				Ca: &ResourceConfig{
					Filename: "aaa",
					Data:     "",
				},
			},
			wantErr: true,
		},
		{
			name: "bad mtls 2",
			cfg: &SecurityConfig{
				Cert: &ResourceConfig{
					Filename: "",
					Data:     "aa",
				},
				Key: &ResourceConfig{
					Filename: "aaa",
					Data:     "",
				},
				Ca: &ResourceConfig{
					Filename: "aaa",
					Data:     "",
				},
			},
			wantErr: true,
		},
		{
			name: "bad mtls 3",
			cfg: &SecurityConfig{
				Cert: &ResourceConfig{
					Filename: "",
					Data:     "aa",
				},
				Key: &ResourceConfig{
					Filename: "",
					Data:     "aa",
				},
				Ca: &ResourceConfig{
					Filename: "aaa",
					Data:     "",
				},
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
