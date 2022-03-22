package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_RestsVarsLoadEnvironmentVars(t *testing.T) {
	c := &RestConfig{
		Port:             2,
		ReadTimeout:      2,
		WriteTimeout:     2,
		SubBuffSize:      2,
		BodyLimit:        "b",
		NetworkTransport: "tcp",
		Cors: &RestCorsConfig{
			AllowOrigins:     []string{"a", "b", "c"},
			AllowMethods:     []string{"a", "b", "c"},
			AllowHeaders:     []string{"a", "b", "c"},
			AllowCredentials: true,
			ExposeHeaders:    []string{"a", "b", "c"},
			MaxAge:           2,
		},
	}
	cEnv := map[string]string{
		"Rest.Enable":                "true",
		"Rest.Port":                  "2",
		"Rest.SubBuffSize":           "2",
		"Rest.BodyLimit":             "b",
		"Rest.ReadTimeout":           "2",
		"Rest.WriteTimeout":          "2",
		"Rest.NetworkTransport":      "tcp",
		"Rest.Cors.AllowOrigins":     `a,b,c`,
		"Rest.Cors.AllowMethods":     `a,b,c`,
		"Rest.Cors.AllowHeaders":     `a,b,c`,
		"Rest.Cors.AllowCredentials": "true",
		"Rest.Cors.ExposeHeaders":    `a,b,c`,
		"Rest.Cors.MaxAge":           "2",
	}
	setEnvValues(cEnv)
	appConfig := getConfigRecord()
	require.EqualValues(t, c, appConfig.Rest)

}

func TestRestConfig_Validate(t *testing.T) {

	tests := []struct {
		name    string
		cfg     *RestConfig
		wantErr bool
	}{
		{
			name:    "default",
			cfg:     defaultRestConfig(),
			wantErr: false,
		},
		{
			name: "bad_rest_port",
			cfg: &RestConfig{
				Port:         -1,
				ReadTimeout:  1,
				WriteTimeout: 1,
				SubBuffSize:  1,
				BodyLimit:    "",
				Cors: &RestCorsConfig{
					AllowOrigins:     nil,
					AllowMethods:     nil,
					AllowHeaders:     nil,
					AllowCredentials: false,
					ExposeHeaders:    nil,
					MaxAge:           0,
				},
			},
			wantErr: true,
		},
		{
			name: "bad_rest_port_2",
			cfg: &RestConfig{
				Port:         234523,
				ReadTimeout:  1,
				WriteTimeout: 1,
				SubBuffSize:  1,
				BodyLimit:    "",
				Cors: &RestCorsConfig{
					AllowOrigins:     nil,
					AllowMethods:     nil,
					AllowHeaders:     nil,
					AllowCredentials: false,
					ExposeHeaders:    nil,
					MaxAge:           0,
				},
			},
			wantErr: true,
		},

		{
			name: "bad_rest_read",
			cfg: &RestConfig{
				Port:         1,
				ReadTimeout:  -1,
				WriteTimeout: 1,
				SubBuffSize:  1,
				BodyLimit:    "",
				Cors: &RestCorsConfig{
					AllowOrigins:     nil,
					AllowMethods:     nil,
					AllowHeaders:     nil,
					AllowCredentials: false,
					ExposeHeaders:    nil,
					MaxAge:           0,
				},
			},
			wantErr: true,
		},
		{
			name: "bad_rest_write",
			cfg: &RestConfig{
				Port:         1,
				ReadTimeout:  1,
				WriteTimeout: -1,
				SubBuffSize:  1,
				BodyLimit:    "",
				Cors: &RestCorsConfig{
					AllowOrigins:     nil,
					AllowMethods:     nil,
					AllowHeaders:     nil,
					AllowCredentials: false,
					ExposeHeaders:    nil,
					MaxAge:           0,
				},
			},
			wantErr: true,
		},
		{
			name: "bad_rest_buf",
			cfg: &RestConfig{
				Port:         1,
				ReadTimeout:  1,
				WriteTimeout: 1,
				SubBuffSize:  -1,
				BodyLimit:    "",
				Cors: &RestCorsConfig{
					AllowOrigins:     nil,
					AllowMethods:     nil,
					AllowHeaders:     nil,
					AllowCredentials: false,
					ExposeHeaders:    nil,
					MaxAge:           0,
				},
			},
			wantErr: true,
		},
		{
			name: "bad_rest_corrs",
			cfg: &RestConfig{
				Port:         1,
				ReadTimeout:  1,
				WriteTimeout: 1,
				SubBuffSize:  1,
				BodyLimit:    "",
				Cors:         nil,
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
