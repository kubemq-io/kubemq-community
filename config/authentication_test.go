package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_AuthenticationLoadEnvironmentVars(t *testing.T) {
	authConfig := &AuthenticationConfig{
		Enable: true,
		JwtConfig: &JWTAuthenticationConfig{
			Key:           "env-def-key",
			FilePath:      "env_filePatch",
			SignatureType: "HS256",
		}}
	authEnv := map[string]string{
		"Authentication.Enable":                  "true",
		"Authentication.JwtConfig.Key":           "env-def-key",
		"Authentication.JwtConfig.FilePath":      "env_filePatch",
		"Authentication.JwtConfig.SignatureType": "HS256",
	}
	setEnvValues(authEnv)
	appConfig := getConfigRecord()
	require.EqualValues(t, authConfig, appConfig.Authentication)

}

func TestAuthenticationConfig_Validate(t *testing.T) {

	tests := []struct {
		name    string
		cfg     *AuthenticationConfig
		wantErr bool
	}{
		{
			name: "empty_config",
			cfg: &AuthenticationConfig{
				Enable: true,
				JwtConfig: &JWTAuthenticationConfig{
					Key:           "",
					FilePath:      "",
					SignatureType: "",
				},
			},
			wantErr: true,
		},
		{
			name: "no_sig",
			cfg: &AuthenticationConfig{
				Enable: true,
				JwtConfig: &JWTAuthenticationConfig{
					Key:           "some-key",
					FilePath:      "some-file",
					SignatureType: "",
				},
			},
			wantErr: true,
		},
		{
			name: "no_key",
			cfg: &AuthenticationConfig{
				Enable: true,
				JwtConfig: &JWTAuthenticationConfig{
					Key:           "",
					FilePath:      "",
					SignatureType: "HS256",
				},
			},
			wantErr: true,
		},
		{
			name: "no_file_exist",
			cfg: &AuthenticationConfig{
				Enable: true,
				JwtConfig: &JWTAuthenticationConfig{
					Key:           "",
					FilePath:      "some-file",
					SignatureType: "HS256",
				},
			},
			wantErr: true,
		},
		{
			name: "disable",
			cfg: &AuthenticationConfig{
				Enable: false,
				JwtConfig: &JWTAuthenticationConfig{
					Key:           "",
					FilePath:      "",
					SignatureType: "",
				},
			},
			wantErr: false,
		},
		{
			name: "proper_config",
			cfg: &AuthenticationConfig{
				Enable: true,
				JwtConfig: &JWTAuthenticationConfig{
					Key:           "some-key",
					FilePath:      "",
					SignatureType: "HS256",
				},
			},
			wantErr: false,
		},
		{
			name: "no_jwt_conf",
			cfg: &AuthenticationConfig{
				Enable: true,
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
