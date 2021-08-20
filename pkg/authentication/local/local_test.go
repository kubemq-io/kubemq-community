package local

import (
	"context"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestLocal_Init(t *testing.T) {

	tests := []struct {
		name   string
		config string

		wantErr bool
	}{
		{
			name: "valid - json",
			config: `
						{
							"method": "HS256",
							"key": "some-key"
						}
						`,
			wantErr: false,
		},
		{
			name: "valid - yaml",
			config: `method: HS256
key: some-key`,
			wantErr: false,
		},
		{
			name: "invalid - json",
			config: `
						{
							"method": "HS256",
							"key": "some-key",,
						}
						`,
			wantErr: true,
		},
		{
			name: "invalid - bad method",
			config: `
						{
							"method": "badmethod",
							"key": "some-key"
						}
						`,
			wantErr: true,
		},
		{
			name: "invalid - bad key",
			config: `
						{
							"method": "RS256",
							"key": "bad key"
						}
						`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := NewLocal()
			err := o.Init(tt.config)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func getJWTToken(key string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.StandardClaims{
		Audience:  "",
		ExpiresAt: time.Now().Unix() + 10,
		Id:        "",
		IssuedAt:  0,
		Issuer:    "",
		NotBefore: 0,
		Subject:   "",
	})
	return token.SignedString([]byte(key))
}

func TestLocal_Verify(t *testing.T) {
	cfg := `
		{
			"method": "HS256",
			"key": "some-key"
		}
						`
	l := NewLocal()
	err := l.Init(cfg)
	require.NoError(t, err)
	idToken, err := getJWTToken("some-key")
	require.NoError(t, err)
	c, err := l.Verify(context.Background(), idToken)
	require.NoError(t, err)
	fmt.Println(c)
}
