package authentication

import (
	"context"
	"github.com/fortytw2/leaktest"
	"github.com/golang-jwt/jwt/v4"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/authentication"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

const testKey = `some-test-key`
const rsaTestPrivateKey = `-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAt2GVE6vIMKlUsJeIQrSXtPG7X+06/qfyfQUjYJYvn1Pff05B
3LcazECYEMd/act5YlnzrMg6hlbcoa9szr3/mpwjJ+l4RKd6J1uZngPaGaRSAr4D
vSYsMe9pRrrOnW9XyRBLiB+EOriq5qkcSTnRcwwoczPekJOK45rEUgdaL5JP/TRh
YYwgayb+zQ5cu+1Hx94m5XldrAzkeQ8ldMeWBPzfasnX6zaBXtdWksJqmpMMWg1N
unH3+NWFyLsPt4BNbtjpKOp7Wq3jDQpp803PsK3/XksM/87qPL4xNCJuV8MF4Pbx
8JE3ZRusK6usmJrohv8+vgIIg4oKdpMjDlrzpQIDAQABAoIBAQChanTFfuTU6IgS
dR/Mz5fl/w3W70OJmp2YrGgqgahjj3lgXqscs9QTzBvOUTx3DFLJXrJd+VWCoHzO
mVKmXJncJunPHPPvQpgEgt2iOHPHNFu37DfwS+SFA97gElkCPVrBMeW6aTuEUL6m
EF9EmW8i3KXSWerjyetsUvPR3ITm714EljGoqnseuShFUk0ptpTri5JNJvYsnd+H
pNC2rkjuMV4mZnafdElmFw9jEOY2J7UBGWbDS8PUDHGTeRN/CTxgpbl0mIqdDDNI
pHogyxHfIaPNbMiQ+86iXpjCHColWFQ1R6wZ9AmFJq08X8chVL6Jdl1WrRC5G7cy
Wg5LM8JhAoGBAOWHHIv9ZbNCRDFA7Ja/hehgPSOU3brUgEyjpE1VhLWQRbvsAFHX
IMsuMlj33NNW4P8/MzgWjmSNcgt9RCzxKejJfzZQb5HakXzwQ+i+SR23r4kW8/Em
9k2rSHnWXuvEVXMxjoFQhP6P6tUR8PhYu66DE0qr/GUD40J+KUG3h3Y5AoGBAMyH
+ewOLVdjIm+Lhp5w8KWfZDUrcNG/EOLOmFM2yAGd9n6pK1qJI7fEp4pMdV9aY/bH
i3h2Dt2PySXl71JkcAl6OnhC9rhAkDRn7NkKyJHZ8yDaE3bdvUN0AEZ69JzPPyW2
t6tws2uyhJC9aTGg51yREIMsj/c6q1ZcBIBoq4jNAoGBALKBvuDcxPCCE/jeTmH7
N9B+sG3ww/UeeV2hUxHV9a0jNCivpZwAnH+IQR3SPwqaIchBRbtUR4/KNazb0l7F
fFuQAgCi0/JyMv4g+h+Thde32KvcjwG41IuZL3eaEh54hiBdpT/K+HPmR7NIDcmH
cQeK50EGuvdw65j1924lpxN5AoGARY0f0cPoa0UM2r2po4toagnPu7zv+oNsrJPw
fKuuS855mgzQ71KfUMiQ6JijeS91ut+Ub/xFhdZ1YmvUfEInTzG/XFH5MCLUZt3I
Tu450k85PDysTcmNqLhzt4PsVr6rDJobzzLd8IueRNIESZob0wCJivHGax4KUa2s
4jW1zykCgYBLLaO6zOA46VETLkAFSpah4mp2lTGijq5bKKMFzqBySvHKGEWAIIp6
trEcJeUYngPsbmucaGeOPCvJ2UIsBk2PRIxBpJ2Cjmo9Em3OfuW1WwW8IeZydj/C
NE8V2yWIuMe+obfCKSdQPwSQogSj7Q1H20prHMl9q3EI/xQWq4t/2Q==
-----END RSA PRIVATE KEY-----`
const rsaTestPublicKey = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAt2GVE6vIMKlUsJeIQrSX
tPG7X+06/qfyfQUjYJYvn1Pff05B3LcazECYEMd/act5YlnzrMg6hlbcoa9szr3/
mpwjJ+l4RKd6J1uZngPaGaRSAr4DvSYsMe9pRrrOnW9XyRBLiB+EOriq5qkcSTnR
cwwoczPekJOK45rEUgdaL5JP/TRhYYwgayb+zQ5cu+1Hx94m5XldrAzkeQ8ldMeW
BPzfasnX6zaBXtdWksJqmpMMWg1NunH3+NWFyLsPt4BNbtjpKOp7Wq3jDQpp803P
sK3/XksM/87qPL4xNCJuV8MF4Pbx8JE3ZRusK6usmJrohv8+vgIIg4oKdpMjDlrz
pQIDAQAB
-----END PUBLIC KEY-----`
const ecTestPrivateKey256 = `-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIOY+8hvBHn2M1bsCx/ON5U4/lFZbVTPiBoGtFtvo6r/8oAoGCCqGSM49
AwEHoUQDQgAEuAfwZOEgDIHSfMknOPwbLvhaWZxLdZafmcxcb4FMAg8ZWrH9Geq2
u7TKbq/AvX4PZYqXFzIixCq0YEh3rM2Rog==
-----END EC PRIVATE KEY-----`
const ecTestPublicKey256 = `-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEuAfwZOEgDIHSfMknOPwbLvhaWZxL
dZafmcxcb4FMAg8ZWrH9Geq2u7TKbq/AvX4PZYqXFzIixCq0YEh3rM2Rog==
-----END PUBLIC KEY-----`

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
func saveToFile(data []byte, filename string) error {
	return ioutil.WriteFile(filename, []byte(data), 0600)
}

func generateToken(signKey, method string, claims *Claims) (string, error) {
	sig, err := authentication.CreateSignSignature([]byte(signKey), method)
	if err != nil {
		return "", err
	}
	if claims == nil {
		return sig.Sign(jwt.StandardClaims{})
	}
	return sig.Sign(claims.StandardClaims)
}

func TestService_Authenticate(t *testing.T) {
	defer leaktest.Check(t)()

	tests := []struct {
		name          string
		authConfig    *config.AuthenticationConfig
		authClaims    *Claims
		signKey       string
		loadFromFile  string
		wantInitErr   bool
		wantVerifyErr bool
	}{
		{
			name: "grant_auth_with_HS_key",
			authConfig: &config.AuthenticationConfig{
				Enable: true,
				JwtConfig: &config.JWTAuthenticationConfig{
					Key:           testKey,
					FilePath:      "",
					SignatureType: "HS256",
				},
			},
			authClaims:    CreateAuthenticationClaims("", time.Now().Unix()+5),
			loadFromFile:  "",
			signKey:       testKey,
			wantInitErr:   false,
			wantVerifyErr: false,
		},
		{
			name: "grant_auth_with_RSA_key",
			authConfig: &config.AuthenticationConfig{
				Enable: true,
				JwtConfig: &config.JWTAuthenticationConfig{
					Key:           "",
					FilePath:      "./authfile",
					SignatureType: "RS512",
				},
			},
			authClaims:    CreateAuthenticationClaims("", time.Now().Unix()+5),
			loadFromFile:  rsaTestPublicKey,
			signKey:       rsaTestPrivateKey,
			wantInitErr:   false,
			wantVerifyErr: false,
		},
		{
			name: "grant_auth_with_ES_key",
			authConfig: &config.AuthenticationConfig{
				Enable: true,
				JwtConfig: &config.JWTAuthenticationConfig{
					Key:           "",
					FilePath:      "./authfile",
					SignatureType: "ES256",
				},
			},
			authClaims:    CreateAuthenticationClaims("", time.Now().Unix()+5),
			loadFromFile:  ecTestPublicKey256,
			signKey:       ecTestPrivateKey256,
			wantInitErr:   false,
			wantVerifyErr: false,
		},
		{
			name: "grant_auth_no_claims",
			authConfig: &config.AuthenticationConfig{
				Enable: true,
				JwtConfig: &config.JWTAuthenticationConfig{
					Key:           testKey,
					FilePath:      "",
					SignatureType: "HS256",
				},
			},
			authClaims:    nil,
			loadFromFile:  "",
			signKey:       testKey,
			wantInitErr:   false,
			wantVerifyErr: false,
		},
		{
			name: "deny_access_with_claims",
			authConfig: &config.AuthenticationConfig{
				Enable: true,
				JwtConfig: &config.JWTAuthenticationConfig{
					Key:           testKey,
					FilePath:      "",
					SignatureType: "HS256",
				},
			},
			authClaims:    CreateAuthenticationClaims("", time.Now().Unix()-5),
			loadFromFile:  "",
			signKey:       testKey,
			wantInitErr:   false,
			wantVerifyErr: true,
		},
		{
			name: "bad_config_bad_file",
			authConfig: &config.AuthenticationConfig{
				Enable: true,
				JwtConfig: &config.JWTAuthenticationConfig{
					Key:           "",
					FilePath:      "./authfile",
					SignatureType: "HS256",
				},
			},
			authClaims:    CreateAuthenticationClaims("", time.Now().Unix()+5),
			loadFromFile:  "",
			signKey:       testKey,
			wantInitErr:   true,
			wantVerifyErr: true,
		},
		{
			name: "bad_config_wrong_public_file",
			authConfig: &config.AuthenticationConfig{
				Enable: true,
				JwtConfig: &config.JWTAuthenticationConfig{
					Key:           "",
					FilePath:      "./authfile",
					SignatureType: "RS256",
				},
			},
			authClaims:    CreateAuthenticationClaims("", time.Now().Unix()+5),
			loadFromFile:  ecTestPublicKey256,
			signKey:       rsaTestPrivateKey,
			wantInitErr:   true,
			wantVerifyErr: true,
		},
		{
			name: "bad_config_no_key_provided",
			authConfig: &config.AuthenticationConfig{
				Enable: true,
				JwtConfig: &config.JWTAuthenticationConfig{
					Key:           "",
					FilePath:      "",
					SignatureType: "HS256",
				},
			},
			authClaims:    CreateAuthenticationClaims("", time.Now().Unix()+5),
			loadFromFile:  "",
			signKey:       testKey,
			wantInitErr:   true,
			wantVerifyErr: true,
		},
		{
			name: "bad_config_no_signature",
			authConfig: &config.AuthenticationConfig{
				Enable: true,
				JwtConfig: &config.JWTAuthenticationConfig{
					Key:           testKey,
					FilePath:      "",
					SignatureType: "",
				},
			},
			authClaims:    CreateAuthenticationClaims("", time.Now().Unix()+5),
			loadFromFile:  "",
			signKey:       testKey,
			wantInitErr:   true,
			wantVerifyErr: true,
		},
		{
			name: "bad_config_wrong_signature",
			authConfig: &config.AuthenticationConfig{
				Enable: true,
				JwtConfig: &config.JWTAuthenticationConfig{
					Key:           testKey,
					FilePath:      "",
					SignatureType: "RS256",
				},
			},
			authClaims:    CreateAuthenticationClaims("", time.Now().Unix()+5),
			loadFromFile:  "",
			signKey:       testKey,
			wantInitErr:   true,
			wantVerifyErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appConfig := config.GetCopyAppConfig("../../test/")
			appConfig.Authentication = tt.authConfig
			if tt.loadFromFile != "" {
				err := saveToFile([]byte(tt.loadFromFile), "./authfile")
				require.NoError(t, err)
				defer os.Remove("./authfile")
			}
			as, err := CreateAuthenticationService(context.Background(), appConfig)
			if tt.wantInitErr {
				require.Error(t, err)
				require.NotNil(t, as)
				require.Nil(t, as.provider)
			} else {
				require.NoError(t, err)
				require.NotNil(t, as)
				require.NotNil(t, as.provider)
			}
			authToken, _ := generateToken(tt.signKey, tt.authConfig.JwtConfig.SignatureType, tt.authClaims)
			resClaims, err := as.Authenticate(authToken)
			if tt.wantVerifyErr {
				require.Error(t, err)
				require.Nil(t, resClaims)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resClaims)
				if tt.authClaims != nil {
					require.EqualValues(t, tt.authClaims.ClientID, resClaims.ClientID)
				}

			}

		})
	}
}

func TestService_Singleton(t *testing.T) {
	defer leaktest.Check(t)()
	appConfig := config.GetCopyAppConfig("../../test/")
	appConfig.Authentication = &config.AuthenticationConfig{
		Enable: true,
		JwtConfig: &config.JWTAuthenticationConfig{
			Key:           testKey,
			FilePath:      "",
			SignatureType: "HS256",
		},
	}

	as, err := CreateAuthenticationService(context.Background(), appConfig)
	require.NoError(t, err)
	require.NotNil(t, as)
	SetSingleton(as)
	singletonAuthService := GetSingleton()
	require.NotNil(t, singletonAuthService)

}
