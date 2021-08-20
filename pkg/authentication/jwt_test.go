package authentication

import (
	"fmt"
	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

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

func TestCreateValidateJWTToken(t *testing.T) {

	tests := []struct {
		name            string
		method          string
		createKey       []byte
		validateKey     []byte
		claims          jwt.StandardClaims
		wantCreateErr   bool
		wantValidateErr bool
	}{
		{
			name:            "create_validate_ok_HMAC",
			method:          "HS256",
			createKey:       []byte("some-key"),
			validateKey:     []byte("some-key"),
			claims:          jwt.StandardClaims{ExpiresAt: time.Now().Add(5 * time.Second).Unix()},
			wantCreateErr:   false,
			wantValidateErr: false,
		},
		{
			name:            "key_expire_HMAC",
			method:          "HS256",
			createKey:       []byte("some-key"),
			validateKey:     []byte("some-key"),
			claims:          jwt.StandardClaims{ExpiresAt: time.Now().Add(-5 * time.Second).Unix()},
			wantCreateErr:   false,
			wantValidateErr: true,
		},
		{
			name:            "invalid_create_validate_keys_HMAC",
			method:          "HS256",
			createKey:       []byte("some-key"),
			validateKey:     []byte("some-key-different"),
			claims:          jwt.StandardClaims{ExpiresAt: time.Now().Add(-5 * time.Second).Unix()},
			wantCreateErr:   false,
			wantValidateErr: true,
		},
		{
			name:            "create_validate_ok_RSA",
			method:          "RS512",
			createKey:       []byte(rsaTestPrivateKey),
			validateKey:     []byte(rsaTestPublicKey),
			claims:          jwt.StandardClaims{ExpiresAt: time.Now().Add(5 * 365 * 1024 * time.Hour).Unix()},
			wantCreateErr:   false,
			wantValidateErr: false,
		},
		{
			name:            "create_validate_ok_ECDA",
			method:          "ES256",
			createKey:       []byte(ecTestPrivateKey256),
			validateKey:     []byte(ecTestPublicKey256),
			claims:          jwt.StandardClaims{ExpiresAt: time.Now().Add(5 * time.Second).Unix()},
			wantCreateErr:   false,
			wantValidateErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			signSig, err := CreateSignSignature(tt.createKey, tt.method)
			require.NoError(t, err)
			require.NotNil(t, signSig)

			verifySig, err := CreateVerifySignature(tt.validateKey, tt.method)
			require.NoError(t, err)
			require.NotNil(t, verifySig)

			authToken, err := signSig.Sign(tt.claims)
			if tt.wantCreateErr {
				require.Error(t, err)
				require.Empty(t, authToken)
			} else {
				require.NoError(t, err)
				require.NotEmpty(t, authToken)
			}
			fmt.Println(authToken)
			err = verifySig.Verify(authToken, &jwt.StandardClaims{})
			if tt.wantValidateErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

		})
	}
}
