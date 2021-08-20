package rest

import (
	"context"
	"fmt"
	"github.com/fortytw2/leaktest"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/authentication"
	sdk "github.com/kubemq-io/kubemq-go"
	"github.com/nats-io/nuid"
	"github.com/stretchr/testify/require"
	"testing"
)

func generateAuthToken(authConfig *config.JWTAuthenticationConfig) string {
	sig, err := authentication.CreateSignSignature([]byte(authConfig.Key), authConfig.SignatureType)
	if err != nil {
		panic(err)
	}
	authToken, err := sig.Sign(nil)
	if err != nil {
		panic(err)
	}

	return authToken
}
func TestRestServer_AuthRPC(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())

	appConfig := getAppConfig()
	authConfig := &config.AuthenticationConfig{
		Enable: true,
		JwtConfig: &config.JWTAuthenticationConfig{
			Key:           "some-key",
			FilePath:      "",
			SignatureType: "HS256",
		},
	}

	appConfig.Authentication = authConfig
	port, svc, s := setup(ctx, t, appConfig)
	defer tearDown(svc, s)
	defer cancel()
	clientWithAuth, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://0.0.0.0:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest), sdk.WithAuthToken(generateAuthToken(authConfig.JwtConfig)))
	require.NoError(t, err)
	defer func() {
		_ = clientWithAuth.Close()
	}()

	err = clientWithAuth.E().
		SetChannel("test_channel").
		SetMetadata("some-metadata").
		Send(context.Background())
	require.NoError(t, err)
	_, err = sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://0.0.0.0:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest), sdk.WithCheckConnection(true))
	require.Error(t, err)

}

func TestRestServer_AuthStream(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	authConfig := &config.AuthenticationConfig{
		Enable: true,
		JwtConfig: &config.JWTAuthenticationConfig{
			Key:           "some-key",
			FilePath:      "",
			SignatureType: "HS256",
		},
	}

	appConfig.Authentication = authConfig
	port, svc, s := setup(ctx, t, appConfig)
	defer tearDown(svc, s)
	defer cancel()
	clientWithAuth, err := sdk.NewClient(ctx, sdk.WithUri(fmt.Sprintf("http://0.0.0.0:%d", port)), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeRest), sdk.WithAuthToken(generateAuthToken(authConfig.JwtConfig)))
	require.NoError(t, err)
	defer func() {
		_ = clientWithAuth.Close()
	}()
	ch, err := clientWithAuth.SubscribeToEvents(ctx, "my-channel", "", make(chan error, 1))
	require.NoError(t, err)
	require.NotNil(t, ch)

}
