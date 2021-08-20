package grpc

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/authentication/local"
	"github.com/nats-io/nuid"

	sdk "github.com/kubemq-io/kubemq-go"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/authentication"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func generateAuthToken(authConfig *config.JWTAuthenticationConfig) string {
	sig, err := authentication.CreateSignSignature([]byte(authConfig.Key), authConfig.SignatureType)
	if err != nil {
		panic(err)
	}
	authToken, err := sig.Sign(nil)
	if err != nil {
		return ""
	}
	return authToken
}

func TestGRPCServer_AuthUnaryCall(t *testing.T) {
	//defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	jwtConfig := &config.JWTAuthenticationConfig{
		Key:           "some-key",
		FilePath:      "",
		SignatureType: "HS256",
	}
	localOps := &local.Options{
		Method: "HS256",
		Key:    "some-key",
	}
	authConfig := &config.AuthenticationConfig{
		Enable:    true,
		JwtConfig: nil,
		Type:      "local",
		Config:    localOps.Marshal(),
	}

	appConfig.Authentication = authConfig
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	clientWithAuth, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC), sdk.WithAuthToken(generateAuthToken(jwtConfig)))
	require.NoError(t, err)
	defer func() {
		_ = clientWithAuth.Close()
	}()
	err = clientWithAuth.E().
		SetChannel("test_channel").
		SetMetadata("some-metadata").
		Send(ctx)
	require.NoError(t, err)
	clientWithoutAuth, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
	require.NoError(t, err)
	err = clientWithoutAuth.E().
		SetChannel("test_channel").
		SetMetadata("some-metadata").
		Send(context.Background())
	require.Error(t, err)
}

func TestGRPCServer_AuthStreamCall(t *testing.T) {
	//defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	appConfig := getAppConfig()
	jwtConfig := &config.JWTAuthenticationConfig{
		Key:           "some-key",
		FilePath:      "",
		SignatureType: "HS256",
	}
	localOps := &local.Options{
		Method: "HS256",
		Key:    "some-key",
	}
	authConfig := &config.AuthenticationConfig{
		Enable:    true,
		JwtConfig: nil,
		Type:      "local",
		Config:    localOps.Marshal(),
	}
	appConfig.Authentication = authConfig
	appConfig, gs, svc := setup(ctx, t, appConfig)
	defer tearDown(gs, svc)
	defer cancel()
	clientWithAuth, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC), sdk.WithAuthToken(generateAuthToken(jwtConfig)))
	require.NoError(t, err)
	defer func() {
		_ = clientWithAuth.Close()
	}()
	errCh := make(chan error, 10)
	_, err = clientWithAuth.SubscribeToEvents(ctx, "some_channel", "", errCh)
	require.NoError(t, err)
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		require.Error(t, fmt.Errorf("should have error"))
	}
	clientWithoutAuth, err := sdk.NewClient(ctx, sdk.WithAddress("0.0.0.0", appConfig.Grpc.Port), sdk.WithClientId(nuid.Next()), sdk.WithTransportType(sdk.TransportTypeGRPC))
	require.NoError(t, err)
	_, err = clientWithoutAuth.SubscribeToEvents(context.Background(), "some_channel_2", "", errCh)
	require.NoError(t, err)
	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		require.Error(t, fmt.Errorf("should have error"))
	}

}
