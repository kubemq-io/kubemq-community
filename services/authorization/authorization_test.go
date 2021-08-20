package authorization

import (
	"context"
	"github.com/fortytw2/leaktest"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/authorization"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func savePolicyFile(policy, filename string) error {
	return ioutil.WriteFile(filename, []byte(policy), 0600)
}

func TestAccessController_Enforce(t *testing.T) {
	defer leaktest.Check(t)()
	tests := []struct {
		name              string
		authConfig        *config.AuthorizationConfig
		acr               *authorization.AccessControlRecord
		dataForFile       string
		dataForWebService string
		result            error
	}{
		{
			name: "bad policy file",
			authConfig: &config.AuthorizationConfig{
				Enable:     true,
				PolicyData: `[{"ClientID":"*","Events":true,"Channel":".*","Read":true,"Write":false}]`,
				FilePath:   "",
			},
			acr: &authorization.AccessControlRecord{
				ClientID: "some-client_id",
				Resource: "events",
				Channel:  "abq",
				Read:     true,
				Write:    false,
			},
			result: entities.ErrNoAccessNoEnforcer,
		},
		{
			name: "invalid provider",
			authConfig: &config.AuthorizationConfig{
				Enable:     true,
				PolicyData: "",
				FilePath:   "",
			},
			acr: &authorization.AccessControlRecord{
				ClientID: "some-client_id",
				Resource: "events",
				Channel:  "abq",
				Read:     true,
				Write:    false,
			},
			result: entities.ErrNoAccessNoEnforcer,
		},
		{
			name: "full_read_access",
			authConfig: &config.AuthorizationConfig{
				Enable:     true,
				PolicyData: `[{"ClientID":".*","Events":true,"Channel":".*","Read":true,"Write":false}]`,
				FilePath:   "",
			},
			acr: &authorization.AccessControlRecord{
				ClientID: "some-client_id",
				Resource: "events",
				Channel:  "foo",
				Read:     true,
				Write:    false,
			},
			result: nil,
		},
		{
			name: "access denied",
			authConfig: &config.AuthorizationConfig{
				Enable:     true,
				PolicyData: `[{"ClientID":".*","Events":true,"Channel":".*","Read":true,"Write":false}]`,
				FilePath:   "",
			},
			acr: &authorization.AccessControlRecord{
				ClientID: "some-client_id",
				Resource: "events",
				Channel:  "foo",
				Read:     false,
				Write:    true,
			},
			result: entities.ErrNoAccessResource,
		},
		{
			name: "access denied_2",
			authConfig: &config.AuthorizationConfig{
				Enable:     true,
				PolicyData: `[{"ClientID":"6","Events":true,"Channel":".*","Read":true,"Write":false}]`,
				FilePath:   "",
			},
			acr: &authorization.AccessControlRecord{
				ClientID: "asdas",
				Resource: "events",
				Channel:  "foo",
				Read:     true,
				Write:    false,
			},
			result: entities.ErrNoAccessResource,
		},
		{
			name: "bad acr",
			authConfig: &config.AuthorizationConfig{
				Enable:     true,
				PolicyData: `[{"ClientID":".*","Events":true,"Channel":".*","Read":true,"Write":false}]`,
				FilePath:   "",
			},
			acr: nil,

			result: entities.ErrNoAccessInvalidParams,
		},
		{
			name: "full_read_access_from_file",
			authConfig: &config.AuthorizationConfig{
				Enable:     true,
				PolicyData: "",
				FilePath:   "./policy.csv",
			},
			acr: &authorization.AccessControlRecord{
				ClientID: "some-client_id",
				Resource: "queues",
				Channel:  "foo",
				Read:     false,
				Write:    true,
			},
			dataForFile: `[{"ClientID":".*","Events":true,"Channel":".*","Read":true,"Write":false},{"ClientID":".*","Queues":true,"Channel":".*","Read":false,"Write":true}]`,
			result:      nil,
		},
		{
			name: "full_read_access_from_web_service",
			authConfig: &config.AuthorizationConfig{
				Enable:     true,
				PolicyData: "",
				FilePath:   "",
				Url:        "url_from_http_test",
			},
			acr: &authorization.AccessControlRecord{
				ClientID: "some-client_id",
				Resource: "queues",
				Channel:  "foo",
				Read:     false,
				Write:    true,
			},
			dataForWebService: `[{"ClientID":".*","Events":true,"Channel":".*","Read":true,"Write":false},{"ClientID":".*","Queues":true,"Channel":".*","Read":false,"Write":true}]`,
			result:            nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appConfig := config.GetCopyAppConfig("../../test/")
			appConfig.Authorization = tt.authConfig
			if tt.dataForFile != "" && tt.authConfig.FilePath != "" {
				err := savePolicyFile(tt.dataForFile, tt.authConfig.FilePath)
				require.NoError(t, err)
				defer os.Remove(tt.authConfig.FilePath)
			}
			if tt.dataForWebService != "" {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(200)
					_, _ = w.Write([]byte(tt.dataForWebService))
				}))
				defer srv.Close()
				tt.authConfig.Url = srv.URL
			}
			ac, _ := CreateAuthorizationService(context.Background(), appConfig)

			require.NotNil(t, ac)
			defer ac.Close()
			err := ac.Enforce(tt.acr)
			require.EqualValues(t, tt.result, err)
		})
	}
}

func TestService_Singleton(t *testing.T) {
	defer leaktest.Check(t)()
	appConfig := config.GetCopyAppConfig("../../test/")
	appConfig.Authorization = &config.AuthorizationConfig{
		Enable:     true,
		PolicyData: `[{"ClientID":".*","Events":true,"Channel":".*","Read":true,"Write":false}]`,
		FilePath:   "",
	}
	as, err := CreateAuthorizationService(context.Background(), appConfig)
	require.NoError(t, err)
	require.NotNil(t, as)
	defer as.Close()
	SetSingleton(as)
	singletonAuthService := GetSingleton()
	require.NotNil(t, singletonAuthService)

}
