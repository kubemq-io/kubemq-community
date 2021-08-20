package authorization

import (
	"context"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/authorization"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"time"
)

var singletonAccessController *Service

type Options struct {
	InMemory       string
	FilePath       string
	WebServiceUrl  string
	ReloadInterval time.Duration
}
type Service struct {
	options  *Options
	enforcer *authorization.Controller
}

func SetSingleton(ac *Service) {
	singletonAccessController = ac
}

func GetSingleton() *Service {
	return singletonAccessController
}

func CreateAuthorizationService(ctx context.Context, appConfig *config.Config) (*Service, error) {

	opts := &Options{
		InMemory:       appConfig.Authorization.PolicyData,
		FilePath:       appConfig.Authorization.FilePath,
		WebServiceUrl:  appConfig.Authorization.Url,
		ReloadInterval: time.Duration(appConfig.Authorization.AutoReload) * time.Second,
	}

	as := &Service{
		options:  opts,
		enforcer: nil,
	}
	provider, err := getProvider(opts)
	if err != nil {
		return as, err
	}
	as.enforcer, err = authorization.NewController(ctx, provider, as.options.ReloadInterval)
	if err != nil {
		return as, err
	}

	return as, nil
}
func (as *Service) Close() {
	if as.enforcer != nil {
		as.enforcer.Close()
	}
}
func getProvider(opts *Options) (authorization.Provider, error) {
	if opts.InMemory != "" {
		return authorization.NewMemoryProvider([]byte(opts.InMemory)), nil
	}
	if opts.FilePath != "" {
		return authorization.NewFileProvider(opts.FilePath), nil
	}
	if opts.WebServiceUrl != "" {
		return authorization.NewWebServiceProvider(opts.WebServiceUrl), nil
	}

	return nil, entities.ErrInvalidAuthorizationProvider

}

func (as *Service) Enforce(acr *authorization.AccessControlRecord) error {
	if as.enforcer == nil {
		return entities.ErrNoAccessNoEnforcer
	}
	if acr == nil {
		return entities.ErrNoAccessInvalidParams
	}
	result, err := as.enforcer.Enforce(acr.Parameters()...)
	if err != nil {
		return err
	}
	if !result {
		return entities.ErrNoAccessResource
	}
	return nil
}
