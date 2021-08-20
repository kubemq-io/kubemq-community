package authentication

import (
	"context"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/authentication"
	"github.com/kubemq-io/kubemq-community/pkg/authentication/local"
	"github.com/kubemq-io/kubemq-community/pkg/cmap"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"io/ioutil"
	"strings"
	"time"
)

var singletonAuthenticator *Service

type Claims struct {
	ClientID       string
	StandardClaims jwt.StandardClaims
}

func (a *Claims) Valid() error {
	return a.StandardClaims.Valid()
}

func CreateAuthenticationClaims(clientID string, expiration int64) *Claims {
	return &Claims{
		ClientID: clientID,
		StandardClaims: jwt.StandardClaims{
			Audience:  "",
			ExpiresAt: expiration,
			Id:        "",
			IssuedAt:  0,
			Issuer:    "",
			NotBefore: 0,
			Subject:   "",
		},
	}
}

type options struct {
	key       string
	filePath  string
	signature string
	kind      string
	config    string
}
type Service struct {
	options  *options
	provider authentication.Provider
	cache    cmap.ConcurrentMap
}

func SetSingleton(auth *Service) {
	singletonAuthenticator = auth
}

func GetSingleton() *Service {
	return singletonAuthenticator
}

func CreateAuthenticationService(ctx context.Context, appConfig *config.Config) (*Service, error) {
	opts := &options{
		kind:   appConfig.Authentication.Type,
		config: appConfig.Authentication.Config,
	}
	if appConfig.Authentication.JwtConfig != nil {
		opts.key = appConfig.Authentication.JwtConfig.Key
		opts.filePath = appConfig.Authentication.JwtConfig.FilePath
		opts.signature = appConfig.Authentication.JwtConfig.SignatureType
	}
	as := &Service{
		options:  opts,
		cache:    cmap.New(),
		provider: nil,
	}
	err := as.setProvider()
	if err != nil {
		return as, err
	}
	return as, nil
}
func (as *Service) setProvider() error {
	if as.options.signature != "" {
		localOps := &local.Options{
			Method: strings.ToUpper(as.options.signature),
			Key:    "",
		}
		key, err := as.getKey()
		if err != nil {
			return err
		}
		localOps.Key = string(key)
		as.options.kind = "local"
		as.options.config = localOps.Marshal()
	}

	switch as.options.kind {
	case "local":
		p := local.NewLocal()

		err := p.Init(as.options.config)
		if err != nil {
			return fmt.Errorf("error local authentication provider initialization, %s", err.Error())
		}
		as.provider = p
	default:
		return fmt.Errorf("invalid authentication provider type")
	}

	return nil
}
func (as *Service) getKey() ([]byte, error) {

	if as.options.key != "" {
		return []byte(as.options.key), nil
	}
	if as.options.filePath != "" {
		data, err := ioutil.ReadFile(as.options.filePath)
		if err != nil {
			return nil, fmt.Errorf("%s %s", entities.ErrAuthInvalidKeyFile, err.Error())
		}
		return data, nil
	}

	return nil, entities.ErrAuthInvalidNoKey
}
func (as *Service) Authenticate(authToken string) (*Claims, error) {
	if as.provider == nil {
		return nil, fmt.Errorf("no authentication provider was properly initialized")
	}
	val, ok := as.cache.Get(authToken)
	if ok {
		claimsResponse := val.(*Claims)
		if claimsResponse.StandardClaims.ExpiresAt > time.Now().Unix() {
			return claimsResponse, nil
		} else {
			as.cache.Remove(authToken)
		}
	}
	sc, err := as.provider.Verify(context.Background(), authToken)
	if err != nil {
		return nil, fmt.Errorf("%s%s", entities.ErrAuthInvalidAuthToken, err.Error())
	}
	claims := &Claims{
		ClientID:       "",
		StandardClaims: sc,
	}
	as.cache.Set(authToken, claims)
	return claims, nil
}
