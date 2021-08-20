package local

import (
	"context"
	"fmt"
	"github.com/golang-jwt/jwt/v4"
)

var jwtSignMethods = map[string]jwt.SigningMethod{
	"HS256": jwt.SigningMethodHS256,
	"HS384": jwt.SigningMethodHS384,
	"HS512": jwt.SigningMethodHS512,
	"RS256": jwt.SigningMethodRS256,
	"RS384": jwt.SigningMethodRS384,
	"RS512": jwt.SigningMethodRS512,
	"ES256": jwt.SigningMethodES256,
	"ES384": jwt.SigningMethodES384,
	"ES512": jwt.SigningMethodES512,
}

type Local struct {
	key      interface{}
	signType jwt.SigningMethod
}

func NewLocal() *Local {
	return &Local{}
}
func (l *Local) Init(config string) error {
	ops := NewOptions(config)
	if err := ops.Validate(); err != nil {
		return err
	}
	l.signType = jwtSignMethods[ops.Method]
	switch l.signType {
	case jwt.SigningMethodHS256, jwt.SigningMethodHS384, jwt.SigningMethodHS512:
		l.key = []byte(ops.Key)
	case jwt.SigningMethodRS256, jwt.SigningMethodRS384, jwt.SigningMethodRS512:
		var err error
		l.key, err = jwt.ParseRSAPublicKeyFromPEM([]byte(ops.Key))
		if err != nil {
			return err
		}
	case jwt.SigningMethodES256, jwt.SigningMethodES384, jwt.SigningMethodES512:
		var err error
		l.key, err = jwt.ParseECPublicKeyFromPEM([]byte(ops.Key))
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *Local) Verify(ctx context.Context, token string) (jwt.StandardClaims, error) {
	claimsResponse := jwt.StandardClaims{}
	_, err := jwt.ParseWithClaims(token, &claimsResponse, func(authToken *jwt.Token) (i interface{}, err error) {
		if authToken.Method.Alg() != l.signType.Alg() {
			return nil, fmt.Errorf("unexpected signing method: %v", authToken.Header["alg"])
		}
		return l.key, nil
	})
	if err != nil {
		return jwt.StandardClaims{}, err
	}
	return claimsResponse, nil
}
