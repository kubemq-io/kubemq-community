package authentication

import (
	"fmt"
	"github.com/dgrijalva/jwt-go"
)

var JWTSignMethods = map[string]jwt.SigningMethod{
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

type SignSignature struct {
	key      interface{}
	signType jwt.SigningMethod
}

func CreateSignSignature(key []byte, method string) (*SignSignature, error) {
	ss := &SignSignature{}
	var ok bool
	var err error
	ss.signType, ok = JWTSignMethods[method]
	if !ok {
		return nil, fmt.Errorf("invalid jwt singing method: %s", method)
	}

	switch ss.signType {
	case jwt.SigningMethodHS256, jwt.SigningMethodHS384, jwt.SigningMethodHS512:
		ss.key = key
	case jwt.SigningMethodRS256, jwt.SigningMethodRS384, jwt.SigningMethodRS512:
		ss.key, err = jwt.ParseRSAPrivateKeyFromPEM(key)
		if err != nil {
			return nil, err
		}
	case jwt.SigningMethodES256, jwt.SigningMethodES384, jwt.SigningMethodES512:
		ss.key, err = jwt.ParseECPrivateKeyFromPEM(key)
		if err != nil {
			return nil, err
		}
	}
	return ss, nil
}

func (ss *SignSignature) Sign(claims jwt.Claims) (string, error) {
	var token *jwt.Token
	if claims != nil {
		token = jwt.NewWithClaims(ss.signType, claims)
	} else {
		token = jwt.New(ss.signType)
	}
	return token.SignedString(ss.key)
}

type VerifySignature struct {
	key      interface{}
	signType jwt.SigningMethod
}

func CreateVerifySignature(key []byte, method string) (*VerifySignature, error) {
	vs := &VerifySignature{}
	var ok bool
	var err error
	vs.signType, ok = JWTSignMethods[method]
	if !ok {
		return nil, fmt.Errorf("invalid jwt singing method: %s", method)
	}

	switch vs.signType {
	case jwt.SigningMethodHS256, jwt.SigningMethodHS384, jwt.SigningMethodHS512:
		vs.key = key
	case jwt.SigningMethodRS256, jwt.SigningMethodRS384, jwt.SigningMethodRS512:
		vs.key, err = jwt.ParseRSAPublicKeyFromPEM(key)
		if err != nil {
			return nil, err
		}
	case jwt.SigningMethodES256, jwt.SigningMethodES384, jwt.SigningMethodES512:
		vs.key, err = jwt.ParseECPublicKeyFromPEM(key)
		if err != nil {
			return nil, err
		}
	}
	return vs, nil
}

func (vs *VerifySignature) Verify(authToken string, claimsResponse jwt.Claims) error {
	token, err := jwt.ParseWithClaims(authToken, claimsResponse, func(token *jwt.Token) (i interface{}, err error) {
		if token.Method.Alg() != vs.signType.Alg() {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return vs.key, nil
	})
	if err != nil {
		return err
	}
	if !token.Valid {
		return fmt.Errorf("invalid auth token")
	}
	return nil
}
