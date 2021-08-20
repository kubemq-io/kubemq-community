package authentication

import (
	"context"
	"github.com/dgrijalva/jwt-go"
)

type Provider interface {
	Init(config string) error
	Verify(ctx context.Context, token string) (jwt.StandardClaims, error)
}
