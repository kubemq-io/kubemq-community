package authentication

import (
	"context"
	"github.com/golang-jwt/jwt/v4"
)

type Provider interface {
	Init(config string) error
	Verify(ctx context.Context, token string) (jwt.StandardClaims, error)
}
