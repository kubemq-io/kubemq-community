package rest

import (
	"github.com/kubemq-io/kubemq-community/services/authentication"
	"github.com/labstack/echo/v4"
	"strings"
)

func Authenticate(auth *authentication.Service) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) (err error) {
			authHeaders := strings.Split(c.Request().Header.Get(echo.HeaderAuthorization), " ")
			res := NewResponse(c)
			if len(authHeaders) == 2 && authHeaders[0] == "Bearer" {
				token := authHeaders[1]
				_, err := auth.Authenticate(token)
				if err != nil {
					return res.SetError(err).SetHttpCode(401).Send()
				}
			} else {
				return res.SetErrorWithText("invalid authentication header").SetHttpCode(401).Send()
			}
			err = next(c)
			return err
		}
	}
}
