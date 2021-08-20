package rest

import (
	"github.com/labstack/echo/v4"
	"go.uber.org/atomic"
)

func Traffic(acceptTraffic *atomic.Bool) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) (err error) {
			if !acceptTraffic.Load() {
				res := NewResponse(c)
				return res.SetHttpCode(503).SetErrorWithText("rest server is not ready to accept requests").Send()
			}
			err = next(c)
			return err
		}
	}
}
