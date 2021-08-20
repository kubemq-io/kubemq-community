package array

import (
	"github.com/kubemq-io/kubemq-community/pkg/authorization"
)

func (a *Array) Authorize(obj interface{}) error {
	if a.authorizationService != nil {
		return a.authorizationService.Enforce(authorization.NewAccessControlRecord(obj))
	}
	return nil
}
