package authorization

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/http"
)

type WebServiceProvider struct {
	url string
	acl AccessControlList
}

func (w *WebServiceProvider) GetPolicy() string {
	return w.acl.RuleSet()
}
func (w *WebServiceProvider) Load() error {
	err := http.Get(context.Background(), w.url, &w.acl)
	if err != nil {
		return fmt.Errorf("error unmarshaling access control list file: %s", err.Error())
	}
	err = w.acl.Validate()
	if err != nil {
		return fmt.Errorf("access control list validation error: %s", err.Error())
	}
	return validatePolicy(w.acl.RuleSet())
}

func NewWebServiceProvider(url string) *WebServiceProvider {
	return &WebServiceProvider{
		url: url,
	}
}
