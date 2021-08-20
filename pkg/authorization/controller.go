package authorization

import (
	"context"
	"fmt"
	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	"regexp"
	"strings"
	"time"
)

const modelStr = `
[request_definition]
r =res, sub, obj, act

[policy_definition]
p = res, sub, obj, act

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = r.res==p.res && regexMatch(r.sub, p.sub) && regexMatch(r.obj, p.obj) && regexMatch(r.act,p.act)
`

type Controller struct {
	provider Provider
	enforcer *casbin.Enforcer
}

func NewController(ctx context.Context, provider Provider, reloadInterval time.Duration) (*Controller, error) {
	c := &Controller{
		provider: provider,
	}

	if err := c.provider.Load(); err != nil {
		return nil, err
	}
	err := c.initEnforcer(c.provider.GetPolicy())
	if err != nil {
		return nil, err
	}
	if reloadInterval > 0 {
		go c.runReloadWorker(ctx, reloadInterval)
	}
	return c, nil
}
func (c *Controller) Close() {

}

func validatePolicy(policy string) error {
	lines := strings.Split(strings.Replace(policy, "\r\n", "\n", -1), "\n")
	re, err := regexp.Compile(`(?ms)\S+,\S+,\S+,(read|write|.*|deny)`)
	if err != nil {
		return err
	}
	for index, line := range lines {
		if line == "" {
			continue
		}
		matches := re.FindAllString(line, -1)
		if len(matches) == 1 {
			fields := strings.Split(matches[0], ",")
			for i, field := range fields {
				_, err := regexp.Compile(field)
				if err != nil {
					return fmt.Errorf("error at line: %d, position: %d, field: %s, error: %s", index, i, field, err.Error())
				}
			}
		} else {
			return fmt.Errorf("error at line %d, worng csv line format: %s", index, line)
		}

	}
	return nil
}

func (c *Controller) initEnforcer(policy string) error {
	m, err := model.NewModelFromString(modelStr)
	if err != nil {
		return fmt.Errorf("error on create casbin model: %s", err.Error())
	}
	c.enforcer, err = casbin.NewEnforcer(m)
	if err != nil {
		return fmt.Errorf("error on create casbin enforcer: %s", err.Error())
	}

	rules := strings.Split(policy, "\n")
	for _, rule := range rules {
		fields := strings.Split(rule, ",")
		ok, err := c.enforcer.AddPolicy(fields)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("rule %s was not added", rule)
		}
	}
	return nil

}

func (c *Controller) Enforce(params ...interface{}) (bool, error) {
	res, err := c.enforcer.Enforce(params...)
	if err != nil {
		return false, fmt.Errorf("error on enforce parameters: %s", err.Error())
	}
	return res, nil
}
func (c *Controller) ReloadPolicy() error {
	err := c.provider.Load()
	if err != nil {
		return fmt.Errorf("error on reload access control list: %s", err.Error())
	}
	return c.initEnforcer(c.provider.GetPolicy())
}

func (c *Controller) runReloadWorker(ctx context.Context, reloadInterval time.Duration) {
	for {
		select {
		case <-time.After(reloadInterval):
			_ = c.ReloadPolicy()
		case <-ctx.Done():
			return
		}
	}
}
