package config

import (
	"fmt"
)

type SecurityModeType int

const (
	SecurityModeNone SecurityModeType = iota
	SecurityModeTLS
	SecurityModeMTLS
)

type SecurityConfig struct {
	Cert *ResourceConfig `json:"cert"`
	Key  *ResourceConfig `json:"key"`
	Ca   *ResourceConfig `json:"ca"`
}

func defaultSecurityConfig() *SecurityConfig {
	bindViperEnv(
		"Security.Cert.Filename",
		"Security.Cert.Data",
		"Security.Key.Filename",
		"Security.Key.Data",
		"Security.Ca.Filename",
		"Security.Ca.Data",
	)
	return &SecurityConfig{
		Cert: defaultResourceConfig(),
		Key:  defaultResourceConfig(),
		Ca:   defaultResourceConfig(),
	}
}

func (s *SecurityConfig) Validate() error {
	switch s.Mode() {
	case SecurityModeNone:
		return nil
	case SecurityModeTLS:
		_, err := s.Cert.Get()
		if err != nil {
			return fmt.Errorf("bad security TLS configuration: Cert data is invalid: %s", err.Error())
		}
		_, err = s.Key.Get()
		if err != nil {
			return fmt.Errorf("bad security TLS configuration: Key data is invalid: %s", err.Error())
		}
		return nil
	case SecurityModeMTLS:
		_, err := s.Cert.Get()
		if err != nil {
			return fmt.Errorf("bad security mTLS configuration: Cert data is invalid: %s", err.Error())
		}
		_, err = s.Key.Get()
		if err != nil {
			return fmt.Errorf("bad security mTLS configuration: Key data is invalid: %s", err.Error())
		}
		_, err = s.Ca.Get()
		if err != nil {
			return fmt.Errorf("bad security mTLS configuration: CA data is invalid: %s", err.Error())
		}
		return nil
	}

	return fmt.Errorf("bad security configuration: invalid mode")
}

func (s *SecurityConfig) Mode() SecurityModeType {

	if s.Key != nil && !s.Key.Empty() && s.Cert != nil && !s.Cert.Empty() {
		if s.Ca != nil && !s.Ca.Empty() {
			return SecurityModeMTLS
		}
		return SecurityModeTLS
	}
	return SecurityModeNone
}
