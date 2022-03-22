package config

import "regexp"

type ApiConfig struct {
	Port             int    `json:"port"`
	NetworkTransport string `json:"networkTransport"`
}

func defaultApiConfig() *ApiConfig {
	bindViperEnv(
		"Api.Port",
		"Api.NetworkTransport",
	)
	return &ApiConfig{
		Port:             8080,
		NetworkTransport: "tcp",
	}
}

func (a *ApiConfig) Validate() error {
	validNetworkTransport, _ := regexp.Compile("tcp(4|6)?")
	if err := validatePort(a.Port); err != nil {
		return NewConfigurationErrorf("bad API config: %s", err.Error())
	}
	if validNetworkTransport.MatchString(a.NetworkTransport) {
		return NewConfigurationError("bad API configuration: NetworkTransport must be tcp or tcp4 or tcp6")
	}
	return nil
}
