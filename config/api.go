package config

type ApiConfig struct {
	Port int `json:"port"`
}

func defaultApiConfig() *ApiConfig {
	bindViperEnv(
		"Api.Port",
	)
	return &ApiConfig{
		Port: 8080,
	}
}

func (a *ApiConfig) Validate() error {
	if err := validatePort(a.Port); err != nil {
		return NewConfigurationErrorf("bad API config: %s", err.Error())
	}
	return nil
}
