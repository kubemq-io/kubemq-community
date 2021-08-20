package config

type RoutingConfig struct {
	Enable     bool   `json:"enable"`
	Data       string `json:"data"`
	FilePath   string `json:"filePath"`
	Url        string `json:"url"`
	AutoReload int    `json:"autoReload"`
}

func defaultRoutingConfig() *RoutingConfig {

	bindViperEnv(
		"Routing.Enable",
		"Routing.Data",
		"Routing.FilePath",
		"Routing.Url",
		"Routing.AutoReload",
	)

	return &RoutingConfig{
		Enable:     false,
		Data:       "",
		FilePath:   "",
		Url:        "",
		AutoReload: 0,
	}
}

func (a *RoutingConfig) Validate() error {
	if !a.Enable {
		return nil
	}
	if validateParameters(a.Data, a.FilePath, a.Url) {
		return NewConfigurationError("missing routing configuration parameters")
	}
	if a.AutoReload < 0 {
		return NewConfigurationError("invalid auto reload time, cannot be negative")
	}
	if a.Data != "" {
		return nil
	}

	if a.FilePath != "" {
		return NewConfigurationErrorf("bad routing configuration: %s", validateFileName(a.FilePath).Error())
	}

	if a.Url != "" {
		if err := validateURL(a.Url); err != nil {
			return NewConfigurationErrorf("bad routing configuration: %s", err.Error())
		}
	}

	return nil
}
