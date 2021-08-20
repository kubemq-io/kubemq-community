package config

type AuthorizationConfig struct {
	Enable     bool   `json:"enable"`
	PolicyData string `json:"policyData"`
	FilePath   string `json:"filePath"`
	Url        string `json:"url"`
	AutoReload int    `json:"autoReload"`
}

func defaultAuthorizationConfig() *AuthorizationConfig {

	bindViperEnv(
		"Authorization.Enable",
		"Authorization.PolicyData",
		"Authorization.FilePath",
		"Authorization.Url",
		"Authorization.AutoReload",
	)

	return &AuthorizationConfig{
		Enable:     false,
		PolicyData: "",
		FilePath:   "",
		Url:        "",
		AutoReload: 0,
	}
}

func (a *AuthorizationConfig) Validate() error {
	if !a.Enable {
		return nil
	}
	if validateParameters(a.PolicyData, a.FilePath, a.Url) {
		return NewConfigurationError("missing authorization configuration parameters")
	}
	if a.AutoReload < 0 {
		return NewConfigurationError("invalid auto reload time, cannot be negative")
	}
	if a.PolicyData != "" {
		return nil
	}

	if a.FilePath != "" {
		return NewConfigurationErrorf("bad authorization configuration: %s", validateFileName(a.FilePath).Error())
	}

	if a.Url != "" {
		if err := validateURL(a.Url); err != nil {
			return NewConfigurationErrorf("bad authorization configuration: %s", err.Error())
		}
	}

	return nil
}
