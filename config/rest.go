package config

import (
	"fmt"
	"net/http"
)

type RestConfig struct {
	Port         int             `json:"port"`
	ReadTimeout  int             `json:"readTimeout"`
	WriteTimeout int             `json:"writeTimeout"`
	SubBuffSize  int             `json:"subBuffSize"`
	BodyLimit    string          `json:"bodyLimit"`
	Cors         *RestCorsConfig `json:"cors"`
}

func defaultRestConfig() *RestConfig {
	bindViperEnv(
		"Rest.Enable",
		"Rest.Port",
		"Rest.SubBuffSize",
		"Rest.BodyLimit",
		"Rest.ReadTimeout",
		"Rest.WriteTimeout",
		"Rest.Cors.AllowOrigins",
		"Rest.Cors.AllowMethods",
		"Rest.Cors.AllowHeaders",
		"Rest.Cors.AllowCredentials",
		"Rest.Cors.ExposeHeaders",
		"Rest.Cors.MaxAge",
	)

	return &RestConfig{
		Port:         9090,
		ReadTimeout:  60,
		WriteTimeout: 60,
		SubBuffSize:  100,
		BodyLimit:    "",
		Cors: &RestCorsConfig{
			AllowOrigins:     []string{"*"},
			AllowMethods:     []string{http.MethodGet, http.MethodPost},
			AllowHeaders:     []string{},
			AllowCredentials: false,
			ExposeHeaders:    []string{},
			MaxAge:           0,
		},
	}
}

func (r *RestConfig) Validate() error {

	if err := validatePort(r.Port); err != nil {
		return NewConfigurationErrorf("bad REST configuration: %s", err.Error())
	}
	if r.SubBuffSize < 0 {
		return NewConfigurationError("bad REST configuration: SubBuffSize cannot be negative")
	}
	if r.ReadTimeout < 0 {
		return NewConfigurationError("bad REST configuration: ReadTimeout cannot be negative")
	}
	if r.WriteTimeout < 0 {
		return NewConfigurationError("bad REST configuration: WriteTimeout cannot be negative")
	}
	if r.Cors == nil {
		return NewConfigurationError("bad Rest configuration: missing Cors configuration")
	}
	if err := r.Cors.Validate(); err != nil {
		return NewConfigurationErrorf("bad REST configuration: %s", err.Error())
	}
	return nil
}

type RestCorsConfig struct {
	AllowOrigins     []string `json:"allowOrigins"`
	AllowMethods     []string `json:"allowMethods"`
	AllowHeaders     []string `json:"allowHeaders"`
	AllowCredentials bool     `json:"allowCredentials"`
	ExposeHeaders    []string `json:"exposeHeaders"`
	MaxAge           int      `json:"maxAge"`
}

func (rc *RestCorsConfig) Validate() error {
	if len(rc.AllowOrigins) == 0 {
		return fmt.Errorf("bad Cors configuration: AllowOrigins cannot be empty")
	}
	if len(rc.AllowMethods) == 0 {
		return fmt.Errorf("bad Cors configuration: AllowMethods cannot be empty")
	}
	if rc.MaxAge < 0 {
		return fmt.Errorf("bad Cors configuration: MaxAge cannot be negative")
	}
	return nil
}
