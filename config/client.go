package config

type ClientConfig struct {
	GrpcHost   string `json:"grpcHost"`
	GrpcPort   int    `json:"grpcPort"`
	ApiAddress string `json:"apiAddress"`
	AuthToken  string `json:"authToken"`
	ClientID   string `json:"clientId"`
}

func defaultClientConfig() *ClientConfig {
	return &ClientConfig{
		GrpcHost:   "localhost",
		GrpcPort:   50000,
		ApiAddress: "http://localhost:8080",
		AuthToken:  "",
		ClientID:   "",
	}
}

func (c *ClientConfig) Validate() error {
	if c.GrpcHost == "" {
		return NewConfigurationErrorf("bad client grpc host configuration: value cannot be empty")
	}
	if err := validatePort(c.GrpcPort); err != nil {
		return NewConfigurationErrorf("bad client grpc port configuration: %s", err.Error())
	}
	if err := validateURL(c.ApiAddress); err != nil {
		return NewConfigurationErrorf("bad client api address configuration: %s", err.Error())
	}
	return nil
}
