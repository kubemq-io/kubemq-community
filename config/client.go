package config

type ClientConfig struct {
	GrpcHost   string `json:"grpc_host"`
	GrpcPort   int    `json:"grpc_port"`
	ApiAddress string `json:"api_address"`
	AuthToken  string `json:"auth_token"`
}

func defaultClientConfig() *ClientConfig {
	return &ClientConfig{
		GrpcHost:   "localhost",
		GrpcPort:   50000,
		ApiAddress: "localhost:8080",
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
