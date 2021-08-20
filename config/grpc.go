package config

type GrpcConfig struct {
	Port        int `json:"port"`
	SubBuffSize int `json:"subBuffSize"`
	BodyLimit   int `json:"bodyLimit"`
}

func defaultGRPCConfig() *GrpcConfig {
	bindViperEnv(
		"Grpc.Port",
		"Grpc.SubBuffSize",
		"Grpc.BodyLimit",
	)

	return &GrpcConfig{
		Port:        50000,
		SubBuffSize: 100,
		BodyLimit:   globalMaxBodySizeDefault,
	}
}

func (g *GrpcConfig) Validate() error {
	if err := validatePort(g.Port); err != nil {
		return NewConfigurationErrorf("bad Grpc configuration: %s", err.Error())
	}
	if g.SubBuffSize < 0 {
		return NewConfigurationError("bad Grpc configuration: SubBuffSize cannot be negative")
	}
	if g.BodyLimit < 0 {
		return NewConfigurationError("bad Grpc configuration: BodyLimit cannot be negative")
	}
	return nil
}
