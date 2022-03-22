package config

import (
	"regexp"
)

type GrpcConfig struct {
	Port             int    `json:"port"`
	SubBuffSize      int    `json:"subBuffSize"`
	BodyLimit        int    `json:"bodyLimit"`
	NetworkTransport string `json:"networkTransport"`
}

func defaultGRPCConfig() *GrpcConfig {
	bindViperEnv(
		"Grpc.Port",
		"Grpc.SubBuffSize",
		"Grpc.BodyLimit",
		"Grpc.NetworkTransport",
	)

	return &GrpcConfig{
		Port:             50000,
		SubBuffSize:      100,
		BodyLimit:        globalMaxBodySizeDefault,
		NetworkTransport: "tcp",
	}
}

func (g *GrpcConfig) Validate() error {
	validNetworkTransport, _ := regexp.Compile("tcp(4|6)?")
	if err := validatePort(g.Port); err != nil {
		return NewConfigurationErrorf("bad Grpc configuration: %s", err.Error())
	}
	if g.SubBuffSize < 0 {
		return NewConfigurationError("bad Grpc configuration: SubBuffSize cannot be negative")
	}
	if g.BodyLimit < 0 {
		return NewConfigurationError("bad Grpc configuration: BodyLimit cannot be negative")
	}
	if validNetworkTransport.MatchString(g.NetworkTransport) {
		return NewConfigurationError("bad Grpc configuration: NetworkTransport must be tcp or tcp4 or tcp6")
	}
	return nil
}
