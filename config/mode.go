package config

import (
	"fmt"
	"runtime"
)

type ModeConfig struct {
	IsContainer bool
	Runtime     string
}

func defaultModeConfig() *ModeConfig {
	return &ModeConfig{
		IsContainer: false,
		Runtime:     fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}
