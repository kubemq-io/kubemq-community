package config

import (
	"fmt"
)

func NewConfigurationError(errStr string) error {
	return fmt.Errorf("configuration validation error: %s", errStr)
}

func NewConfigurationErrorf(format string, args ...interface{}) error {
	return NewConfigurationError(fmt.Sprintf(format, args...))
}
