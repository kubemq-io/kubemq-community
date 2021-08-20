package config

import (
	"fmt"
	"strings"
)

type StoreConfig struct {
	CleanStore       bool   `json:"cleanStore"`
	StorePath        string `json:"storePath"`
	MaxQueues        int    `json:"maxQueues"`
	MaxSubscribers   int    `json:"maxSubscribers"`
	MaxMessages      int    `json:"maxMessages"`
	MaxQueueSize     int64  `json:"maxQueueSize"`
	MaxRetention     int    `json:"maxRetention"`
	MaxPurgeInactive int    `json:"maxPurgeInactive"`
}

func defaultStoreConfig() *StoreConfig {
	bindViperEnv(
		"Store.CleanStore",
		"Store.StorePath",
		"Store.MaxQueues",
		"Store.MaxSubscribers",
		"Store.MaxMessages",
		"Store.MaxQueueSize",
		"Store.MaxRetention",
		"Store.MaxPurgeInactive",
	)
	return &StoreConfig{
		CleanStore:       false,
		StorePath:        "./store",
		MaxQueues:        0,
		MaxSubscribers:   0,
		MaxMessages:      0,
		MaxQueueSize:     0,
		MaxRetention:     1440,
		MaxPurgeInactive: 1440,
	}
}

func (s *StoreConfig) Validate() error {
	if s.StorePath == "" {
		return NewConfigurationError("bad store configuration: store path cannot be empty")
	}
	if strings.HasPrefix(s.StorePath, "/") {
		s.StorePath = fmt.Sprintf(".%s", s.StorePath)
	}
	if s.MaxQueues < 0 {
		return NewConfigurationError("bad store configuration: MaxQueues cannot be negative")
	}
	if s.MaxSubscribers < 0 {
		return NewConfigurationError("bad store configuration: MaxSubscribers cannot be negative")
	}
	if s.MaxMessages < 0 {
		return NewConfigurationError("bad store configuration: MaxMessages cannot be negative")
	}

	if s.MaxQueueSize < 0 {
		return NewConfigurationError("bad store configuration: MaxQueueSize cannot be negative")
	}
	if s.MaxRetention < 0 {
		return NewConfigurationError("bad store configuration: MaxRetention cannot be negative")
	}

	if s.MaxPurgeInactive < 0 {
		return NewConfigurationError("bad store configuration: MaxPurgeInactive cannot be negative")
	}
	return nil
}
