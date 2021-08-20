package config

type QueueConfig struct {
	MaxNumberOfMessages       int32 `json:"maxNumberOfMessages"`
	MaxWaitTimeoutSeconds     int32 `json:"maxWaitTimeoutSeconds"`
	MaxExpirationSeconds      int32 `json:"maxExpirationSeconds"`
	MaxDelaySeconds           int32 `json:"maxDelaySeconds"`
	MaxReceiveCount           int32 `json:"maxReceiveCount"`
	MaxVisibilitySeconds      int32 `json:"maxVisibilitySeconds"`
	DefaultVisibilitySeconds  int32 `json:"defaultVisibilitySeconds"`
	DefaultWaitTimeoutSeconds int32 `json:"defaultWaitTimeoutSeconds"`
	MaxInflight               int32 `json:"maxInflight"`
	PubAckWaitSeconds         int32 `json:"pubAckWaitSeconds"`
}

func defaultQueueConfig() *QueueConfig {
	bindViperEnv(
		"Queue.MaxNumberOfMessages",
		"Queue.MaxWaitTimeoutSeconds",
		"Queue.MaxExpirationSeconds",
		"Queue.MaxDelaySeconds",
		"Queue.MaxReceiveCount",
		"Queue.MaxVisibilitySeconds",
		"Queue.DefaultVisibilitySeconds",
		"Queue.DefaultWaitTimeoutSeconds",
		"Queue.MaxInflight",
		"Queue.PubAckWaitSeconds",
	)
	return &QueueConfig{
		MaxNumberOfMessages:       1024,
		MaxWaitTimeoutSeconds:     3600,
		MaxExpirationSeconds:      43200,
		MaxDelaySeconds:           43200,
		MaxReceiveCount:           1024,
		MaxVisibilitySeconds:      43200,
		DefaultVisibilitySeconds:  60,
		DefaultWaitTimeoutSeconds: 1,
		MaxInflight:               2048,
		PubAckWaitSeconds:         60,
	}
}

func (q *QueueConfig) Validate() error {

	if q.MaxNumberOfMessages < 0 {
		return NewConfigurationError("bad queue configuration: MaxNumberOfMessages cannot be negative")
	}

	if q.MaxWaitTimeoutSeconds < 0 {
		return NewConfigurationError("bad queue configuration: MaxWaitTimeoutSeconds cannot be negative")
	}
	if q.MaxExpirationSeconds < 0 {
		return NewConfigurationError("bad queue configuration: MaxExpirationSeconds cannot be negative")
	}
	if q.MaxDelaySeconds < 0 {
		return NewConfigurationError("bad queue configuration: MaxDelaySeconds cannot be negative")
	}

	if q.MaxReceiveCount < 0 {
		return NewConfigurationError("bad queue configuration: MaxVisibilitySeconds cannot be negative")
	}
	if q.MaxVisibilitySeconds < 0 {
		return NewConfigurationError("bad queue configuration: MaxNumberOfMessages cannot be negative")
	}
	if q.DefaultVisibilitySeconds < 0 {
		return NewConfigurationError("bad queue configuration: DefaultVisibilitySeconds cannot be negative")
	}
	if q.DefaultWaitTimeoutSeconds < 0 {
		return NewConfigurationError("bad queue configuration: DefaultWaitTimeoutSeconds cannot be negative")
	}
	if q.MaxInflight < 0 {
		return NewConfigurationError("bad queue configuration: MaxInflight cannot be negative")
	}
	if q.PubAckWaitSeconds < 0 {
		return NewConfigurationError("bad queue configuration: PubAckWaitSeconds cannot be negative")
	}

	return nil
}
