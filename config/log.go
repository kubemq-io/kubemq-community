package config

type LogLevelType string

const (
	LogLevelTypeTrace = "trace"
	LogLevelTypeDebug = "debug"
	LogLevelTypeInfo  = "info"
	LogLevelTypeWarn  = "warn"
	LogLevelTypeError = "error"
	LogLevelTypeFatal = "fatal"
)

type LogConfig struct {
	Level LogLevelType `json:"level"`
}

func defaultLogConfig() *LogConfig {
	bindViperEnv(
		"Log.Level",
	)
	return &LogConfig{
		Level: LogLevelTypeInfo,
	}
}

func (l *LogConfig) Validate() error {
	if l.Level == "" {
		return NewConfigurationError("bad logging configuration: invalid log level")
	}
	return nil
}
