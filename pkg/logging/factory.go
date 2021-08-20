package logging

import (
	"context"
	"github.com/kubemq-io/kubemq-community/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var loggerFactory *LoggerFactory
var core = initCore()
var encoderConfig = zapcore.EncoderConfig{
	TimeKey:        "time",
	LevelKey:       "level",
	NameKey:        "name",
	CallerKey:      "caller",
	MessageKey:     "msg",
	StacktraceKey:  "stack",
	LineEnding:     zapcore.DefaultLineEnding,
	EncodeLevel:    zapcore.CapitalLevelEncoder,
	EncodeTime:     zapcore.ISO8601TimeEncoder,
	EncodeDuration: zapcore.StringDurationEncoder,
	EncodeCaller:   zapcore.FullCallerEncoder,
}

func initCore() zapcore.Core {
	var w zapcore.WriteSyncer
	std, _, _ := zap.Open("stdout")

	w = zap.CombineWriteSyncers(std)
	enc := zapcore.NewConsoleEncoder(encoderConfig)
	return zapcore.NewCore(
		enc,
		zapcore.AddSync(w),
		zapcore.DebugLevel)
}

type LoggerFactory struct {
	Stopped      chan struct{}
	host         string
	cfg          *config.LogConfig
	LoggingLevel zap.AtomicLevel
	isDebug      bool
}

func GetLogFactory() *LoggerFactory {
	if loggerFactory == nil {
		loggerFactory = CreateLoggerFactory(context.Background(), config.Host(), &config.LogConfig{Level: "debug"})
	}
	return loggerFactory
}
func Close() {
	if loggerFactory != nil {
		loggerFactory.Shutdown()
	}

}

func CreateLoggerFactory(ctx context.Context, host string, logConfig *config.LogConfig) *LoggerFactory {
	if loggerFactory != nil {
		return loggerFactory
	}
	lf := &LoggerFactory{
		Stopped:      make(chan struct{}, 1),
		cfg:          logConfig,
		LoggingLevel: zap.NewAtomicLevel(),
		host:         host,
	}
	lf.SetLevel(logConfig.Level)
	go lf.watcher(ctx)
	loggerFactory = lf
	return lf
}
func (lf *LoggerFactory) watcher(ctx context.Context) {
	<-ctx.Done()
	lf.Shutdown()
}
func (lf *LoggerFactory) Shutdown() {
	lf.Stopped <- struct{}{}
}

func (lf *LoggerFactory) SetLevel(value config.LogLevelType) {
	lf.isDebug = false
	switch value {
	case config.LogLevelTypeTrace:
		lf.LoggingLevel = zap.NewAtomicLevelAt(zap.DebugLevel)
		lf.isDebug = true
	case config.LogLevelTypeDebug:
		lf.LoggingLevel = zap.NewAtomicLevelAt(zap.DebugLevel)
		lf.isDebug = true
	case config.LogLevelTypeInfo:
		lf.LoggingLevel = zap.NewAtomicLevelAt(zap.InfoLevel)
	case config.LogLevelTypeWarn:
		lf.LoggingLevel = zap.NewAtomicLevelAt(zap.WarnLevel)
	case config.LogLevelTypeError:
		lf.LoggingLevel = zap.NewAtomicLevelAt(zap.ErrorLevel)
	case config.LogLevelTypeFatal:
		lf.LoggingLevel = zap.NewAtomicLevelAt(zap.FatalLevel)
	default:
		lf.LoggingLevel = zap.NewAtomicLevelAt(zap.DebugLevel)
		lf.isDebug = true
	}
}

func (lf *LoggerFactory) NewLogger(name string) *Logger {
	zapLogger := zap.New(core, zap.IncreaseLevel(lf.LoggingLevel))
	l := &Logger{
		SugaredLogger: zapLogger.Sugar().With("host", lf.host, "module", name),
	}
	return l

}

func (lf *LoggerFactory) NewEchoLogger(name string) *EchoLogger {
	zapLogger := zap.New(core, zap.IncreaseLevel(lf.LoggingLevel))
	l := &Logger{
		SugaredLogger: zapLogger.Sugar().With("host", lf.host, "module", name),
	}

	el := &EchoLogger{
		Logger: l,
	}
	return el

}
