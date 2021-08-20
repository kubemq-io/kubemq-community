package logging

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

type Logger struct {
	*zap.SugaredLogger
}

func (l *Logger) Noticef(format string, v ...interface{}) {
	str := fmt.Sprintf(format, v...)
	str = strings.Replace(str, "nats", "broker", -1)
	l.Info(str)
}

func (l *Logger) Tracef(format string, v ...interface{}) {
	str := fmt.Sprintf(format, v...)
	str = strings.Replace(str, "nats", "broker", -1)
	l.Debug(str)
}
func (l *Logger) Fatalf(format string, v ...interface{}) {
	str := fmt.Sprintf(format, v...)
	str = strings.Replace(str, "nats", "broker", -1)
	l.DPanicf(str)
}

func (l *Logger) NewWith(p1, p2 string) *Logger {
	return &Logger{SugaredLogger: l.SugaredLogger.With(p1, p2)}
}

func (l *Logger) Write(p []byte) (n int, err error) {
	l.Info(string(p))
	return len(p), nil
}
