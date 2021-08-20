package logging

import (
	"io"

	"fmt"

	"github.com/labstack/gommon/log"
)

type EchoLogger struct {
	*Logger
	prefix string
	level  uint8
}

func (el *EchoLogger) Output() io.Writer {
	return el
}
func (el *EchoLogger) SetHeader(h string) {

}

func (el *EchoLogger) SetOutput(w io.Writer) {

}

func (el *EchoLogger) Prefix() string {
	return el.prefix
}

func (el *EchoLogger) SetPrefix(p string) {
	el.prefix = p
}

func (el *EchoLogger) Level() log.Lvl {
	return log.Lvl(el.level)
}

func (el *EchoLogger) SetLevel(v log.Lvl) {
	el.level = uint8(v)
}

func (el *EchoLogger) Print(i ...interface{}) {
	el.Info(i)
}

func (el *EchoLogger) Printf(format string, args ...interface{}) {
	str := fmt.Sprintf(format, args...)
	el.Info(str)
}

func (el *EchoLogger) Printj(j log.JSON) {
	el.Info(j)
}

func (el *EchoLogger) Debugj(j log.JSON) {
	el.Debug(j)
}

func (el *EchoLogger) Infoj(j log.JSON) {
	el.Info(j)
}

func (el *EchoLogger) Warnj(j log.JSON) {
	el.Warn(j)
}

func (el *EchoLogger) Errorj(j log.JSON) {
	el.Error(j)
}

func (el *EchoLogger) Fatalj(j log.JSON) {
	el.Fatal(j)
}

func (el *EchoLogger) Panicj(j log.JSON) {
	el.Fatal(j)
}
