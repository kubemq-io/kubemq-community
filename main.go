package main

import (
	"fmt"
	"github.com/kubemq-io/kubemq-community/server"
	_ "go.uber.org/automaxprocs"
	"os"
	"os/signal"
	"syscall"
)
var (
	version = ""
)


func run() error {
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	srv := server.New().SetVersion(version)
	err := srv.Run()
	if err != nil {
		return err
	}

	<-gracefulShutdown
	srv.Close()
	return nil
}
func main() {
	if err := run(); err != nil {
		_,_=fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
