package main

import (
	"fmt"
	"github.com/kubemq-io/kubemq-community/cmd/root"
	"os"
)

var (
	version = ""
)

func main() {
	if err := root.Execute(version, os.Args); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
