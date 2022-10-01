package main

import (
	"embed"
	"fmt"
	"github.com/kubemq-io/kubemq-community/cmd/root"
	"github.com/kubemq-io/kubemq-community/services/api"
	"os"
)

var (
	version = ""
)

//go:embed assets/*
var staticAssets embed.FS

func main() {
	api.StaticAssets = staticAssets
	if err := root.Execute(version, os.Args); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
