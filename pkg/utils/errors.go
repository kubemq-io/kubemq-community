package utils

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

func CheckErr(err error, cmd ...*cobra.Command) {
	if err == nil {
		return
	}
	msg := err.Error()
	if !strings.HasPrefix(msg, "error: ") {
		msg = fmt.Sprintf("error: %s", msg)
	}
	fmt.Fprintln(os.Stderr, msg)
	if cmd != nil {
		if cmd[0].HasExample() {
			fmt.Fprintln(os.Stderr, "Try:", cmd[0].Example)
		}
	}
	os.Exit(1)
}
