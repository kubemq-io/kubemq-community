package config

import (
	"fmt"
	"net/url"
	"os"
)

func validateParameters(params ...string) bool {
	for _, param := range params {
		if param != "" {
			return false
		}
	}

	return true
}

func validateFileName(fileName string) error {
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		return fmt.Errorf("file %s dose not exist", fileName)
	}
	return nil
}

func validateURL(str string) error {
	u, err := url.Parse(str)

	if err != nil {
		return fmt.Errorf("url %s is invalid: %s", str, err.Error())
	}

	if u.Scheme == "" {
		return fmt.Errorf("url %s is invalid, no scheme", str)
	}
	if u.Host == "" {
		return fmt.Errorf("url %s is invalid, no host", str)
	}
	return nil
}

func validatePort(port int) error {
	if port <= 0 || port > 65535 {
		return fmt.Errorf("port %d is invalid", port)
	}
	return nil
}
