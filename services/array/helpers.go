package array

import "strings"

func IsRoutable(channel string) bool {
	if strings.Contains(channel, ";") || strings.Contains(channel, ":") {
		return true
	}
	return false
}
