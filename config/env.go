package config

import (
	"github.com/spf13/viper"
	"os"
	"regexp"
	"strings"
)

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func ToSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}
func convertEnvFormat(str string) string {
	str1 := ToSnakeCase(str)
	str2 := strings.Replace(str1, ".", "", -1)
	return strings.ToUpper(str2)
}

func bindViperEnv(keys ...string) {
	for _, key := range keys {
		_ = viper.BindEnv(key, convertEnvFormat(key))
	}
}

func setEnvValues(kv map[string]string) {
	for key, value := range kv {
		_ = os.Setenv(convertEnvFormat(key), value)

	}
}
