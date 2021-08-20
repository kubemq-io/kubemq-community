package config

import (
	"io/ioutil"
)

type ResourceConfig struct {
	Filename string `json:"filename"`
	Data     string `json:"data"`
}

func defaultResourceConfig() *ResourceConfig {
	return &ResourceConfig{
		Filename: "",
		Data:     "",
	}
}
func (r *ResourceConfig) Get() ([]byte, error) {
	if r.Data != "" {
		return []byte(r.Data), nil
	}
	buff, err := ioutil.ReadFile(r.Filename)
	if err != nil {
		return nil, err
	}
	return buff, nil
}

func (r *ResourceConfig) Empty() bool {
	if r.Data != "" || r.Filename != "" {
		return false
	}
	return true
}
func (r *ResourceConfig) GetWithoutWithSpace() ([]byte, error) {
	if r.Data != "" {
		return cleanWhiteSpaces([]byte(r.Data)), nil
	}
	buff, err := ioutil.ReadFile(r.Filename)
	if err != nil {
		return nil, err
	}
	return cleanWhiteSpaces(buff), nil
}

func cleanWhiteSpaces(input []byte) []byte {
	var output []byte
	for i := 0; i < len(input); i++ {
		if input[i] != 9 && input[i] != 10 && input[i] != 11 && input[i] != 12 && input[i] != 13 && input[i] != 32 && input[i] != 133 && input[i] != 160 {
			output = append(output, input[i])
		}
	}
	return output
}
