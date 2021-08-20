package local

import (
	"encoding/json"
	"fmt"
	"github.com/ghodss/yaml"
)

type Options struct {
	Method string `json:"method"`
	Key    string `json:"key"`
}

func NewOptions(config string) *Options {
	ops := &Options{}
	err := json.Unmarshal([]byte(config), ops)
	if err != nil {
		_ = yaml.Unmarshal([]byte(config), ops)
	}
	return ops
}

func (o *Options) Validate() error {
	_, ok := jwtSignMethods[o.Method]
	if !ok {
		return fmt.Errorf("invalid jwt signature method")
	}
	if o.Key == "" {
		return fmt.Errorf("jwt public key cannot be empty")
	}
	return nil
}
func (o *Options) Marshal() string {
	data, _ := json.Marshal(o)
	return string(data)
}
