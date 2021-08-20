package routing

import (
	"fmt"
	"regexp"
)

type RouteTableEntry struct {
	Key    string
	Routes string
}

func (rt *RouteTableEntry) Validate() error {
	var err error
	if rt.Key == "" {
		return fmt.Errorf("invalid routing table entry key, cannot be empty")
	}
	_, err = regexp.Compile(rt.Key)
	if err != nil {
		return fmt.Errorf("invalid routing table entry key, regular expression error: %s", err.Error())
	}
	if rt.Routes == "" {
		return fmt.Errorf("invalid routing table entry routes data, cannot be empty")
	}
	return nil
}
