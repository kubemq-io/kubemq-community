package broker

import (
	"encoding/json"
	"io/ioutil"
	"strings"
)

type loadStatus struct {
	LastError string
}

func NewFromFile(file string) (*loadStatus, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	l := &loadStatus{}
	err = json.Unmarshal(data, l)
	if err != nil {

		return nil, err
	}
	return l, nil
}

func (l *loadStatus) Save(file string) error {
	data, err := json.Marshal(l)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(file, data, 0600)
}
func (l *loadStatus) UpdateLastError(value string) *loadStatus {
	l.LastError = value
	return l
}
func (l *loadStatus) HasRecoveryErrors() bool {
	if strings.Contains(l.LastError, "EoF") {
		return true
	}
	if strings.Contains(l.LastError, "streaming state was recovered but cluster log path") {
		return true
	}

	return false
}
