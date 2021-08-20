package routing

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

type MemoryProvider struct {
	data  []byte
	table []*RouteTableEntry
}

func (m *MemoryProvider) GetData() []*RouteTableEntry {

	return m.table
}
func (m *MemoryProvider) Load() error {
	buff := m.data
	base64Buff, err := base64.StdEncoding.DecodeString(string(m.data))
	if err == nil {
		buff = base64Buff
	}
	err = json.Unmarshal(buff, &m.table)
	if err != nil {
		return fmt.Errorf("error unmarshaling rouuting table data: %s", err.Error())
	}
	for _, entry := range m.table {
		err := entry.Validate()
		if err != nil {
			return fmt.Errorf("validation error on routing table entry, key: %s routes: %s, error: %s", entry.Key, entry.Routes, err.Error())
		}
	}
	return nil
}

func NewMemoryProvider(data []byte) *MemoryProvider {
	return &MemoryProvider{
		data: data,
	}
}
