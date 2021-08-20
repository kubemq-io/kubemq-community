package routing

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type FileProvider struct {
	path  string
	table []*RouteTableEntry
}

func (f *FileProvider) GetData() []*RouteTableEntry {
	return f.table
}
func (f *FileProvider) Load() error {
	data, err := ioutil.ReadFile(f.path)
	if err != nil {
		return fmt.Errorf("error loading access route table data file: %s", err.Error())
	}
	err = json.Unmarshal(data, &f.table)
	if err != nil {
		return fmt.Errorf("error unmarshaling rouuting table file: %s", err.Error())
	}

	for _, entry := range f.table {
		err := entry.Validate()
		if err != nil {
			return fmt.Errorf("validation error on routing table entry, key: %s routes: %s, error: %s", entry.Key, entry.Routes, err.Error())
		}
	}

	return nil

}

func NewFileProvider(path string) *FileProvider {
	return &FileProvider{
		path: path,
	}
}
