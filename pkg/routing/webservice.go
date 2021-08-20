package routing

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/http"
)

type WebServiceProvider struct {
	url   string
	table []*RouteTableEntry
}

func (w *WebServiceProvider) GetData() []*RouteTableEntry {
	return w.table
}
func (w *WebServiceProvider) Load() error {

	err := http.Get(context.Background(), w.url, &w.table)
	if err != nil {
		return fmt.Errorf("error unmarshaling rouuting table data: %s", err.Error())
	}
	for _, entry := range w.table {
		err := entry.Validate()
		if err != nil {
			return fmt.Errorf("validation error on routing table entry, key: %s routes: %s, error: %s", entry.Key, entry.Routes, err.Error())
		}
	}

	return nil
}

func NewWebServiceProvider(url string) *WebServiceProvider {
	return &WebServiceProvider{
		url: url,
	}
}
