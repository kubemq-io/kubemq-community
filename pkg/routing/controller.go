package routing

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Controller struct {
	sync.Mutex
	provider Provider
	*RouteTable
}

func NewController(ctx context.Context, provider Provider, reloadInterval time.Duration) (*Controller, error) {
	if provider == nil {
		return &Controller{

			provider:   nil,
			RouteTable: newRouteTable(nil),
		}, nil
	}
	c := &Controller{
		provider: provider,
	}

	if err := c.provider.Load(); err != nil {
		return nil, err
	}
	if err := c.initRoutingTable(c.provider.GetData()); err != nil {
		return nil, err
	}
	if reloadInterval > 0 {
		go c.runReloadWorker(ctx, reloadInterval)
	}
	return c, nil
}
func (c *Controller) Close() {

}
func (c *Controller) initRoutingTable(table []*RouteTableEntry) error {
	c.Lock()
	defer c.Unlock()
	c.RouteTable = newRouteTable(table)
	return nil

}
func (c *Controller) ReloadTable() error {
	err := c.provider.Load()
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error on reload routing table entries list: %s", err.Error()))
	}
	return c.initRoutingTable(c.provider.GetData())
}

func (c *Controller) runReloadWorker(ctx context.Context, reloadInterval time.Duration) {
	for {
		select {
		case <-time.After(reloadInterval):
			_ = c.ReloadTable()
		case <-ctx.Done():
			return
		}
	}
}

func (c *Controller) GetRoutes(channel string) *Route {
	route := NewRoute().Parse(channel)
	if c.RouteTable != nil {
		c.Lock()
		defer c.Unlock()
		c.RouteTable.Parse(route)
	}
	return route
}
