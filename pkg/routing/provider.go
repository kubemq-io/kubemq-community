package routing

type Provider interface {
	GetData() []*RouteTableEntry
	Load() error
}
