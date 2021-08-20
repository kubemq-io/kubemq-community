package api

type Info struct {
	Host      string `json:"host"`
	Version   string `json:"version"`
	IsHealthy bool   `json:"is_healthy"`
	IsReady   bool   `json:"is_ready"`
}

func NewInfo() *Info {
	return &Info{
		Host:      "",
		Version:   "",
		IsHealthy: false,
		IsReady:   false,
	}
}

func (i *Info) SetHost(value string) *Info {
	i.Host = value
	return i
}

func (i *Info) SetVersion(value string) *Info {
	i.Version = value
	return i
}

func (i *Info) SetIsHealthy(value bool) *Info {
	i.IsHealthy = value
	return i
}

func (i *Info) SetIsReady(value bool) *Info {
	i.IsReady = value
	return i
}
