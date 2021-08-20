package client

import (
	"github.com/kubemq-io/broker/pkg/pipe"
	"time"
)

type Options struct {
	ClientID          string
	Deadline          time.Duration
	MemoryPipe        *pipe.Pipe
	AutoReconnect     bool
	MaxInflight       int
	PubAckWaitSeconds int
}

func NewClientOptions(clientId string) *Options {
	return &Options{
		ClientID:          clientId,
		Deadline:          0,
		MemoryPipe:        nil,
		AutoReconnect:     true,
		MaxInflight:       2048,
		PubAckWaitSeconds: 60,
	}
}

func (o *Options) SetMemoryPipe(value *pipe.Pipe) *Options {
	o.MemoryPipe = value
	return o
}

func (o *Options) SetDeadline(value time.Duration) *Options {
	o.Deadline = value
	return o
}

func (o *Options) SetAutoReconnect(value bool) *Options {
	o.AutoReconnect = value
	return o
}
func (o *Options) SetMaxInflight(value int) *Options {
	o.MaxInflight = value
	return o
}
func (o *Options) SetPubAckWaitSeconds(value int) *Options {
	o.PubAckWaitSeconds = value
	return o
}
