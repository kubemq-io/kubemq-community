package broker

import (
	"sync"
)

type HealthNotifier struct {
	sync.Mutex
	subscribers map[string]func(state bool)
}

func NewHealthNotifier() *HealthNotifier {
	return &HealthNotifier{subscribers: map[string]func(state bool){}}
}

func (hn *HealthNotifier) Send(value bool) {
	hn.Lock()
	defer hn.Unlock()
	for _, sub := range hn.subscribers {
		sub(value)
	}
}
func (hn *HealthNotifier) Register(name string, sub func(state bool)) {
	hn.Lock()
	defer hn.Unlock()
	hn.subscribers[name] = sub
}
func (hn *HealthNotifier) UnRegister(name string) {
	hn.Lock()
	defer hn.Unlock()
	delete(hn.subscribers, name)
}
