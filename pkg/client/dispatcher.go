package client

import (
	"github.com/kubemq-io/broker/client/stan"
	"go.uber.org/atomic"
	"sync"
)

type DispatchItem struct {
	id      int
	channel string
	data    []byte
}
type AsyncDispatcher struct {
	sync.Mutex
	conn stan.Conn
}

func NewDispatcher(conn stan.Conn) *AsyncDispatcher {
	ad := &AsyncDispatcher{
		Mutex: sync.Mutex{},
		conn:  conn,
	}
	return ad
}

func (ad *AsyncDispatcher) DispatchAsync(items []*DispatchItem) map[int]error {
	result := make(map[int]error)
	sentItems := atomic.NewInt32(0)
	receivedItems := atomic.NewInt32(0)
	doneCh := make(chan bool, 1)
	sendComplete := atomic.NewBool(false)
	for i := 0; i < len(items); i++ {
		item := items[i]
		sentItems.Inc()
		_, err := ad.conn.PublishAsync(item.channel, item.data, func(s string, err error) {
			ad.Lock()
			defer ad.Unlock()
			_, ok := result[item.id]
			if ok {
				return
			}
			result[item.id] = err
			receivedItems.Inc()
			if sendComplete.Load() && sentItems.Load() == receivedItems.Load() {
				doneCh <- true
			}
		})
		if err != nil {
			sentItems.Dec()
		}
	}

	sendComplete.Toggle()
	<-doneCh
	return result
}
