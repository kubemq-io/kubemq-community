package client

import (
	"context"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/nats-io/nuid"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAsyncDispatcher_DispatchAsyncSingleNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	a := getConfig(t)
	s1 := setupSingle(ctx, t, a)
	defer tearDownSingle(s1, a)
	defer cancel()
	o := getClientOptions(t, a)
	c, err := NewQueueClient(o, a.Queue)
	require.NoError(t, err)
	defer func() {
		_ = c.Disconnect()
	}()
	channel := nuid.Next()
	body := randStringBytes(10000)

	msgs := 10000
	var items []*DispatchItem
	for i := 0; i < msgs; i++ {
		msg := &pb.QueueMessage{
			MessageID: nuid.Next(),
			ClientID:  "some-client-id",
			Channel:   channel,
			Metadata:  "",
			Body:      body,
		}
		data, _ := msg.Marshal()
		items = append(items, &DispatchItem{
			id:      i,
			channel: msg.Channel,
			data:    data,
		})
	}

	ad := NewDispatcher(c.queueConn)
	results := ad.DispatchAsync(items)
	require.Equal(t, len(items), len(results))
}
