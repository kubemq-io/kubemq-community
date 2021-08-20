package client

import (
	"context"
	"fmt"
	"github.com/fortytw2/leaktest"
	"github.com/kubemq-io/kubemq-community/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/nats-io/nuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"sync"
	"testing"
	"time"
)

func isDuplicatedMessages(messages []*pb.QueueMessage) bool {
	dupMap := make(map[string]string)
	for _, msg := range messages {
		_, ok := dupMap[msg.MessageID]
		if ok {
			return true
		} else {
			dupMap[msg.MessageID] = msg.MessageID
		}
	}
	return false
}
func TestQueueClient_PollClient_AutoAck_Poll_Exact(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 2000,
			AutoAck:     true,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(int(request.MaxItems)), request.Channel, 100, 0, 0, 1, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))

}
func TestQueueClient_PollClient_AutoAck_Poll_Fewer(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 2000,
			AutoAck:     true,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(request.MaxItems)+5, request.Channel, 100, 0, 0, 1, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 5, len(response.Messages))
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))

}
func TestQueueClient_PollClient_AutoAck_Poll_Higher(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    20,
			WaitTimeout: 2000,
			AutoAck:     true,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, 15, request.Channel, 100, 0, 0, 1, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 15, len(response.Messages))
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))
}
func TestQueueClient_PollClient_AutoAck_CancelDuringWait(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 20000,
			AutoAck:     true,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, 5, request.Channel, 100, 0, 0, 1, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.Error(t, err)
	require.Nil(t, response)
	newRequest := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     request.Channel,
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 2000,
			AutoAck:     true,
		},
	}
	newResponse, err := sender.c.Poll(context.Background(), newRequest)
	require.NoError(t, err)
	require.EqualValues(t, 5, len(newResponse.Messages))

}
func TestQueueClient_PollClient_AutoAck_ConcurrentPoll_MultipleChannels_MultipleClients(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)

	channel := nuid.New().Next()
	maxItems := 1000000
	wg := sync.WaitGroup{}
	concurrent := 100
	wg.Add(concurrent)
	count := *atomic.NewInt32(0)
	timeCount := *atomic.NewInt64(0)
	for i := 0; i < concurrent; i++ {
		go func(index int) {
			localChannel := fmt.Sprintf("%s.%d", channel, index)
			sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
			require.NoError(t, err)
			err = sender.sendMessagesWithPolicy(ctx, maxItems/concurrent, localChannel, 100, 0, 0, 1, "")
			require.NoError(t, err)
			request := &QueueDownstreamRequest{
				TransactionId: "",
				QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
					Channel:     localChannel,
					ClientID:    uuid.New(),
					MaxItems:    int32(maxItems / concurrent),
					WaitTimeout: 10000,
					AutoAck:     true,
				},
			}
			start := time.Now()
			defer func() {
				timeCount.Add(time.Since(start).Nanoseconds())
				wg.Done()
			}()
			response, err := sender.c.Poll(ctx, request)
			if err != nil {
				return
			} else {
				require.NotNil(t, response)
				count.Add(int32(len(response.Messages)))
			}

		}(i)
	}
	wg.Wait()
	require.EqualValues(t, int32(maxItems), count.Load())
}
func TestQueueClient_PollClient_ManualAck_AckAll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 5000,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(request.MaxItems), request.Channel, 100, 0, 0, 1, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	time.Sleep(2 * time.Second)
	err = response.AckAll()
	require.NoError(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))

}
func TestQueueClient_PollClient_ManualAck_Poll_Close_AckAll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    20,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, 10, request.Channel, 100, 0, 0, 1, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(response.Messages))
	response.Close()
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(response.Messages))
	err = response.AckAll()
	require.NoError(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))
}
func TestQueueClient_PollClient_ManualAck_AckRange(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(request.MaxItems), request.Channel, 100, 0, 0, 1, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	err = response.AckRange([]int64{1, 3, 5})
	require.NoError(t, err)
	response.Close()
	require.NoError(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 7, len(response.Messages))
	err = response.AckRange([]int64{2, 4, 6})
	require.NoError(t, err)
	err = response.AckRange([]int64{7, 8, 9})
	require.NoError(t, err)
	response.Close()
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 1, len(response.Messages))
	response.Close()
	require.NoError(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 1, len(response.Messages))
	require.EqualValues(t, uint(10), response.Messages[0].Attributes.Sequence)

}
func TestQueueClient_PollClient_ManualAck_ContextCancelled_AckAll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(request.MaxItems), request.Channel, 100, 0, 0, 1, "")
	require.NoError(t, err)
	newCtx, newCancel := context.WithCancel(ctx)
	response, err := sender.c.Poll(newCtx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	newCancel()
	response.Close()
	require.NoError(t, err)
	err = response.AckAll()
	require.Error(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(response.Messages))

}
func TestQueueClient_PollClient_ManualAck_ContextCancelled_AckRange(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(request.MaxItems), request.Channel, 100, 0, 0, 1, "")
	require.NoError(t, err)
	newCtx, newCancel := context.WithCancel(ctx)
	response, err := sender.c.Poll(newCtx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	newCancel()
	response.Close()
	require.NoError(t, err)
	err = response.AckRange([]int64{1})
	require.Error(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(response.Messages))

}
func TestQueueClient_PollClient_ManualAck_NoMessages_AckAll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}

	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))

	err = response.AckAll()
	require.Error(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))
}
func TestQueueClient_PollClient_AutoAck_Expired(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 2000,
			AutoAck:     true,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(request.MaxItems), request.Channel, 100, 2, 0, 1, "")
	require.NoError(t, err)
	time.Sleep(3 * time.Second)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))

}
func TestQueueClient_PollClient_AutoAck_NoMaxItems(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	totalMessages := 150000
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    0,
			WaitTimeout: 100,
			AutoAck:     true,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, totalMessages, request.Channel, 100, 0, 0, 1, "")
	require.NoError(t, err)
	time.Sleep(time.Second)
	receivedCount := 0
	for {

		response, err := sender.c.Poll(ctx, request)
		require.NoError(t, err)
		receivedCount += len(response.Messages)
		if receivedCount >= totalMessages {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.EqualValues(t, totalMessages, receivedCount)
}
func TestQueueClient_PollClient_ManualAck_NoMaxItems(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	totalMessages := 150000
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    0,
			WaitTimeout: 100,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, totalMessages, request.Channel, 100, 0, 0, 1, "")
	require.NoError(t, err)
	time.Sleep(time.Second)
	receivedCount := 0
	var responses []*QueueDownstreamResponse
	for {
		require.NoError(t, err)
		response, err := sender.c.Poll(ctx, request)
		require.NoError(t, err)
		responses = append(responses, response)
		receivedCount += len(response.Messages)
		if receivedCount >= totalMessages {
			break
		}
	}
	require.EqualValues(t, totalMessages, receivedCount)
	for _, response := range responses {
		require.NoError(t, response.AckAll())
	}
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))
}
func TestQueueClient_PollClient_ManualAck_AckAll_Error(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	totalMessages := 1000
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    1000,
			WaitTimeout: 1000,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, totalMessages, request.Channel, 100, 0, 0, 1, "")
	require.NoError(t, err)
	time.Sleep(time.Second)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, totalMessages, len(response.Messages))
	_ = response.subscription.Close()
	require.Error(t, response.AckAll())

}
func TestQueueClient_PollClient_ManualAck_AckRange_Error(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	totalMessages := 1000
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    1000,
			WaitTimeout: 1000,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, totalMessages, request.Channel, 100, 0, 0, 1, "")
	require.NoError(t, err)
	time.Sleep(time.Second)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, totalMessages, len(response.Messages))
	_ = response.subscription.Close()
	require.Error(t, response.AckRange([]int64{1}))

}
func TestQueueClient_PollClient_Request_NoClientId_Error(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)

	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			RequestID:        "",
			ClientID:         "",
			RequestTypeData:  0,
			Channel:          nuid.New().Next(),
			MaxItems:         1000,
			WaitTimeout:      1000,
			AutoAck:          false,
			SequenceRange:    nil,
			RefTransactionId: "",
		},
	}
	_, err = sender.c.Poll(ctx, request)
	require.Error(t, err)

}
func TestQueueClient_PollClient_Request_NoChannel_Error(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)

	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			RequestID:        "",
			ClientID:         "someid",
			RequestTypeData:  0,
			Channel:          "",
			MaxItems:         1000,
			WaitTimeout:      1000,
			AutoAck:          false,
			SequenceRange:    nil,
			RefTransactionId: "",
		},
	}
	_, err = sender.c.Poll(ctx, request)
	require.Error(t, err)

}
func TestQueueClient_PollClient_Request_BadChannel_Error(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)

	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			RequestID:        "",
			ClientID:         "someid",
			RequestTypeData:  0,
			Channel:          ">",
			MaxItems:         1000,
			WaitTimeout:      1000,
			AutoAck:          false,
			SequenceRange:    nil,
			RefTransactionId: "",
		},
	}
	_, err = sender.c.Poll(ctx, request)
	require.Error(t, err)

}
func TestQueueClient_PollClient_ManualAck_NAckAll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(int(request.MaxItems)), request.Channel, 100, 0, 0, 2, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	err = response.NackAll()
	require.NoError(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	err = response.AckAll()
	require.NoError(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))
}
func TestQueueClient_PollClient_ManualAck_NAckAll_Max_Receive2(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    1,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(int(request.MaxItems)), request.Channel, 100, 0, 0, 2, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	err = response.NackAll()
	require.NoError(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	err = response.NackAll()
	require.NoError(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))
}
func TestQueueClient_PollClient_ManualAck_NAckRange_Max_Receive2(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			RequestID:        "",
			ClientID:         uuid.New(),
			RequestTypeData:  0,
			Channel:          nuid.New().Next(),
			MaxItems:         1,
			WaitTimeout:      2000,
			AutoAck:          false,
			ReQueueChannel:   "",
			SequenceRange:    nil,
			RefTransactionId: "",
			Metadata:         nil,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(int(request.MaxItems)), request.Channel, 100, 0, 0, 2, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	err = response.NackRange([]int64{1})
	require.NoError(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	err = response.NackRange([]int64{2})
	require.NoError(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))
}
func TestQueueClient_PollClient_ManualAck_NAckRange_OneByOne(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			RequestID:        "",
			ClientID:         uuid.New(),
			RequestTypeData:  0,
			Channel:          nuid.New().Next(),
			MaxItems:         10,
			WaitTimeout:      2000,
			AutoAck:          false,
			ReQueueChannel:   "",
			SequenceRange:    nil,
			RefTransactionId: "",
			Metadata:         nil,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(request.MaxItems), request.Channel, 100, 0, 0, 1, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	for _, msg := range response.Messages {
		err := response.NackRange([]int64{int64(msg.Attributes.Sequence)})
		require.NoError(t, err)
	}
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))
}
func TestQueueClient_PollClient_ManualAck_NAckAll_Max_Receive2_DeadLetter(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    1,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(int(request.MaxItems)), request.Channel, 100, 0, 0, 2, "dead-letter")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	err = response.NackAll()
	require.NoError(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	err = response.NackAll()
	require.NoError(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))
	request2 := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     "dead-letter",
			ClientID:    uuid.New(),
			MaxItems:    1,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}
	response2, err := sender.c.Poll(ctx, request2)
	require.NoError(t, err)
	require.EqualValues(t, int(request2.MaxItems), len(response2.Messages))

}
func TestQueueClient_PollClient_ManualAck_ReQueueAll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(int(request.MaxItems)), request.Channel, 100, 0, 0, 0, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	err = response.ReQueueAll("re-route-channel")
	require.NoError(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response.Messages))
	request2 := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     "re-route-channel",
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}
	response2, err := sender.c.Poll(ctx, request2)
	require.NoError(t, err)
	require.EqualValues(t, int(request2.MaxItems), len(response2.Messages))
}
func TestQueueClient_PollClient_ManualAck_ReRouteRange(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    2,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(int(request.MaxItems)), request.Channel, 100, 0, 0, 2, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	err = response.ReQueueRange([]int64{1}, "re-route-channel")
	require.NoError(t, err)
	err = response.NackRange([]int64{2})
	require.NoError(t, err)
	response, err = sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, 1, len(response.Messages))
	request2 := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     "re-route-channel",
			ClientID:    uuid.New(),
			MaxItems:    1,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}
	response2, err := sender.c.Poll(ctx, request2)
	require.NoError(t, err)
	require.EqualValues(t, 1, len(response2.Messages))
}
func TestQueueClient_PollClient_ManualAck_ActiveOffsets(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(request.MaxItems), request.Channel, 100, 0, 0, 2, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	offsets, err := response.ActiveOffsets()
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(offsets))
	require.NoError(t, response.AckRange([]int64{1, 2, 3}))
	offsets, err = response.ActiveOffsets()
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems)-3, len(offsets))
}
func TestQueueClient_PollClient_ManualAck_ActiveOffsets_After_AckAll(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	sender, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	request := &QueueDownstreamRequest{
		TransactionId: "",
		QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
			Channel:     nuid.New().Next(),
			ClientID:    uuid.New(),
			MaxItems:    10,
			WaitTimeout: 2000,
			AutoAck:     false,
		},
	}
	err = sender.sendMessagesWithPolicy(ctx, int(request.MaxItems), request.Channel, 100, 0, 0, 2, "")
	require.NoError(t, err)
	response, err := sender.c.Poll(ctx, request)
	require.NoError(t, err)
	require.EqualValues(t, int(request.MaxItems), len(response.Messages))
	err = response.AckAll()
	require.NoError(t, err)
	_, err = response.ActiveOffsets()
	require.Error(t, err)
}
func TestQueueClient_PollClient_Continue_Smoke_Test(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	for j := 0; j < 100; j++ {
		fmt.Println("start loop:", j)
		client, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
		require.NoError(t, err)
		channel := "smoke_test"
		maxItems := 10000
		iter := 10
		waittime := 1000
		recItems := 0
		err = client.sendMessagesWithPolicy(ctx, maxItems, channel, 100, 0, 0, 0, "")
		require.NoError(t, err)
		for i := 0; i < iter; i++ {
			request := &QueueDownstreamRequest{
				TransactionId: "",
				QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
					Channel:     channel,
					ClientID:    uuid.New(),
					MaxItems:    int32(maxItems / iter),
					WaitTimeout: int32(waittime),
					AutoAck:     false,
				},
			}
			response, err := client.c.Poll(ctx, request)
			require.NoError(t, err)
			if len(response.Messages) > 0 {
				err = response.AckAll()
				require.NoError(t, err)
			}
			recItems += len(response.Messages)
		}
		require.EqualValues(t, maxItems, recItems)

		client.disconnect()
		fmt.Println("end loop:", j)
	}
	time.Sleep(time.Second)
}
func TestQueueClient_PollClient_Continue_Smoke_Test_DelayMessage(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	c2, err := NewQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	c2.SetDelayMessagesProcessor(ctx)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	for j := 0; j < 10; j++ {
		fmt.Println("start loop:", j)
		client, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
		require.NoError(t, err)
		channel := "smoke_test"
		maxItems := 1000
		iter := 10
		waittime := 1000
		recItems := 0
		err = client.sendMessagesWithPolicy(ctx, maxItems, channel, 100, 0, 3, 0, "")
		require.NoError(t, err)
		for i := 0; i < iter+4; i++ {
			request := &QueueDownstreamRequest{
				TransactionId: "",
				QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
					Channel:     channel,
					ClientID:    uuid.New(),
					MaxItems:    int32(maxItems / iter),
					WaitTimeout: int32(waittime),
					AutoAck:     false,
				},
			}
			response, err := client.c.Poll(ctx, request)
			require.NoError(t, err)
			if len(response.Messages) > 0 {
				err = response.AckAll()
				require.NoError(t, err)
			}
			recItems += len(response.Messages)
		}
		require.EqualValues(t, maxItems, recItems)
		client.disconnect()
		fmt.Println("end loop:", j)
	}
	time.Sleep(time.Second)
}
func TestQueueClient_PollClient_Parallel_Send_Receive_ManualAck(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := getConfig(t)
	s1 := setupSingle(ctx, t, cfg)
	defer tearDownSingle(s1, cfg)
	defer cancel()
	time.Sleep(1 * time.Second)
	client, err := newTestQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	channel := "parallel_test"
	recCount := atomic.NewInt32(0)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 25; i++ {
			request := &QueueDownstreamRequest{
				TransactionId: "",
				QueuesDownstreamRequest: &pb.QueuesDownstreamRequest{
					Channel:     channel,
					ClientID:    uuid.New(),
					MaxItems:    100,
					WaitTimeout: 1000,
					AutoAck:     false,
				},
			}
			response, err := client.c.Poll(ctx, request)
			require.NoError(t, err)
			go func(resp *QueueDownstreamResponse) {
				for _, msg := range resp.Messages {
					time.Sleep(100 * time.Millisecond)
					err := resp.AckRange([]int64{int64(msg.Attributes.Sequence)})
					require.NoError(t, err)
					recCount.Inc()
				}
			}(response)
			fmt.Printf("Num of Messages: %d\n", recCount.Load())
		}

	}()

	err = client.sendMessagesWithPolicy(ctx, 1000, channel, 100, 0, 0, 0, "")
	require.NoError(t, err)
	wg.Wait()
	require.EqualValues(t, int32(1000), recCount.Load())
}
