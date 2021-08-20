package metrics

import (
	"context"
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	InitExporter(ctx)

	os.Exit(m.Run())
}

func TestExporter_Report(t *testing.T) {

	event := &pb.Event{
		ClientID: "some-client",
		Channel:  "a",
		Metadata: "1234567890",
		Body:     []byte("1234567890"),
		Store:    false,
	}
	event2 := &pb.Event{
		ClientID: "some-client",
		Channel:  "b",
		Metadata: "1234567890",
		Body:     []byte("1234567890"),
		Store:    false,
	}
	eventStore := &pb.Event{
		ClientID: "some-client",
		Channel:  "a",
		Metadata: "1234567890",
		Body:     []byte("1234567890"),
		Store:    true,
	}
	eventStore2 := &pb.Event{
		ClientID: "some-client3",
		Channel:  "b",
		Metadata: "1234567890",
		Body:     []byte("1234567890"),
		Store:    true,
	}
	eventsReceive := &pb.EventReceive{

		Channel:  "a",
		Metadata: "1234567890",
		Body:     []byte("1234567890"),
	}
	commandRequest := &pb.Request{
		ClientID:        "some-client",
		RequestTypeData: 1,
		Channel:         "a",
		Metadata:        "1234567890",
		Body:            []byte("1234567890"),
	}
	commandRequest2 := &pb.Request{
		ClientID:        "some-client",
		RequestTypeData: 1,
		Channel:         "b",
		Metadata:        "1234567890",
		Body:            []byte("1234567890"),
	}

	queryRequest := &pb.Request{
		ClientID:        "some-client",
		RequestTypeData: 2,
		Channel:         "a",
		Metadata:        "1234567890",
		Body:            []byte("1234567890"),
	}
	queryRequest2 := &pb.Request{
		ClientID:        "some-client",
		RequestTypeData: 2,
		Channel:         "b",
		Metadata:        "1234567890",
		Body:            []byte("1234567890"),
	}
	commandRequestRecive := &pb.Request{
		ClientID:        "some-client",
		RequestTypeData: 1,
		Channel:         "a",
		ReplyChannel:    "b",
		Metadata:        "1234567890",
		Body:            []byte("1234567890"),
	}
	queryRequestReceive := &pb.Request{
		ClientID:        "some-client",
		RequestTypeData: 2,
		Channel:         "a",
		ReplyChannel:    "b",
		Metadata:        "1234567890",
		Body:            []byte("1234567890"),
	}
	response := &pb.Response{
		ClientID: "some-client",
		Metadata: "1234567890",
		Body:     []byte("1234567890"),
	}
	queue := &pb.QueueMessage{
		ClientID: "some-client",
		Channel:  "a",
		Metadata: "1234567890",
		Body:     []byte("1234567890"),
	}
	queueBatch := &pb.QueueMessage{
		ClientID: "some-client",
		Channel:  "b",
		Metadata: "1234567890",
		Body:     []byte("1234567890"),
	}
	queueStream := &pb.QueueMessage{
		ClientID: "some-client",
		Channel:  "c",
		Metadata: "1234567890",
		Body:     []byte("1234567890"),
	}
	e := GetExporter()
	ReportEvent(event, &pb.Result{Sent: true})
	ReportEvent(event, &pb.Result{Sent: true})
	ReportEvent(event2, &pb.Result{Sent: false})

	ReportEvent(eventStore, &pb.Result{Sent: true})
	ReportEvent(eventStore, &pb.Result{Sent: true})
	ReportEvent(eventStore2, &pb.Result{Sent: false})

	ReportEventReceive(eventsReceive, &pb.Subscribe{SubscribeTypeData: 1, ClientID: "client_id"})
	ReportEventReceive(eventsReceive, &pb.Subscribe{SubscribeTypeData: 2, ClientID: "client_id"})

	ReportRequest(commandRequest, &pb.Response{Executed: true}, nil)
	ReportRequest(commandRequest, nil, fmt.Errorf("some-error"))
	ReportRequest(commandRequest2, &pb.Response{Executed: false}, nil)

	ReportRequest(queryRequest, &pb.Response{Executed: true}, nil)
	ReportRequest(queryRequest, nil, fmt.Errorf("some-error"))
	ReportRequest(queryRequest2, &pb.Response{Executed: false}, nil)

	ReportRequest(commandRequestRecive, nil, nil)
	ReportRequest(commandRequestRecive, nil, fmt.Errorf("some-error"))
	ReportRequest(queryRequestReceive, nil, nil)
	ReportRequest(queryRequestReceive, nil, fmt.Errorf("some-error"))

	ReportResponse(response, nil)
	ReportResponse(response, fmt.Errorf("some-error"))

	ReportSendQueueMessage(queue, &pb.SendQueueMessageResult{IsError: false})
	ReportSendQueueMessage(queue, &pb.SendQueueMessageResult{IsError: true})

	ReportSendQueueMessageBatch(&pb.QueueMessagesBatchRequest{
		BatchID:              "",
		Messages:             []*pb.QueueMessage{queueBatch, queueBatch},
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}, &pb.QueueMessagesBatchResponse{
		BatchID:              "",
		Results:              []*pb.SendQueueMessageResult{&pb.SendQueueMessageResult{IsError: false}, &pb.SendQueueMessageResult{IsError: true}},
		HaveErrors:           false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})

	ReportReceiveQueueMessages(&pb.ReceiveQueueMessagesRequest{
		ClientID: "some-client-id",
	}, &pb.ReceiveQueueMessagesResponse{
		Messages: []*pb.QueueMessage{queue},
		IsError:  false,
	})
	ReportReceiveQueueMessages(&pb.ReceiveQueueMessagesRequest{
		ClientID: "some-client-id",
	}, &pb.ReceiveQueueMessagesResponse{
		Messages: []*pb.QueueMessage{queue},
		IsError:  true,
	})

	ReportReceiveQueueMessages(&pb.ReceiveQueueMessagesRequest{
		ClientID: "some-client-id",
		IsPeak:   true,
	}, &pb.ReceiveQueueMessagesResponse{
		Messages: []*pb.QueueMessage{queue},
		IsError:  false,
	})
	ReportReceiveStreamQueueMessage(queueStream)

	ReportClient("events", "send", "ch", 1)
	ReportPending("events_store", "some-client-id", "ch", 1)

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, uint64(0), e.MetricsDropped())
	s, err := e.Stats()
	require.NoError(t, err)
	require.NotNil(t, s)
	require.Equal(t, 41, len(s))
	cs, err := e.Channels()
	require.NoError(t, err)
	require.NotEmpty(t, cs)
	var totalVol, totalMsg, totalErr float64
	var totalCh int
	chSum, err := e.ChannelSummery()
	require.NoError(t, err)
	for _, sum := range chSum {

		totalCh += sum.TotalChannels
		totalVol += sum.TotalVolume
		totalMsg += sum.TotalMessages
		totalErr += sum.TotalErrors
	}
	require.Equal(t, 14, totalCh)
	require.Equal(t, float64(24), totalMsg)
	require.Equal(t, float64(8), totalErr)
	require.Equal(t, float64(814), totalVol)
	cli, _, err := e.Clients()
	require.NoError(t, err)
	require.Equal(t, 4, len(cli))

}

func TestExporter_ReportErrors(t *testing.T) {

	eventsReceive := &pb.EventReceive{
		Channel:  "a",
		Metadata: "1234567890",
		Body:     []byte("1234567890"),
	}
	queue := &pb.QueueMessage{
		Channel:  "a",
		Metadata: "1234567890",
		Body:     []byte("1234567890"),
	}
	e := GetExporter()
	ReportEvent(nil, nil)
	ReportEventReceive(eventsReceive, nil)
	ReportEventReceive(nil, &pb.Subscribe{SubscribeTypeData: 1, ClientID: "client_id"})
	ReportRequest(nil, nil, nil)
	ReportResponse(nil, nil)
	ReportSendQueueMessage(queue, nil)
	ReportSendQueueMessage(nil, &pb.SendQueueMessageResult{IsError: false})

	ReportSendQueueMessageBatch(&pb.QueueMessagesBatchRequest{
		BatchID:              "",
		Messages:             []*pb.QueueMessage{},
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}, nil)
	ReportSendQueueMessageBatch(nil, &pb.QueueMessagesBatchResponse{
		BatchID:              "",
		Results:              []*pb.SendQueueMessageResult{&pb.SendQueueMessageResult{IsError: false}, &pb.SendQueueMessageResult{IsError: true}},
		HaveErrors:           false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	})
	//
	ReportReceiveQueueMessages(&pb.ReceiveQueueMessagesRequest{
		ClientID: "some-client-id",
	}, nil)
	ReportReceiveQueueMessages(nil, &pb.ReceiveQueueMessagesResponse{
		Messages: []*pb.QueueMessage{queue},
		IsError:  true,
	})
	//
	ReportReceiveStreamQueueMessage(nil)
	ReportClient("", "send", "ch", 1)
	ReportPending("", "some-client-id", "ch", 1)
	require.Equal(t, uint64(14), e.MetricsDropped())

}
