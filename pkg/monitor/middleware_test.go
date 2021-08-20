package monitor

import (
	"context"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	pb "github.com/kubemq-io/protobuf/go"
	"testing"

	"github.com/fortytw2/leaktest"

	"github.com/stretchr/testify/require"
)

func TestNewQueryMonitorMiddleware_NoResponse(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, appConfig, ns := setup(ctx, t)
	defer tearDown(c, appConfig, ns)
	channel := "TestMonitorMiddleware.1"
	done := make(chan struct{}, 10)
	request := &pb.Request{
		RequestID:            "some_id",
		RequestTypeData:      pb.Request_Query,
		ClientID:             "some-client-id",
		Channel:              channel,
		Metadata:             "some-metadata",
		Body:                 []byte("some-body"),
		ReplyChannel:         "",
		Timeout:              100,
		CacheKey:             "",
		CacheTTL:             0,
		Span:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	mr := &MonitorRequest{
		Kind:        entities.KindTypeQuery,
		Channel:     channel,
		MaxBodySize: 0,
	}
	errChan := make(chan error, 10)
	rxChan := make(chan *Transport, 10)
	md, err := NewMonitorMiddleware(ctx, appConfig)
	defer md.Shutdown()
	require.NoError(t, err)
	go NewRequestResponseMonitor(ctx, rxChan, mr, errChan)
	sleep(1)
	go func() {
		_, _ = c.SendQuery(ctx, request)
		md.CheckAndSendQuery(ctx, request, nil, entities.ErrRequestTimeout)
		done <- struct{}{}
	}()
	msg := <-rxChan
	require.True(t, msg.Kind == "query")
	msg = <-rxChan
	require.True(t, msg.Kind == "response_error")
	<-done
}
func TestNewQueryMonitorMiddleware_WithResponse(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, appConfig, ns := setup(ctx, t)
	defer tearDown(c, appConfig, ns)
	channel := "TestMonitorMiddleware.2"
	request := &pb.Request{
		RequestID:            "some_id",
		RequestTypeData:      pb.Request_Query,
		ClientID:             "some-client-id",
		Channel:              channel,
		Metadata:             "some-metadata",
		Body:                 []byte("some-body"),
		ReplyChannel:         "",
		Timeout:              100,
		CacheKey:             "",
		CacheTTL:             0,
		Span:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	mr := &MonitorRequest{
		Kind:        entities.KindTypeQuery,
		Channel:     channel,
		MaxBodySize: 0,
	}
	done := make(chan struct{}, 10)
	errChan := make(chan error, 10)
	rxChan := make(chan *Transport, 10)
	md, err := NewMonitorMiddleware(ctx, appConfig)
	defer md.Shutdown()
	require.NoError(t, err)
	go NewRequestResponseMonitor(ctx, rxChan, mr, errChan)
	sleep(1)
	go func() {
		_, _ = c.SendQuery(ctx, request)
		md.CheckAndSendQuery(ctx, request, &pb.Response{RequestID: request.RequestID}, nil)
		done <- struct{}{}
	}()
	msg := <-rxChan
	require.True(t, msg.Kind == "query")
	msg = <-rxChan
	require.True(t, msg.Kind == "response")
	<-done

}

func TestNewQueryMonitorMiddleware_WithResponseForTwoMonitors(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, appConfig, ns := setup(ctx, t)
	defer tearDown(c, appConfig, ns)
	channel := "TestMonitorMiddleware.3"
	request := &pb.Request{
		RequestID:            "some_id",
		RequestTypeData:      pb.Request_Query,
		ClientID:             "test-client-id",
		Channel:              channel,
		Metadata:             "some-metadata",
		Body:                 []byte("some-body"),
		ReplyChannel:         "",
		Timeout:              100,
		CacheKey:             "",
		CacheTTL:             0,
		Span:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	mr := &MonitorRequest{
		Kind:        entities.KindTypeQuery,
		Channel:     channel,
		MaxBodySize: 0,
	}
	errChan := make(chan error, 10)
	done := make(chan struct{}, 10)
	md, err := NewMonitorMiddleware(ctx, appConfig)
	defer md.Shutdown()
	require.NoError(t, err)
	// first monitor
	go func() {
		rxChan := make(chan *Transport, 10)
		go NewRequestResponseMonitor(ctx, rxChan, mr, errChan)
		msg := <-rxChan
		require.True(t, msg.Kind == "query")
		msg = <-rxChan
		require.True(t, msg.Kind == "response")

	}()
	//second monitor
	go func() {
		rxChan := make(chan *Transport, 10)
		go NewRequestResponseMonitor(ctx, rxChan, mr, errChan)
		msg := <-rxChan
		require.True(t, msg.Kind == "query")
		msg = <-rxChan
		require.True(t, msg.Kind == "response")
		msg = <-rxChan
		require.True(t, msg.Kind == "response")
		done <- struct{}{}
	}()
	sleep(1)
	go func() {
		_, _ = c.SendQuery(ctx, request)
		md.CheckAndSendQuery(ctx, request, &pb.Response{RequestID: request.RequestID}, nil)
		md.CheckAndSendQuery(ctx, request, &pb.Response{RequestID: request.RequestID}, nil)
	}()
	<-done
}
