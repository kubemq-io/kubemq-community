package monitor

import (
	"context"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/nats-io/nuid"

	"github.com/kubemq-io/kubemq-community/pkg/logging"

	"fmt"

	"encoding/json"

	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/client"
)

const (
	responseChannelPrefix = "kubemq_reports"
)

func NewEventsMonitor(ctx context.Context, dataChan chan *Transport,
	monitorRequest *MonitorRequest, errChan chan error) {
	appConfig := config.GetAppConfig()
	ncOpts := client.NewClientOptions("monitor-client" + nuid.Next()).
		SetMemoryPipe(appConfig.Broker.MemoryPipe)

	logger := logging.GetLogFactory().NewLogger("event_monitor")
	var nc *client.Client
	var err error
	switch  monitorRequest.Kind {
	case entities.KindTypeEventStore:
		nc, err = client.NewStoreClient(ncOpts)
	case entities.KindTypeEvent:
		nc, err = client.NewClient(ncOpts)
	default:
		errChan <- fmt.Errorf("invalid request kind")
		return
	}

	if err != nil {
		errChan <- fmt.Errorf("create server client error: %s", err.Error())
		return
	}
	logger.Debugw("monitor for messages started", "channel", monitorRequest.Channel, "kind", entities.KindNames[monitorRequest.Kind], "client_id", ncOpts.ClientID)
	defer func() {
		logger.Debugw("monitor for messages ended", "channel", monitorRequest.Channel, "kind", entities.KindNames[monitorRequest.Kind], "client_id", ncOpts.ClientID)
		_ = nc.Disconnect()

	}()

	subReq := &pb.Subscribe{
		SubscribeTypeData:    0,
		ClientID:             "monitor-request-" + nuid.Next(),
		Channel:              monitorRequest.Channel,
		Group:                "",
		EventsStoreTypeData:  0,
		EventsStoreTypeValue: 0,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}

	msgChan := make(chan *pb.EventReceive, 100)
	if monitorRequest.Kind == entities.KindTypeEvent {
		subReq.SubscribeTypeData = pb.Subscribe_Events
		err = nc.SubscribeToEvents(ctx, subReq, msgChan)
	} else {
		subReq.SubscribeTypeData = pb.Subscribe_EventsStore
		subReq.EventsStoreTypeData = pb.Subscribe_StartNewOnly
		err = nc.SubscribeToEventsStore(ctx, subReq, msgChan)
	}

	if err != nil {
		errChan <- fmt.Errorf("subscribe to messages error: %s", err.Error())
		return
	}
	for {
		select {
		case msg := <-msgChan:
			dataChan <- NewTransportFromMessageReceived(msg).SetPayload(msg)
		case <-ctx.Done():
			return
		}
	}
}

func NewRequestResponseMonitor(ctx context.Context, dataChan chan *Transport, monitorRequest *MonitorRequest, errChan chan error) {
	appConfig := config.GetAppConfig()
	ncOpts := client.NewClientOptions("monitor-client" + nuid.Next()).
		SetMemoryPipe(appConfig.Broker.MemoryPipe)

	logger := logging.GetLogFactory().NewLogger("request_monitor")
	nc, err := client.NewClient(ncOpts)
	if err != nil {
		errChan <- fmt.Errorf("create server client error: %s", err.Error())
		return
	}
	logger.Debugw("monitor for requests started", "channel", monitorRequest.Channel, "client_id", ncOpts.ClientID)
	defer func() {
		logger.Debugw("monitor for requests ended", "channel", monitorRequest.Channel, "client_id", ncOpts.ClientID)

		unregisterChannel(monitorRequest.Channel, nc)
		_ = nc.Disconnect()
	}()
	reqChan := make(chan *pb.Request, 100)
	resChan := make(chan *pb.EventReceive, 100)

	subReqForRequests := &pb.Subscribe{
		SubscribeTypeData:    0,
		ClientID:             "request_response_monitor_client",
		Channel:              monitorRequest.Channel,
		Group:                "",
		EventsStoreTypeData:  0,
		EventsStoreTypeValue: 0,
	}
	if monitorRequest.Kind == entities.KindTypeCommand {
		subReqForRequests.SubscribeTypeData = pb.Subscribe_Commands
		err = nc.SubscribeToCommands(ctx, subReqForRequests, reqChan)
	} else {
		subReqForRequests.SubscribeTypeData = pb.Subscribe_Queries
		err = nc.SubscribeToQueries(ctx, subReqForRequests, reqChan)
	}

	if err != nil {
		errChan <- fmt.Errorf("subscribe to request error: %s", err.Error())
		return
	}
	subReqForMessages := &pb.Subscribe{
		SubscribeTypeData:    pb.Subscribe_Events,
		ClientID:             "request_response_monitor_client",
		Channel:              fmt.Sprintf("%s.%s", responseChannelPrefix, monitorRequest.Channel),
		Group:                "",
		EventsStoreTypeData:  0,
		EventsStoreTypeValue: 0,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	err = nc.SubscribeToEvents(ctx, subReqForMessages, resChan)
	if err != nil {
		errChan <- fmt.Errorf("subscribe to response error: %s", err.Error())
		return
	}

	registerChannel(monitorRequest.Channel, nc)
	for {
		select {
		case req := <-reqChan:
			dataChan <- NewTransportFromRequest(req).SetPayload(req)
		case msg := <-resChan:
			switch msg.Metadata {
			case "response_error":
				var t *Transport
				resErr := &ResponseError{}
				err := json.Unmarshal(msg.Body, resErr)
				if err != nil {
					t = NewTransportFromMessageReceived(msg).SetPayload(msg)
					t.Error = fmt.Errorf("response json unmarshal error: %s", err.Error())
				} else {
					t = NewTransportFromResponseError(resErr).SetPayload(resErr)
					t.Channel = monitorRequest.Channel
				}
				dataChan <- t
			case "response":
				var t *Transport
				res := &pb.Response{}
				err := res.Unmarshal(msg.Body)
				if err != nil {
					t = NewTransportFromMessageReceived(msg).SetPayload(msg)
					t.Error = fmt.Errorf("response json unmarshal error: %s", err.Error())
				} else {
					t = NewTransportFromResponse(res).SetPayload(res)
					t.Channel = monitorRequest.Channel
				}
				dataChan <- t
			}
		case <-ctx.Done():
			return
		}
	}
}

func NewQueueMessagesMonitor(ctx context.Context, dataChan chan *Transport, monitorRequest *MonitorRequest, errChan chan error) {
	appConfig := config.GetAppConfig()
	ncOpts := client.NewClientOptions("monitor-client" + nuid.Next()).
		SetMemoryPipe(appConfig.Broker.MemoryPipe)
	logger := logging.GetLogFactory().NewLogger("request_monitor")
	qc, err := client.NewQueueClient(ncOpts, &config.QueueConfig{
		MaxNumberOfMessages:       0,
		MaxWaitTimeoutSeconds:     0,
		MaxExpirationSeconds:      0,
		MaxDelaySeconds:           0,
		MaxReceiveCount:           0,
		MaxVisibilitySeconds:      0,
		DefaultVisibilitySeconds:  0,
		DefaultWaitTimeoutSeconds: 0,
	})
	if err != nil {
		errChan <- fmt.Errorf("create queue client error: %s", err.Error())
		return
	}
	logger.Debugw("monitor for queue messages started", "channel", monitorRequest.Channel, "client_id", ncOpts.ClientID)
	defer func() {
		logger.Debugw("monitor for requests ended", "channel", monitorRequest.Channel, "client_id", ncOpts.ClientID)
		_ = qc.Disconnect()
	}()
	msgCh := make(chan *pb.QueueMessage, 100)
	doneCh := make(chan bool, 2)
	errCh := make(chan error, 2)

	go qc.MonitorQueueMessages(ctx, monitorRequest.Channel, msgCh, errCh, doneCh)
	if err != nil {
		errChan <- fmt.Errorf("subscribe to queue messages error: %s", err.Error())
		return
	}

	for {
		select {
		case msg := <-msgCh:
			select {
			case dataChan <- NewTransportFromQueueMessage(msg).SetPayload(msg):

			case <-ctx.Done():

			}
		case err := <-errCh:
			errCh <- err
		case <-ctx.Done():
			return
		}
	}
}
