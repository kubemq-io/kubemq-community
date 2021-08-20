package array

import (
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"math/rand"

	"github.com/kubemq-io/kubemq-community/services/authorization"
	"github.com/kubemq-io/kubemq-community/services/routing"
	"github.com/nats-io/nuid"
	"sync"

	"go.uber.org/atomic"

	"time"

	"context"

	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/client"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"github.com/kubemq-io/kubemq-community/pkg/monitor"
	pb "github.com/kubemq-io/protobuf/go"
)

const queueDownstreamClientPoolSize = 200
const queueUpstreamClientPoolSize = 200

type Array struct {
	eventSender       EventsSender
	eventsStoreSender EventsStoreSender
	QuerySender
	CommandSender
	ResponseSender
	Stopped    chan struct{}
	context    context.Context
	cancelFunc context.CancelFunc
	appConfig  *config.Config
	clients    sync.Map

	logger            *logging.Logger
	sharedClient      *client.Client
	sharedStoreClient *client.Client
	sharedQueueClient *client.QueueClient
	monitorMiddleware *monitor.Middleware
	isShutdown                 *atomic.Bool
	clientsCount               *atomic.Int64
	authorizationService       *authorization.Service
	router                     *routing.Service
	queueClientsPool           *client.QueuePool
	queueDownstreamClientsPool *client.QueuePool
	queueDownstreamCounter     *atomic.Uint64
	queueUpstreamClientsPool *client.QueuePool
	queueUpstreamCounter     *atomic.Uint64
}

func Start(ctx context.Context, appConfig *config.Config) (*Array, error) {
	rand.Seed(time.Now().UnixNano())
	na := &Array{

		Stopped: make(chan struct{}, 1),

		appConfig: appConfig,

		logger: logging.GetLogFactory().NewLogger("array"),

		isShutdown:           atomic.NewBool(false),
		clientsCount:         atomic.NewInt64(0),
		authorizationService: authorization.GetSingleton(),
		router:               routing.GetSingleton(),
		queueClientsPool: client.NewQueuePool(ctx, &client.QueuePoolOptions{
			KillAfter: 1 * time.Minute,
			MaxUsage:  10,
		}, appConfig),
		queueDownstreamCounter: atomic.NewUint64(0),
		queueDownstreamClientsPool: client.NewQueuePool(ctx, &client.QueuePoolOptions{
			KillAfter: 1 * time.Minute,
			MaxUsage:  10,
		}, appConfig),
		queueUpstreamCounter: atomic.NewUint64(0),
		queueUpstreamClientsPool: client.NewQueuePool(ctx, &client.QueuePoolOptions{
			KillAfter: 1 * time.Minute,
			MaxUsage:  10,
		}, appConfig),
	}

	na.context, na.cancelFunc = context.WithCancel(ctx)
	var err error
	ncOpts := client.NewClientOptions("shared-client").
		SetMemoryPipe(appConfig.Broker.MemoryPipe).
		SetMaxInflight(int(appConfig.Queue.MaxInflight)).
		SetPubAckWaitSeconds(int(appConfig.Queue.PubAckWaitSeconds))

	na.sharedClient, err = client.NewClient(ncOpts)
	if err != nil {
		return nil, err
	}
	nqcOpts := client.NewClientOptions(fmt.Sprintf("%s_at_%s", "shared-stored-client-", na.appConfig.Host)).
		SetMemoryPipe(appConfig.Broker.MemoryPipe).
		SetMaxInflight(int(appConfig.Queue.MaxInflight)).
		SetPubAckWaitSeconds(int(appConfig.Queue.PubAckWaitSeconds))

	retryCounter := 0
	for {
		na.logger.Debugf("try to connect to stream server for shared store client, attempt: %d", retryCounter)
		if retryCounter > 20 {
			return nil, entities.ErrPersistenceServerNotReady
		}
		na.sharedStoreClient, err = client.NewStoreClient(nqcOpts)
		if err == nil {
			na.logger.Debugf("connection to stream server for shared store client succeed")
			break
		}
		na.logger.Errorf("connection to stream server for shared store client failed, error: %s", err.Error())
		time.Sleep(time.Second)
		retryCounter++

	}

	nqueueOpts := client.NewClientOptions(fmt.Sprintf("%s_at_%s", "shared-queue-client-", na.appConfig.Host)).
		SetMemoryPipe(appConfig.Broker.MemoryPipe).
		SetMaxInflight(int(appConfig.Queue.MaxInflight)).
		SetPubAckWaitSeconds(int(appConfig.Queue.PubAckWaitSeconds))
	retryCounter = 0
	for {
		na.logger.Debugf("try to connect to stream server for shared queue client, attempt: %d", retryCounter)
		if retryCounter > 20 {
			return nil, entities.ErrPersistenceServerNotReady
		}
		na.sharedQueueClient, err = client.NewQueueClient(nqueueOpts, appConfig.Queue)
		if err == nil {
			na.logger.Debugf("connection to stream server for shared queue client succeed")
			break
		}
		na.logger.Errorf("connection to stream server for shared store shared failed, error: %s", err.Error())
		time.Sleep(time.Second)
		retryCounter++
	}
	na.monitorMiddleware, err = monitor.NewMonitorMiddleware(na.context, appConfig)
	if err != nil {
		return nil, err
	}
	// set the shared queue client is the processor of the delayed messages
	na.sharedQueueClient.SetDelayMessagesProcessor(na.context)
	na.eventSender = ChainEventsSenders(na.sharedClient, EventsSenderLogging(na.logger))
	na.eventsStoreSender = ChainEventsStoreSenders(na.sharedStoreClient, EventsStoreSenderLogging(na.logger))
	na.QuerySender = ChainQuerySender(na.sharedClient,  QuerySenderMonitor(na.monitorMiddleware), QuerySenderLogging(na.logger))
	na.CommandSender = ChainCommandSender(na.sharedClient, CommandSenderMonitor(na.monitorMiddleware), CommandSenderLogging(na.logger))
	na.ResponseSender = ChainResponseSender(na.sharedClient, ResponseSenderLogging(na.logger))
	//go na.watch()
	return na, nil
}

func (a *Array) Close() {
	a.logger.Warnw("array shutdown in progress")
	a.isShutdown.Store(true)
	_ = a.sharedClient.Disconnect()
	_ = a.sharedStoreClient.Disconnect()
	a.sharedQueueClient.ShutdownDelayedMessagesProcessor()
	_ = a.sharedQueueClient.Disconnect()
	a.queueClientsPool.Close()
	a.queueDownstreamClientsPool.Close()
	a.queueUpstreamClientsPool.Close()
	var clientsIDs []string
	a.clients.Range(func(key, value interface{}) bool {
		clientsIDs = append(clientsIDs, key.(string))
		return true
	})
	for i := 0; i < len(clientsIDs); i++ {
		go func(index int) {
			err := a.DeleteClient(clientsIDs[index])
			if err != nil {
				a.logger.Errorf("error closing connection: %d error: %s", index, err.Error())
			}
		}(i)
	}
	time.Sleep(1 * time.Second)
	for i := 10; i > 0; i-- {
		if a.ClientsCount() > 0 {
			a.logger.Warnf("closing connections, %d left, waiting for %d more seconds...", a.ClientsCount(), i)
			time.Sleep(time.Second)
		}
	}
	a.logger.Debugw("monitor middleware shutdown in progress")
	a.monitorMiddleware.Shutdown()
	<-a.monitorMiddleware.Stopped
	a.logger.Debugw("monitor middleware shutdown completed")

	a.logger.Warnw("array shutdown is completed")
	a.cancelFunc()
	a.Stopped <- struct{}{}
}

func (a *Array) Monitor() *monitor.Middleware {
	return a.monitorMiddleware
}

func (a *Array) NewRawNatsClient(ctx context.Context, clientID string) (*client.Client, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	ncOpts := client.NewClientOptions(clientID).
		SetMemoryPipe(a.appConfig.Broker.MemoryPipe)
	newClient, err := client.NewClient(ncOpts)
	if err != nil {
		return nil, err
	}

	return newClient, nil
}

func (a *Array) NewClient(ctx context.Context, id string, clientId string) (*client.Client, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}

	if id == "" {
		return nil, entities.ErrInvalidClientID
	}
	ncOpts := client.NewClientOptions(clientId).
		SetMemoryPipe(a.appConfig.Broker.MemoryPipe)
	newClient, err := client.NewClient(ncOpts)
	if err != nil {
		return nil, err
	}

	a.clientsCount.Inc()
	currentValue, ok := a.clients.LoadOrStore(id, newClient)
	if ok {
		currentClient := currentValue.(*client.Client)
		_ = newClient.Disconnect()
		return currentClient, nil
	}

	return newClient, nil
}

func (a *Array) NewStoreClient(ctx context.Context, clientID string) (*client.Client, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}

	if clientID == "" {
		return nil, entities.ErrInvalidClientID
	}

	ncOpts := client.NewClientOptions(clientID).
		SetMemoryPipe(a.appConfig.Broker.MemoryPipe)
	newClient, err := client.NewStoreClient(ncOpts)
	if err != nil {
		return nil, err
	}

	a.clientsCount.Inc()
	currentValue, ok := a.clients.LoadOrStore(clientID, newClient)
	if ok {
		currentClient := currentValue.(*client.Client)
		_ = newClient.Disconnect()
		return currentClient, nil
	}
	return newClient, nil
}
func (a *Array) NewQueueClientFromPool(channel string) (*client.QueueClient, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}

	if channel == "" {
		return nil, entities.ErrInvalidClientID
	}

	c, err := a.queueClientsPool.GetClient(channel)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (a *Array) NewQueueClient(ctx context.Context, clientID string) (*client.QueueClient, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}

	if clientID == "" {
		return nil, entities.ErrInvalidClientID
	}

	ncOpts := client.NewClientOptions(clientID).
		SetMemoryPipe(a.appConfig.Broker.MemoryPipe).
		SetMaxInflight(int(a.appConfig.Queue.MaxInflight)).
		SetPubAckWaitSeconds(int(a.appConfig.Queue.PubAckWaitSeconds))

	newClient, err := client.NewQueueClient(ncOpts, a.appConfig.Queue)
	if err != nil {
		return nil, err
	}

	a.clientsCount.Inc()
	currentValue, ok := a.clients.LoadOrStore(clientID, newClient)
	if ok {
		currentClient := currentValue.(*client.QueueClient)
		_ = newClient.Disconnect()
		return currentClient, nil
	}
	return newClient, nil
}
func (a *Array) NewQueueDownstreamClientFromPool() (*client.QueueClient, string, error) {
	if a.isShutdown.Load() {
		return nil, "", entities.ErrShutdownMode
	}
	id := a.queueDownstreamCounter.Inc() % queueDownstreamClientPoolSize
	channel := fmt.Sprintf("downstream_client_%d", id)
	c, err := a.queueDownstreamClientsPool.GetClient(channel)
	if err != nil {
		return nil, "", err
	}
	return c, channel, nil
}
func (a *Array) ReleaseQueueDownstreamClientFromPool(id string) {
	a.queueDownstreamClientsPool.ReleaseClient(id)
}



func (a *Array) NewQueueUpstreamClientFromPool() (*client.QueueClient, string, error) {
	if a.isShutdown.Load() {
		return nil, "", entities.ErrShutdownMode
	}
	id := a.queueUpstreamCounter.Inc() % queueUpstreamClientPoolSize
	channel := fmt.Sprintf("upstream_client_%d", id)
	c, err := a.queueUpstreamClientsPool.GetClient(channel)
	if err != nil {
		return nil, "", err
	}
	return c, channel, nil
}
func (a *Array) ReleaseQueueUpstreamClientFromPool(id string) {
	a.queueUpstreamClientsPool.ReleaseClient(id)
}

func (a *Array) GetClientsList() (list []string) {
	a.clients.Range(func(key, value interface{}) bool {
		list = append(list, key.(string))
		return true
	})
	return
}
func (a *Array) DeleteClient(id string) error {
	currentValue, ok := a.clients.Load(id)
	if ok {
		switch v := currentValue.(type) {
		case *client.Client:

			err := v.Disconnect()
			if err != nil {
				return err
			}
		case *client.QueueClient:
			err := v.Disconnect()
			if err != nil {
				return err
			}
		}

		a.clients.Delete(id)
		a.clientsCount.Dec()
		return nil
	}
	//a.hubPool.Delete(id)
	a.clientsCount.Dec()
	return nil

}

func (a *Array) ClientsCount() int {

	return int(a.clientsCount.Load())
}

func (a *Array) SendEvents(ctx context.Context, msg *pb.Event) (*pb.Result, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	if IsRoutable(msg.Channel) {
		return a.processEventsRoutingMessages(ctx, msg)
	} else {
		return a.sendEvents(ctx, msg)
	}
}

func (a *Array) processEventsRoutingMessages(ctx context.Context, msg *pb.Event) (*pb.Result, error) {
	routingMessages, err := a.router.CreateRouteMessages(msg.Channel, msg)
	if err != nil {
		return nil, err
	}
	firstMessage := routingMessages.Events[0]
	result, err := a.sendEvents(ctx, firstMessage)
	if err != nil {
		return result, err
	}
	if len(routingMessages.Events) > 1 {
		go func() {
			for i := 1; i < len(routingMessages.Events); i++ {
				event := routingMessages.Events[i]
				_, err := a.sendEvents(ctx, event)
				if err != nil {
					a.logger.Errorf("error sending routed event to events channel %s: %s", event.Channel, err.Error())
				}
			}
		}()

	}

	if len(routingMessages.EventsStore) > 0 {
		go func() {
			for i := 0; i < len(routingMessages.EventsStore); i++ {
				eventStore := routingMessages.EventsStore[i]
				_, err := a.sendEventsStore(ctx, eventStore)
				if err != nil {
					a.logger.Errorf("error sending routed event_store to events_store channel %s: %s", eventStore.Channel, err.Error())
				}
			}
		}()
	}

	if len(routingMessages.QueueMessages) > 0 {
		go func() {
			for i := 0; i < len(routingMessages.QueueMessages); i++ {
				queueMessage := routingMessages.QueueMessages[i]
				_, err := a.sendQueueMessage(ctx, queueMessage)
				if err != nil {
					a.logger.Errorf("error sending routed queue message to queue channel %s: %s", queueMessage.Channel, err.Error())
				}
			}
		}()
	}

	return result, nil
}
func (a *Array) processEventsStoreRoutingMessages(ctx context.Context, msg *pb.Event) (*pb.Result, error) {
	routingMessages, err := a.router.CreateRouteMessages(msg.Channel, msg)
	if err != nil {
		return nil, err
	}
	firstMessage := routingMessages.EventsStore[0]
	result, err := a.sendEventsStore(ctx, firstMessage)
	if err != nil {
		return result, err
	}
	if len(routingMessages.EventsStore) > 1 {
		go func() {
			for i := 1; i < len(routingMessages.EventsStore); i++ {
				event := routingMessages.EventsStore[i]
				_, err := a.sendEventsStore(ctx, event)
				if err != nil {
					a.logger.Errorf("error sending routed event_store to events_store channel %s: %s", event.Channel, err.Error())
				}
			}
		}()

	}

	if len(routingMessages.Events) > 0 {
		go func() {
			for i := 0; i < len(routingMessages.Events); i++ {
				event := routingMessages.Events[i]
				_, err := a.sendEvents(ctx, event)
				if err != nil {
					a.logger.Errorf("error sending routed event to events channel %s: %s", event.Channel, err.Error())
				}
			}
		}()
	}

	if len(routingMessages.QueueMessages) > 0 {
		go func() {
			for i := 0; i < len(routingMessages.QueueMessages); i++ {
				queueMessage := routingMessages.QueueMessages[i]
				_, err := a.sendQueueMessage(ctx, queueMessage)
				if err != nil {
					a.logger.Errorf("error sending routed queue message to queue channel %s: %s", queueMessage.Channel, err.Error())
				}
			}
		}()
	}

	return result, nil
}

func (a *Array) processQueueMessageRoutingMessages(ctx context.Context, msg *pb.QueueMessage) (*pb.SendQueueMessageResult, error) {
	routingMessages, err := a.router.CreateRouteMessages(msg.Channel, msg)
	if err != nil {
		return nil, err
	}
	firstMessage := routingMessages.QueueMessages[0]
	result, err := a.sendQueueMessage(ctx, firstMessage)
	if err != nil {
		return result, err
	}
	if len(routingMessages.QueueMessages) > 1 {
		go func() {
			for i := 1; i < len(routingMessages.QueueMessages); i++ {
				queueMessage := routingMessages.QueueMessages[i]
				_, err := a.sendQueueMessage(ctx, queueMessage)
				if err != nil {
					a.logger.Errorf("error sending routed queue message to queue channel %s: %s", queueMessage.Channel, err.Error())
				}
			}
		}()

	}

	if len(routingMessages.Events) > 0 {
		go func() {
			for i := 0; i < len(routingMessages.Events); i++ {
				event := routingMessages.Events[i]
				_, err := a.sendEvents(ctx, event)
				if err != nil {
					a.logger.Errorf("error sending routed queue message to events channel %s: %s", event.Channel, err.Error())
				}
			}
		}()
	}

	if len(routingMessages.EventsStore) > 0 {
		go func() {
			for i := 0; i < len(routingMessages.EventsStore); i++ {
				eventStore := routingMessages.EventsStore[i]
				_, err := a.sendEventsStore(ctx, eventStore)
				if err != nil {
					a.logger.Errorf("error sending routed queue message to events_store channel %s: %s", eventStore.Channel, err.Error())
				}
			}
		}()
	}

	return result, nil
}
func (a *Array) sendEvents(ctx context.Context, msg *pb.Event) (*pb.Result, error) {

	if err := a.Authorize(msg); err != nil {
		return nil, err
	}
	result, err := a.eventSender.SendEvents(ctx, msg)
	return result, err
}

func (a *Array) SendEventsStore(ctx context.Context, msg *pb.Event) (*pb.Result, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	if IsRoutable(msg.Channel) {
		return a.processEventsStoreRoutingMessages(ctx, msg)
	} else {
		return a.sendEventsStore(ctx, msg)
	}

}
func (a *Array) sendEventsStore(ctx context.Context, msg *pb.Event) (*pb.Result, error) {
	if err := a.Authorize(msg); err != nil {
		return nil, err
	}

	result, err := a.eventsStoreSender.SendEventsStore(ctx, msg)
	return result, err
}
func (a *Array) SendQuery(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	if err := a.Authorize(req); err != nil {
		return nil, err
	}
	res, err := a.QuerySender.SendQuery(ctx, req)
	return res, err
}

func (a *Array) SendCommand(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	if err := a.Authorize(req); err != nil {
		return nil, err
	}
	res, err := a.CommandSender.SendCommand(ctx, req)
	return res, err
}
func (a *Array) SendResponse(ctx context.Context, res *pb.Response) error {
	if a.isShutdown.Load() {
		return entities.ErrShutdownMode
	}
	if err := a.Authorize(res); err != nil {
		return err
	}
	return a.ResponseSender.SendResponse(ctx, res)
}

func (a *Array) SendQueueMessage(ctx context.Context, msg *pb.QueueMessage) (*pb.SendQueueMessageResult, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	if IsRoutable(msg.Channel) {
		return a.processQueueMessageRoutingMessages(ctx, msg)
	} else {
		return a.sendQueueMessage(ctx, msg)
	}
}

func (a *Array) sendQueueMessage(ctx context.Context, msg *pb.QueueMessage) (*pb.SendQueueMessageResult, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	if err := a.Authorize(msg); err != nil {
		return nil, err
	}

	nc, err := a.NewQueueClientFromPool(msg.Channel + "-sender")
	if err != nil {
		return nil, err
	}
	defer a.queueClientsPool.ReleaseClient(msg.Channel + "-sender")
	res := ChainQueueSenders(nc, QueueSenderLogging(a.logger)).SendQueueMessage(ctx, msg)
	return res, nil
}

func (a *Array) SendQueueMessagesBatch(ctx context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	foundRouting := false
	for _, msg := range req.Messages {
		if IsRoutable(msg.Channel) {
			foundRouting = true
			break
		}
	}

	if foundRouting {
		return a.sendQueueMessagesBatchWithRouting(ctx, req)
	} else {
		return a.sendQueueMessagesBatch(ctx, req)
	}
}
func (a *Array) sendQueueMessagesBatchWithRouting(ctx context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	batchResult := &pb.QueueMessagesBatchResponse{
		BatchID:    req.BatchID,
		Results:    nil,
		HaveErrors: false,
	}
	for _, msg := range req.Messages {
		result, err := a.SendQueueMessage(ctx, msg)
		if err != nil {
			return nil, err
		}
		if result.IsError {
			batchResult.HaveErrors = true
		}
		batchResult.Results = append(batchResult.Results, result)
	}
	return batchResult, nil
}
func (a *Array) sendQueueMessagesBatch(ctx context.Context, req *pb.QueueMessagesBatchRequest) (*pb.QueueMessagesBatchResponse, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	for _, msg := range req.Messages {
		if err := a.Authorize(msg); err != nil {
			return nil, err
		}
	}
	nc,id, err := a.NewQueueUpstreamClientFromPool()
	if err != nil {
		return nil, err
	}
	defer a.ReleaseQueueUpstreamClientFromPool(id)
	res, err := ChainQueueBatchSenders(nc, QueueBatchSenderLogging(a.logger)).SendQueueMessagesBatch(ctx, req)
	return res, err
}

func (a *Array) ReceiveQueueMessages(ctx context.Context, req *pb.ReceiveQueueMessagesRequest) (*pb.ReceiveQueueMessagesResponse, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	if err := a.Authorize(req); err != nil {
		return nil, err
	}
	nc, err := a.NewQueueClientFromPool(req.Channel + "-receiver")
	if err != nil {
		return nil, err
	}
	defer a.queueClientsPool.ReleaseClient(req.Channel + "-receiver")

	res, err := ChainQueueReceivers(nc, QueueReceiverLogging(a.logger)).ReceiveQueueMessages(ctx, req)
	return res, err
}

func (a *Array) AckAllQueueMessages(ctx context.Context, req *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	if err := a.Authorize(req); err != nil {
		return nil, err
	}
	nc, err := a.NewQueueClientFromPool(req.Channel + "-receiver")
	if err != nil {
		return nil, err
	}
	defer a.queueClientsPool.ReleaseClient(req.Channel + "-receiver")
	res, err := ChainAckAllQueueMessages(nc, AckAllQueueMessagesLogging(a.logger)).AckAllQueueMessages(ctx, req)
	return res, err
}
func (a *Array) queueStreamMessageMiddleware(qc *client.QueueClient, msg *pb.StreamQueueMessagesRequest) error {
	if err := a.Authorize(msg); err != nil {
		return err
	}
	return nil
}

func (a *Array) StreamQueueMessage(ctx context.Context, requests chan *pb.StreamQueueMessagesRequest, response chan *pb.StreamQueueMessagesResponse, done chan bool) (string, error) {
	if a.isShutdown.Load() {
		return "", entities.ErrShutdownMode
	}

	queueClient, id, err := a.NewQueueDownstreamClientFromPool()
	if err != nil {
		return "", err
	}
	queueClient.SetQueueStreamMiddlewareFunc(a.queueStreamMessageMiddleware)
	go queueClient.StreamQueueMessage(ctx, requests, response, done)
	return id, nil
}

func (a *Array) SubscribeEventsStore(ctx context.Context, subReq *pb.Subscribe, msgCh chan *pb.EventReceive, errCh chan error) (string, error) {
	if a.isShutdown.Load() {
		return "", entities.ErrShutdownMode
	}
	if err := a.Authorize(subReq); err != nil {
		return "", err
	}
	nc, err := a.NewStoreClient(ctx, subReq.ClientID)
	if err != nil {
		return "", err
	}

	err = ChainEventsStoreReceiver(nc, EventsStoreReceiverLogging(a.logger)).SubscribeToEventsStore(ctx, subReq, msgCh)
	if err != nil {
		_ = a.DeleteClient(subReq.ClientID)
		a.clients.Delete(subReq.ClientID)
		return "", err
	}

	return subReq.ClientID, err
}

func (a *Array) SubscribeEvents(ctx context.Context, subReq *pb.Subscribe, msgCh chan *pb.EventReceive, errCh chan error) (string, error) {
	if a.isShutdown.Load() {
		return "", entities.ErrShutdownMode
	}
	if err := a.Authorize(subReq); err != nil {
		return "", err
	}
	id := nuid.Next()
	nc, err := a.NewClient(ctx, id, subReq.ClientID)
	if err != nil {
		return "", err
	}

	err = ChainEventsReceiver(nc, EventsReceiverLogging(a.logger)).SubscribeToEvents(ctx, subReq, msgCh)
	if err != nil {
		_ = a.DeleteClient(id)
		a.clients.Delete(id)
		return "", err
	}

	return id, err

}
func (a *Array) SubscribeToQueries(ctx context.Context, subReq *pb.Subscribe, reqCh chan *pb.Request, errCh chan error) (string, error) {
	if a.isShutdown.Load() {
		return "", entities.ErrShutdownMode
	}
	if err := a.Authorize(subReq); err != nil {
		return "", err
	}
	randID := nuid.Next()
	nc, err := a.NewClient(ctx, randID, subReq.ClientID)
	if err != nil {
		return "", err
	}

	err = ChainQueryReceiver(nc, QueryReceiverLogging(a.logger)).SubscribeToQueries(ctx, subReq, reqCh)
	if err != nil {
		_ = a.DeleteClient(randID)
		a.clients.Delete(randID)
		return "", err
	}
	return randID, err
}
func (a *Array) SubscribeToCommands(ctx context.Context, subReq *pb.Subscribe, reqCh chan *pb.Request, errCh chan error) (string, error) {
	if a.isShutdown.Load() {
		return "", entities.ErrShutdownMode
	}
	if err := a.Authorize(subReq); err != nil {
		return "", err
	}
	randID := nuid.Next()
	nc, err := a.NewClient(ctx, randID, subReq.ClientID)
	if err != nil {
		return "", err
	}

	err = ChainCommandReceiver(nc, CommandReceiverLogging(a.logger)).SubscribeToCommands(ctx, subReq, reqCh)
	if err != nil {
		_ = a.DeleteClient(randID)
		a.clients.Delete(randID)
		return "", err
	}
	return randID, err
}
