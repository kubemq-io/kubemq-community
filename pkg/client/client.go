package client

import (
	"fmt"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/nats-io/nuid"

	"sync"
	"time"

	nats "github.com/kubemq-io/broker/client/nats"
	stan "github.com/kubemq-io/broker/client/stan"
	"github.com/kubemq-io/kubemq-community/pkg/logging"

	"context"

	"go.uber.org/atomic"
)

const (
	prefixEvents      = "_EVENTS_."
	prefixEventsStore = "_EVENTS_STORE_."
	prefixCommands    = "_COMMANDS_."
	prefixQueries     = "_QUERIES_."
	prefixQueues      = "_QUEUES_."
	delayQueueChannel = "_QUEUE_DELAY_"
)
const (
	storeConnectionTime = 30 * time.Second
	defaultDeadline     = 2000 * time.Millisecond
	storePingInterval   = 30000
	storePingMaxOut     = 30000
)

type Client struct {
	options *Options
	mu      sync.Mutex
	logger  *logging.Logger

	isUp            *atomic.Bool
	baseConn        *nats.Conn
	subscriber      *nats.Subscription
	storeConn       stan.Conn
	queueConn       stan.Conn
	storeSubscriber stan.Subscription
	writeDeadline   time.Duration
	isStoreClient   bool
	isQueueClient   bool
}

func getBaseClient(opts *Options, isPersistence bool) (*Client, error) {
	err := validateOptions(opts, isPersistence)
	if err != nil {
		return nil, err
	}
	nc := &Client{
		options:       opts,
		logger:        logging.GetLogFactory().NewLogger("client-" + opts.ClientID),
		isUp:          atomic.NewBool(false),
		isStoreClient: isPersistence,
	}
	if opts.Deadline == 0 {
		nc.writeDeadline = defaultDeadline
	} else {
		nc.writeDeadline = opts.Deadline
	}
	return nc, nil
}

func NewClient(opts *Options) (*Client, error) {
	nc, err := getBaseClient(opts, false)
	if err != nil {
		return nil, err
	}
	err = nc.connect(opts)
	if err != nil {
		return nil, err
	}
	nc.isUp.Store(true)
	return nc, nil
}

func NewStoreClient(opts *Options) (*Client, error) {
	nc, err := getBaseClient(opts, true)
	if err != nil {
		return nil, err
	}
	err = nc.connect(opts)
	if err != nil {
		return nil, err
	}

	nc.storeConn, err = nc.connectStore(nc.baseConn, opts)
	if err != nil {
		return nil, err
	}
	nc.isUp.Store(true)
	return nc, nil
}

func (c *Client) connectStore(conn *nats.Conn, opts *Options) (stan.Conn, error) {
	storeConn, err := stan.Connect("kubemq", opts.ClientID,
		stan.NatsConn(conn),
		stan.NatsURL(fmt.Sprintf("nats://0.0.0.0:%d", 4224)),
		stan.ConnectWait(storeConnectionTime),
		stan.Pings(storePingInterval, storePingMaxOut),
		stan.SetConnectionLostHandler(func(conn stan.Conn, err error) {
			c.logger.Errorf("store client %s connection lost", opts.ClientID)
		}),
	)
	return storeConn, err
}

func (c *Client) connect(opts *Options) error {

	natsOpts := nats.Options{
		Url: fmt.Sprintf("nats://0.0.0.0:%d", 4224),
		DisconnectedErrCB: func(conn *nats.Conn, err error) {
			conn.Close()
		},
	}
	if opts.AutoReconnect {
		natsOpts.AllowReconnect = true
		natsOpts.MaxReconnect = -1
		natsOpts.ReconnectWait = 100 * time.Millisecond
		natsOpts.ReconnectBufSize = 1000
	}
	if opts.MemoryPipe != nil {
		natsOpts.CustomDialer = opts.MemoryPipe
	} else {
		return fmt.Errorf("no memory pipe exist")
	}

	conn, err := natsOpts.Connect()
	if err != nil {
		return err
	}
	c.baseConn = conn

	return nil
}
func (c *Client) UnSubscribe() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isStoreClient {
		_ = c.storeSubscriber.Unsubscribe()
	}
	return c.subscriber.Unsubscribe()
}
func (c *Client) Disconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.isUp.Load() {
		return nil
	}
	c.isUp.Store(false)

	if c.isStoreClient {
		c.storeConn.Close()
	}
	if c.isQueueClient {
		c.queueConn.Close()
	}

	c.baseConn.Close()
	return nil
}

func (c *Client) SendEvents(ctx context.Context, event *pb.Event) (*pb.Result, error) {

	if !c.isUp.Load() {
		return nil, entities.ErrConnectionNoAvailable
	}
	if err := ValidateEventMessage(event); err != nil {
		return nil, err
	}
	if event.EventID == "" {
		event.EventID = nuid.Next()
	}
	result := &pb.Result{
		EventID: event.EventID,
		Sent:    true,
		Error:   "",
	}

	channel := fmt.Sprintf("%s%s", prefixEvents, event.Channel)
	data, err := event.Marshal()
	if err != nil {
		return nil, err
	}
	err = c.baseConn.Publish(channel, data)
	if err != nil {
		result.Sent = false
		result.Error = err.Error()
	}

	return result, nil

}
func (c *Client) SendEventsStore(ctx context.Context, event *pb.Event) (*pb.Result, error) {

	if !c.isUp.Load() {
		return nil, entities.ErrConnectionNoAvailable
	}
	if err := ValidateEventMessage(event); err != nil {
		return nil, err
	}

	if event.EventID == "" {
		event.EventID = nuid.Next()
	}
	result := &pb.Result{
		EventID: event.EventID,
		Sent:    true,
		Error:   "",
	}
	channel := prefixEventsStore + event.Channel

	data, err := event.Marshal()
	if err != nil {
		return nil, err
	}

	_, err = c.storeConn.PublishAsync(channel, data, func(s string, err error) {
	})
	if err != nil {
		result.Sent = false
		result.Error = err.Error()
	}

	return result, nil

}

func (c *Client) SubscribeToEvents(ctx context.Context, subReq *pb.Subscribe, eventsCh chan *pb.EventReceive) error {
	if !c.isUp.Load() {
		return entities.ErrConnectionNoAvailable
	}

	if err := ValidateSubscriptionToEvents(subReq, entities.KindTypeEvent); err != nil {
		return err
	}
	var err error

	channel := fmt.Sprintf("%s%s", prefixEvents, subReq.Channel)

	c.subscriber, err = c.baseConn.QueueSubscribe(channel, subReq.Group, func(r *nats.Msg) {

		eventReceive, err := UnmarshalToEventReceive(r.Data)
		if err != nil {
			c.logger.Errorf("error on unmarshal message", "error", err.Error())
			return
		}

		select {
		case <-ctx.Done():
			return
		case eventsCh <- eventReceive:
		case <-time.After(c.writeDeadline):
			c.logger.Warnw("events message dropped due to slow consumer", "channel", channel, "event_id", eventReceive.EventID, "metadata", eventReceive.Metadata)
		}

	})
	return err
}

func (c *Client) SubscribeToEventsStore(ctx context.Context, subReq *pb.Subscribe, eventsCh chan *pb.EventReceive) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.isUp.Load() {
		return entities.ErrConnectionNoAvailable
	}

	if err := ValidateSubscriptionToEvents(subReq, entities.KindTypeEventStore); err != nil {
		return err
	}
	subOpts := ParseSubscriptionRequest(subReq)
	channel := prefixEventsStore + subReq.Channel

	var err error
	c.storeSubscriber, err = c.storeConn.QueueSubscribe(channel, subReq.Group, func(msg *stan.Msg) {

		c.mu.Lock()
		defer c.mu.Unlock()
		var err error
		eventReceive, err := unmarshalToEventReceive(msg.Data)
		if err != nil {
			c.logger.Errorf("error on unmarshal message, %s", err.Error())
			return
		}
		eventReceive.Timestamp = msg.Timestamp
		eventReceive.Sequence = msg.Sequence
		select {
		case <-ctx.Done():
			return
		case eventsCh <- eventReceive:

		case <-time.After(c.writeDeadline):
			c.logger.Warnw("events store message dropped due to slow consumer", "channel", channel, "event_id", eventReceive.EventID, "metadata", eventReceive.Metadata)
		}

	}, subOpts...)

	return err
}

func (c *Client) SendCommand(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	if !c.isUp.Load() {
		return nil, entities.ErrConnectionNoAvailable
	}

	if err := ValidateRequest(req); err != nil {
		return nil, err
	}
	if req.RequestID == "" {
		req.RequestID = nuid.Next()
	}
	channel := prefixCommands + req.Channel

	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(req.Timeout)*time.Millisecond)
	defer cancel()
	data, err := req.Marshal()
	if err != nil {
		return nil, err
	}
	resMsg, err := c.baseConn.RequestWithContext(ctxTimeout, channel, data)
	if err != nil {
		return nil, entities.ErrRequestTimeout
	}

	res := &pb.Response{}
	err = res.Unmarshal(resMsg.Data)
	if err != nil {
		return nil, entities.ErrInvalidResponseFormat
	}
	res.Body = nil
	res.Metadata = ""
	res.CacheHit = false
	res.ReplyChannel = ""

	return res, nil
}

func (c *Client) SendQuery(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	if !c.isUp.Load() {
		return nil, entities.ErrConnectionNoAvailable
	}

	if err := ValidateRequest(req); err != nil {
		return nil, err
	}
	if req.RequestID == "" {
		req.RequestID = nuid.Next()
	}
	channel := prefixQueries + req.Channel

	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(req.Timeout)*time.Millisecond)
	defer cancel()
	data, err := req.Marshal()
	if err != nil {
		return nil, err
	}
	resMsg, err := c.baseConn.RequestWithContext(ctxTimeout, channel, data)
	if err != nil {
		return nil, entities.ErrRequestTimeout
	}
	res := &pb.Response{}
	err = res.Unmarshal(resMsg.Data)
	if err != nil {
		return nil, entities.ErrInvalidResponseFormat
	}
	res.ReplyChannel = ""
	return res, nil
}

func (c *Client) SendResponse(ctx context.Context, res *pb.Response) error {
	if !c.isUp.Load() {
		return entities.ErrConnectionNoAvailable
	}
	if err := ValidateResponse(res); err != nil {
		return err
	}
	data, err := res.Marshal()
	if err != nil {
		return err
	}

	err = c.baseConn.Publish(res.ReplyChannel, data)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) SubscribeToCommands(ctx context.Context, subReq *pb.Subscribe, reqCh chan *pb.Request) error {
	if !c.isUp.Load() {
		return entities.ErrConnectionNoAvailable
	}
	if err := ValidateSubscriptionToRequests(subReq); err != nil {
		return err
	}
	channel := prefixCommands + subReq.Channel

	var err error
	c.subscriber, err = c.baseConn.QueueSubscribe(channel, subReq.Group, func(r *nats.Msg) {

		req := &pb.Request{}
		err := req.Unmarshal(r.Data)
		if err == nil {
			req.ReplyChannel = r.Reply
			req.CacheKey = ""
			req.CacheTTL = 0
			req.Timeout = 0
			select {
			case <-ctx.Done():
				return
			case reqCh <- req:

			case <-time.After(c.writeDeadline):
				c.logger.Warnw("command request dropped due to slow consumer", "channel", channel, "client_id", req.ClientID, "request_id", req.RequestID, "metadata", req.Metadata)
			}

		} else {
			c.logger.Errorw("error on unmarshal commands request", "error", err)
		}
	})
	return err
}

func (c *Client) SubscribeToQueries(ctx context.Context, subReq *pb.Subscribe, reqCh chan *pb.Request) error {
	if !c.isUp.Load() {
		return entities.ErrConnectionNoAvailable
	}
	if err := ValidateSubscriptionToRequests(subReq); err != nil {
		return err
	}
	channel := prefixQueries + subReq.Channel

	var err error
	c.subscriber, err = c.baseConn.QueueSubscribe(channel, subReq.Group, func(r *nats.Msg) {
		req := &pb.Request{}
		err := req.Unmarshal(r.Data)
		if err == nil {
			req.ReplyChannel = r.Reply
			req.CacheKey = ""
			req.CacheTTL = 0
			req.Timeout = 0
			select {
			case <-ctx.Done():
				return
			case reqCh <- req:

			case <-time.After(c.writeDeadline):
				c.logger.Warnw("query request dropped due to slow consumer", "channel", channel, "client_id", req.ClientID, "request_id", req.RequestID, "metadata", req.Metadata)
			}

		} else {
			c.logger.Errorw("error on unmarshal query request", "error", err)
		}
	})
	return err
}
