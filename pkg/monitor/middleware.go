package monitor

import (
	"context"
	"github.com/nats-io/nuid"

	"github.com/kubemq-io/kubemq-community/config"
	pb "github.com/kubemq-io/protobuf/go"
	"sync"

	"github.com/kubemq-io/kubemq-community/pkg/logging"

	"encoding/json"

	"fmt"

	"github.com/kubemq-io/kubemq-community/pkg/client"
)

const middlewareCommChannel = "kubemq_monitor"

type MiddlewareMessage struct {
	Kind             string `json:"kind"`
	MonitoredChannel string `json:"monitored_channel"`
	PublishChannel   string `json:"publish_channel"`
}

func NewMiddlewareMessage(msg *pb.EventReceive) *MiddlewareMessage {
	mm := &MiddlewareMessage{}
	if msg.Metadata != "middleware_message" {
		return mm
	}
	_ = json.Unmarshal(msg.Body, mm)
	return mm
}
func (mm *MiddlewareMessage) ToMessage(channel string) *pb.Event {
	data, _ := json.Marshal(mm)
	return &pb.Event{
		ClientID: "internal-client-id",
		Channel:  channel,
		Metadata: "middleware_message",
		Body:     data,
	}
}

func registerChannel(channel string, nc *client.Client) {
	mm := &MiddlewareMessage{
		Kind:             "register",
		MonitoredChannel: channel,
		PublishChannel:   fmt.Sprintf("%s.%s", responseChannelPrefix, channel),
	}
	_, _ = nc.SendEvents(context.Background(), mm.ToMessage(middlewareCommChannel))
}

func unregisterChannel(channel string, nc *client.Client) {
	mm := &MiddlewareMessage{
		Kind:             "de-register",
		MonitoredChannel: channel,
	}
	_, _ = nc.SendEvents(context.Background(), mm.ToMessage(middlewareCommChannel))
}

type Middleware struct {
	Stopped chan struct{}

	context                context.Context
	cancelFunc             context.CancelFunc
	mu                     sync.RWMutex
	channelRegistrationMap map[string]int
	natsClient             *client.Client
	logger                 *logging.Logger
	wg                     sync.WaitGroup
}

func NewMonitorMiddleware(ctx context.Context, appConfig *config.Config) (*Middleware, error) {

	md := &Middleware{
		Stopped:                make(chan struct{}, 1),
		channelRegistrationMap: map[string]int{},
		logger:                 logging.GetLogFactory().NewLogger("monitor_middleware"),
	}
	md.context, md.cancelFunc = context.WithCancel(ctx)
	var err error
	natsOptions := client.NewClientOptions("monitor-middleware-client" + nuid.Next()).
		SetMemoryPipe(appConfig.Broker.MemoryPipe)

	md.natsClient, err = client.NewClient(natsOptions)
	if err != nil {
		return nil, err
	}
	msgCh := make(chan *pb.EventReceive, 10)
	subReq := &pb.Subscribe{
		ClientID: "middleware_monitor_client",
		Channel:  middlewareCommChannel,
		Group:    "",
	}
	err = md.natsClient.SubscribeToEvents(md.context, subReq, msgCh)
	if err != nil {
		return nil, err
	}
	md.wg.Add(1)
	go md.runMiddlewareCommChannel(md.context, msgCh)
	return md, nil
}

func (md *Middleware) Shutdown() {
	md.cancelFunc()
	md.wg.Wait()
	_ = md.natsClient.Disconnect()
	md.Stopped <- struct{}{}
}
func (md *Middleware) runMiddlewareCommChannel(ctx context.Context, msgChan chan *pb.EventReceive) {
	defer md.wg.Done()
	for {
		select {
		case msg := <-msgChan:
			mm := NewMiddlewareMessage(msg)
			switch mm.Kind {
			case "register":
				md.mu.Lock()
				md.channelRegistrationMap[mm.MonitoredChannel]++
				md.mu.Unlock()
			case "de-register":
				md.mu.Lock()
				md.channelRegistrationMap[mm.MonitoredChannel]--
				if md.channelRegistrationMap[mm.MonitoredChannel] == 0 {
					delete(md.channelRegistrationMap, mm.MonitoredChannel)
				}
				md.mu.Unlock()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (md *Middleware) CheckAndSendQuery(ctx context.Context, req *pb.Request, res *pb.Response, err error) {
	md.mu.RLock()
	defer md.mu.RUnlock()
	_, ok := md.channelRegistrationMap[req.Channel]
	if !ok {
		return
	}
	var msg *pb.Event
	if err != nil {
		re := NewResponseErrorFromRequest(req, err)
		msg = re.ToMessage(fmt.Sprintf("%s.%s", responseChannelPrefix, req.Channel))
	} else {
		msg = responseToMessage(res, fmt.Sprintf("%s.%s", responseChannelPrefix, req.Channel))
	}
	_, pubErr := md.natsClient.SendEvents(ctx, msg)
	if pubErr != nil {
		md.logger.Errorw("error on publish monitor middleware", "error", pubErr)
	}

}

func (md *Middleware) CheckAndSendCommand(ctx context.Context, req *pb.Request, res *pb.Response, err error) {
	md.mu.RLock()
	defer md.mu.RUnlock()
	_, ok := md.channelRegistrationMap[req.Channel]
	if !ok {
		return
	}
	var msg *pb.Event
	if err != nil {
		re := NewResponseErrorFromRequest(req, err)
		msg = re.ToMessage(fmt.Sprintf("%s.%s", responseChannelPrefix, req.Channel))
	} else {
		msg = responseToMessage(res, fmt.Sprintf("%s.%s", responseChannelPrefix, req.Channel))
	}
	_, pubErr := md.natsClient.SendEvents(ctx, msg)
	if pubErr != nil {
		md.logger.Errorw("error on publish monitor middleware", "error", pubErr)
	}

}
