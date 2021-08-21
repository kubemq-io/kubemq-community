package metrics

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/api"
	ws "github.com/kubemq-io/kubemq-community/pkg/http"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/atomic"
	"net/http"
	"sync"
	"time"
)

var reportMetricSize = 1000
var reportMetricWorkers = 5
var reportMetricDropAfter = 100 * time.Millisecond
var promPort = 36742

var node = config.Host()
var labels = []string{"node", "client_id", "type", "side", "channel"}
var exporter *Exporter
var once sync.Once

func InitExporter(ctx context.Context) *Exporter {
	once.Do(func() {
		exporter = initExporter(ctx)
	})
	return exporter
}

func GetExporter() *Exporter {
	once.Do(func() {
		exporter = initExporter(context.Background())
	})
	return exporter
}

type Exporter struct {
	messagesCollector *promCounterMetric
	pendingCollector  *promGaugeMetric
	volumeCollector   *promCounterMetric
	clientsCollector  *promGaugeMetric
	errorsCollector   *promCounterMetric
	reportMetricCh    chan *reportMetric
	metricsDropped    *atomic.Uint64
	lastUpdate        *atomic.Int64
}

func LastUpdate() time.Time {
	if exporter != nil {
		return time.Unix(0, exporter.lastUpdate.Load())
	} else {
		return time.Now().UTC()
	}

}
func (e *Exporter) MetricsDropped() uint64 {
	return e.metricsDropped.Load()
}

func (e *Exporter) PrometheusString() (string, error) {
	str, err := ws.GetString(context.Background(), fmt.Sprintf("http://localhost:%d/metrics", promPort))
	if err != nil {
		return "", err
	}
	return str, nil
}
func (e *Exporter) PrometheusHandler() http.Handler {
	return promhttp.Handler()
}

func (e *Exporter) Channels() ([]*ChannelStats, error) {
	st, err := e.Stats()
	if err != nil {
		return nil, err
	}
	return toChannelStats(st), nil
}
func (e *Exporter) ChannelSummery(cs ...[]*ChannelStats) ([]*ChannelsSummery, error) {
	var stats []*ChannelStats
	var err error
	if len(cs) == 0 {
		stats, err = e.Channels()
		if err != nil {

			return nil, err
		}
	} else {
		stats = cs[0]
	}

	return toChannelsSummery(stats), nil
}

func (e *Exporter) Clients() ([]*ClientsStats, int, error) {
	st, err := e.Stats()
	if err != nil {
		return nil, 0, err
	}
	onlineClients := getOnlineClients(st)
	return toClientStats(st), onlineClients, nil
}

func (e *Exporter) Stats() ([]*Stats, error) {
	str, err := e.PrometheusString()
	if err != nil {
		return nil, err
	}
	results, err := parse(str)
	if err != nil {
		return nil, err
	}
	return toStats(results), nil
}
func (e *Exporter) Snapshot() (*api.Snapshot, error) {
	str, err := e.PrometheusString()
	if err != nil {
		return nil, err
	}
	results, err := parse(str)
	if err != nil {
		return nil, err
	}
	return getSnapshot(results), nil
}

func initExporter(ctx context.Context) *Exporter {
	e := &Exporter{
		reportMetricCh: make(chan *reportMetric, reportMetricSize),
		metricsDropped: atomic.NewUint64(0),
		lastUpdate:     atomic.NewInt64(0),
	}
	if !e.initPromMetrics() {
		return nil
	}
	e.initPromServer()
	for i := 0; i < reportMetricWorkers; i++ {
		go e.runWorker(ctx)
	}
	return e
}

func (e *Exporter) initPromMetrics() bool {
	e.messagesCollector = newPromCounterMetric(
		"messages",
		"count",
		"counts messages per node,type,side,channel",
		labels...,
	)
	e.pendingCollector = newPromGaugeMetric(
		"messages",
		"pending",
		"current pending messages per node,type,side,channel",
		labels...,
	)
	e.volumeCollector = newPromCounterMetric(
		"messages",
		"volume",
		"sum volume per node,type,side,channel",
		labels...,
	)
	e.clientsCollector = newPromGaugeMetric(
		"clients",
		"count",
		"holds amount of connected clients per node,type,side,channel",
		labels...,
	)
	e.errorsCollector = newPromCounterMetric(
		"errors",
		"count",
		"counts errors per node,type,side,channel",
		labels...,
	)

	err := prometheus.Register(e.messagesCollector.metric)
	if err != nil {
		return false
	}
	err = prometheus.Register(e.volumeCollector.metric)
	if err != nil {
		return false
	}
	err = prometheus.Register(e.clientsCollector.metric)
	if err != nil {
		return false
	}
	err = prometheus.Register(e.errorsCollector.metric)
	if err != nil {
		return false
	}
	err = prometheus.Register(e.pendingCollector.metric)

	return err == nil
}
func (e *Exporter) initPromServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		_ = http.ListenAndServe(fmt.Sprintf(":%d", promPort), mux)
	}()

}

func getLabels(ty, cl, sd, ch string) prometheus.Labels {
	return prometheus.Labels{
		"node":      node,
		"client_id": cl,
		"type":      ty,
		"side":      sd,
		"channel":   ch,
	}
}
func (e *Exporter) reportEvent(event *pb.Event, res *pb.Result) {
	var lbls prometheus.Labels
	if event.Store {
		lbls = getLabels("events_store", event.ClientID, "send", event.Channel)
	} else {
		lbls = getLabels("events", event.ClientID, "send", event.Channel)
	}
	if res != nil {
		if res.Sent {
			e.messagesCollector.inc(lbls)
			e.volumeCollector.add(float64(event.Size()), lbls)
		} else {
			e.errorsCollector.inc(lbls)
		}
	}
}

func (e *Exporter) reportEventReceive(event *pb.EventReceive, subReq *pb.Subscribe) {
	var lbls prometheus.Labels
	if subReq.SubscribeTypeData == 2 {
		lbls = getLabels("events_store", subReq.ClientID, "receive", event.Channel)
	} else {
		lbls = getLabels("events", subReq.ClientID, "receive", event.Channel)
	}

	e.messagesCollector.inc(lbls)
	e.volumeCollector.add(float64(event.Size()), lbls)

}

func (e *Exporter) reportRequest(request *pb.Request, response *pb.Response, err error) {
	var lbls prometheus.Labels
	var reqType, side string
	if request.RequestTypeData == pb.Request_Command {
		reqType = "commands"
	}
	if request.RequestTypeData == pb.Request_Query {
		reqType = "queries"
	}
	if request.ReplyChannel == "" {
		side = "send"
	} else {
		side = "receive"
	}
	lbls = getLabels(reqType, request.ClientID, side, request.Channel)
	e.messagesCollector.inc(lbls)
	volume := request.Size()
	e.volumeCollector.add(float64(volume), lbls)
	if err == nil && response != nil && !response.Executed {
		e.errorsCollector.inc(lbls)
	}
	if response != nil {
		reslbls := getLabels("responses", response.ClientID, "send", request.Channel)
		e.messagesCollector.inc(reslbls)
		e.volumeCollector.add(float64(response.Size()), reslbls)

	}

}
func (e *Exporter) reportResponse(response *pb.Response, err error) {
	lbls := getLabels("responses", response.ClientID, "send", "")
	if err != nil {
		e.errorsCollector.inc(lbls)
	}
}

func (e *Exporter) reportSendQueueMessage(message *pb.QueueMessage, res *pb.SendQueueMessageResult) {
	lbls := getLabels("queues", message.ClientID, "send", message.Channel)
	if !res.IsError {
		e.messagesCollector.inc(lbls)
		e.volumeCollector.add(float64(message.Size()), lbls)
	} else {
		e.errorsCollector.inc(lbls)
	}
}
func (e *Exporter) reportSendQueueMessageBatch(batch *pb.QueueMessagesBatchRequest, results *pb.QueueMessagesBatchResponse) {

	for i := 0; i < len(batch.Messages); i++ {
		message := batch.Messages[i]
		res := results.Results[i]
		lbls := getLabels("queues", message.ClientID, "send", message.Channel)
		if !res.IsError {
			e.messagesCollector.inc(lbls)
			e.volumeCollector.add(float64(message.Size()), lbls)
		} else {
			e.errorsCollector.inc(lbls)
		}
	}
}
func (e *Exporter) reportQueueUpstreamRequest(request *pb.QueuesUpstreamRequest) {

	for i := 0; i < len(request.Messages); i++ {
		message := request.Messages[i]
		lbls := getLabels("queues", message.ClientID, "send", message.Channel)
		e.messagesCollector.inc(lbls)
		e.volumeCollector.add(float64(message.Size()), lbls)
	}
}
func (e *Exporter) reportReceiveQueueMessages(request *pb.ReceiveQueueMessagesRequest, res *pb.ReceiveQueueMessagesResponse) {
	for i := 0; i < len(res.Messages); i++ {
		message := res.Messages[i]
		lbls := getLabels("queues", request.ClientID, "receive", message.Channel)
		if !res.IsError {
			e.messagesCollector.inc(lbls)
			e.volumeCollector.add(float64(message.Size()), lbls)
		} else {
			e.errorsCollector.inc(lbls)
		}
	}

}

func (e *Exporter) reportReceiveStreamQueueMessage(message *pb.QueueMessage) {
	lbls := getLabels("queues", message.ClientID, "receive", message.Channel)
	e.messagesCollector.inc(lbls)
	e.volumeCollector.add(float64(message.Size()), lbls)

}
func (e *Exporter) reportClient(pattern, side, channel string, value float64) {
	lbls := getLabels(pattern, "", side, channel)
	e.clientsCollector.add(value, lbls)
}

func (e *Exporter) reportPending(pattern, clientId, channel string, value float64) {
	lbls := getLabels(pattern, clientId, "receive", channel)
	e.pendingCollector.set(value, lbls)
}

func (e *Exporter) insertReport(metric *reportMetric) {
	select {
	case e.reportMetricCh <- metric:
	case <-time.After(reportMetricDropAfter):
		e.metricsDropped.Inc()
	}

}

func (e *Exporter) runWorker(ctx context.Context) {

	for {
		select {
		case metric := <-e.reportMetricCh:
			e.processMetric(metric)
		case <-ctx.Done():
			return
		}
	}
}

func (e *Exporter) processMetric(metric *reportMetric) {
	if metric.params == nil {
		e.metricsDropped.Inc()
		return
	}
	switch metric.reportType {
	case reportTypeEvents:
		ev, _ := metric.params[0].(*pb.Event)
		res, _ := metric.params[1].(*pb.Result)
		e.reportEvent(ev, res)

	case reportTypeEventsReceive:
		ev, _ := metric.params[0].(*pb.EventReceive)
		req, _ := metric.params[1].(*pb.Subscribe)
		e.reportEventReceive(ev, req)
	case reportTypeRequest:
		req, _ := metric.params[0].(*pb.Request)
		res, _ := metric.params[1].(*pb.Response)
		err, _ := metric.params[2].(error)
		e.reportRequest(req, res, err)

	case reportTypeResponse:
		res, _ := metric.params[0].(*pb.Response)
		err, _ := metric.params[1].(error)
		e.reportResponse(res, err)
	case reportTypeSendQueueMessage:

		msg, _ := metric.params[0].(*pb.QueueMessage)
		res, _ := metric.params[1].(*pb.SendQueueMessageResult)
		e.reportSendQueueMessage(msg, res)
	case reportTypeSendQueueMessageBatch:
		btch, _ := metric.params[0].(*pb.QueueMessagesBatchRequest)
		results, _ := metric.params[1].(*pb.QueueMessagesBatchResponse)
		e.reportSendQueueMessageBatch(btch, results)
	case reportTypeQueueUpstreamRequest:
		req, _ := metric.params[0].(*pb.QueuesUpstreamRequest)
		e.reportQueueUpstreamRequest(req)
	case reportTypeReceiveQueueMessages:
		req, _ := metric.params[0].(*pb.ReceiveQueueMessagesRequest)
		res, _ := metric.params[1].(*pb.ReceiveQueueMessagesResponse)
		e.reportReceiveQueueMessages(req, res)
	case reportTypeReceiveStreamQueueMessage:
		msg, _ := metric.params[0].(*pb.QueueMessage)
		e.reportReceiveStreamQueueMessage(msg)
	case reportTypeClients:
		pattern, _ := metric.params[0].(string)
		side, _ := metric.params[1].(string)
		channel, _ := metric.params[2].(string)
		value, _ := metric.params[3].(float64)
		e.reportClient(pattern, side, channel, value)
	case reportTypePending:
		pattern, _ := metric.params[0].(string)
		clientId, _ := metric.params[1].(string)
		channel, _ := metric.params[2].(string)
		value, _ := metric.params[3].(float64)
		e.reportPending(pattern, clientId, channel, value)
	}
	e.lastUpdate.Store(time.Now().UTC().UnixNano())
}
