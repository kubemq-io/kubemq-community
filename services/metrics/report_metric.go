package metrics

import (
	pb "github.com/kubemq-io/protobuf/go"
)

type reportType int32

const (
	reportTypeEvents reportType = iota
	reportTypeEventsReceive
	reportTypeRequest
	reportTypeResponse
	reportTypeSendQueueMessage
	reportTypeSendQueueMessageBatch
	reportTypeReceiveQueueMessages
	reportTypeReceiveStreamQueueMessage
	reportTypeClients
	reportTypePending
	reportTypeQueueUpstreamRequest
)

type reportMetric struct {
	reportType reportType
	params     []interface{}
}

func ReportEvent(event *pb.Event, result *pb.Result) {
	if exporter != nil {
		if event == nil {
			exporter.metricsDropped.Inc()
			return
		}
		if event.ClientID == "" || event.Channel == "" {
			exporter.metricsDropped.Inc()
			return
		}
		exporter.insertReport(&reportMetric{
			reportType: reportTypeEvents,
			params:     []interface{}{event, result},
		})
	}
}

func ReportEventReceive(event *pb.EventReceive, subReq *pb.Subscribe) {
	if exporter != nil {
		if event == nil || subReq == nil {
			exporter.metricsDropped.Inc()
			return
		}
		if subReq.SubscribeTypeData != 1 && subReq.SubscribeTypeData != 2 {
			exporter.metricsDropped.Inc()
			return
		}

		if subReq.ClientID == "" {
			exporter.metricsDropped.Inc()
			return
		}
		exporter.insertReport(&reportMetric{
			reportType: reportTypeEventsReceive,
			params:     []interface{}{event, subReq},
		})
	}
}
func ReportRequest(request *pb.Request, response *pb.Response, err error) {
	if exporter != nil {
		if request == nil {
			exporter.metricsDropped.Inc()
			return
		}
		if request.Channel == "" || request.ClientID == "" {
			exporter.metricsDropped.Inc()
			return
		}
		exporter.insertReport(&reportMetric{
			reportType: reportTypeRequest,
			params:     []interface{}{request, response, err},
		})
	}

}

func ReportResponse(response *pb.Response, err error) {
	if exporter != nil {
		if response == nil {
			exporter.metricsDropped.Inc()
			return
		}
		if response.ClientID == "" {
			exporter.metricsDropped.Inc()
			return
		}
		exporter.insertReport(&reportMetric{
			reportType: reportTypeResponse,
			params:     []interface{}{response, err},
		})
	}

}
func ReportSendQueueMessage(msg *pb.QueueMessage, res *pb.SendQueueMessageResult) {

	if exporter != nil {

		if msg == nil || res == nil {
			exporter.metricsDropped.Inc()
			return
		}

		exporter.insertReport(&reportMetric{
			reportType: reportTypeSendQueueMessage,
			params:     []interface{}{msg, res},
		})
	}
}
func ReportSendQueueMessageBatch(batch *pb.QueueMessagesBatchRequest, res *pb.QueueMessagesBatchResponse) {
	if exporter != nil {
		if batch == nil || res == nil {
			exporter.metricsDropped.Inc()
			return
		}
		exporter.insertReport(&reportMetric{
			reportType: reportTypeSendQueueMessageBatch,
			params:     []interface{}{batch, res},
		})
	}
}
func ReportQueueUpstreamRequest(req *pb.QueuesUpstreamRequest) {
	if exporter != nil {
		exporter.insertReport(&reportMetric{
			reportType: reportTypeQueueUpstreamRequest,
			params:     []interface{}{req},
		})
	}
}
func ReportReceiveQueueMessages(request *pb.ReceiveQueueMessagesRequest, response *pb.ReceiveQueueMessagesResponse) {
	if exporter != nil {
		if request == nil || response == nil {
			exporter.metricsDropped.Inc()
			return
		}
		if request.IsPeak {
			return
		}
		exporter.insertReport(&reportMetric{
			reportType: reportTypeReceiveQueueMessages,
			params:     []interface{}{request, response},
		})
	}
}

func ReportReceiveStreamQueueMessage(message *pb.QueueMessage) {
	if exporter != nil {
		if message == nil {
			exporter.metricsDropped.Inc()
			return
		}
		exporter.insertReport(&reportMetric{
			reportType: reportTypeReceiveStreamQueueMessage,
			params:     []interface{}{message},
		})
	}
}

func ReportClient(pattern, side, channel string, value float64) {
	if exporter != nil {
		if pattern == "" || side == "" || channel == "" || value == 0 {
			exporter.metricsDropped.Inc()
		} else {
			exporter.insertReport(&reportMetric{
				reportType: reportTypeClients,
				params:     []interface{}{pattern, side, channel, value},
			})
		}
	}
}

func ReportPending(pattern, clientId, channel string, value float64) {
	if exporter != nil {
		if pattern == "" || channel == "" || clientId == "" {
			exporter.metricsDropped.Inc()
		} else {
			exporter.insertReport(&reportMetric{
				reportType: reportTypePending,
				params:     []interface{}{pattern, clientId, channel, value},
			})
		}
	}
}
