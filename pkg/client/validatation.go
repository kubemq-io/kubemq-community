package client

import (
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	pb "github.com/kubemq-io/protobuf/go"
	"strings"
)

func ValidateEventMessage(msg *pb.Event) error {
	if msg.ClientID == "" {
		return entities.ErrInvalidClientID
	}
	if msg.Channel == "" {
		return entities.ErrInvalidChannel
	}
	if strings.HasSuffix(msg.Channel, ".") {
		return entities.ErrInvalidEndChannelSeparator
	}
	if strings.Contains(msg.Channel, " ") {
		return entities.ErrInvalidWhitespace
	}
	if strings.Contains(msg.Channel, "*") || strings.Contains(msg.Channel, ">") {
		return entities.ErrInvalidWildcards
	}

	if msg.Metadata == "" && len(msg.Body) == 0 {
		return entities.ErrMessageEmpty
	}
	return nil
}

func ValidateQueueMessage(msg *pb.QueueMessage) error {
	if msg.ClientID == "" {
		return entities.ErrInvalidClientID
	}
	if msg.Channel == "" {
		return entities.ErrInvalidQueueName
	}
	if strings.HasSuffix(msg.Channel, ".") {
		return entities.ErrInvalidQueueName
	}
	if strings.Contains(msg.Channel, " ") {
		return entities.ErrInvalidQueueName
	}
	if strings.Contains(msg.Channel, "*") || strings.Contains(msg.Channel, ">") {
		return entities.ErrInvalidQueueName
	}

	if msg.Metadata == "" && len(msg.Body) == 0 {
		return entities.ErrMessageEmpty
	}
	return nil
}

func ValidateReceiveQueueMessageRequest(subRequest *pb.ReceiveQueueMessagesRequest) error {

	if subRequest.ClientID == "" {
		return entities.ErrInvalidClientID
	}
	if subRequest.Channel == "" {
		return entities.ErrInvalidQueueName
	}
	if strings.HasSuffix(subRequest.Channel, ".") {
		return entities.ErrInvalidQueueName
	}
	if strings.Contains(subRequest.Channel, " ") {
		return entities.ErrInvalidQueueName
	}
	if strings.Contains(subRequest.Channel, "*") || strings.Contains(subRequest.Channel, ">") {
		return entities.ErrInvalidQueueName
	}

	return nil
}

func ValidateSubscriptionToEvents(subReq *pb.Subscribe, kindID entities.KindType) error {

	if subReq.ClientID == "" {
		return entities.ErrInvalidClientID
	}
	if subReq.Channel == "" {
		return entities.ErrInvalidChannel
	}
	if strings.HasSuffix(subReq.Channel, ".") {
		return entities.ErrInvalidEndChannelSeparator
	}
	if strings.Contains(subReq.Channel, " ") {
		return entities.ErrInvalidWhitespace
	}
	if kindID != entities.KindTypeEventStore && subReq.EventsStoreTypeData != pb.Subscribe_EventsStoreTypeUndefined {
		return entities.ErrInvalidEventStoreType
	}
	if kindID == entities.KindTypeEventStore && (strings.Contains(subReq.Channel, "*") || strings.Contains(subReq.Channel, ">")) {
		return entities.ErrInvalidWildcards
	}
	if kindID == entities.KindTypeEventStore && subReq.EventsStoreTypeData == pb.Subscribe_EventsStoreTypeUndefined {
		return entities.ErrInvalidSubscriptionType
	}

	if kindID == entities.KindTypeEventStore && subReq.EventsStoreTypeData == pb.Subscribe_StartAtSequence && subReq.EventsStoreTypeValue <= 0 {
		return entities.ErrInvalidStartSequenceValue
	}

	if kindID == entities.KindTypeEventStore && subReq.EventsStoreTypeData == pb.Subscribe_StartAtTime && subReq.EventsStoreTypeValue <= 0 {
		return entities.ErrInvalidStartAtTimeValue
	}

	if kindID == entities.KindTypeEventStore && subReq.EventsStoreTypeData == pb.Subscribe_StartAtTimeDelta && subReq.EventsStoreTypeValue <= 0 {
		return entities.ErrInvalidStartAtTimeDeltaValue
	}
	return nil
}

func ValidateRequest(req *pb.Request) error {
	if req.ClientID == "" {
		return entities.ErrInvalidClientID
	}
	if req.Channel == "" {
		return entities.ErrInvalidChannel
	}
	if strings.HasSuffix(req.Channel, ".") {
		return entities.ErrInvalidEndChannelSeparator
	}
	if strings.Contains(req.Channel, " ") {
		return entities.ErrInvalidWhitespace
	}
	if strings.Contains(req.Channel, "*") || strings.Contains(req.Channel, ">") {
		return entities.ErrInvalidWildcards
	}
	if req.Timeout <= 0 {
		return entities.ErrInvalidSetTimeout
	}

	if req.Metadata == "" && len(req.Body) == 0 {
		return entities.ErrRequestEmpty
	}

	if req.CacheKey != "" && req.CacheTTL <= 0 {
		return entities.ErrInvalidCacheTTL
	}

	return nil
}

func ValidateResponse(res *pb.Response) error {
	if res.ClientID == "" {
		return entities.ErrInvalidClientID
	}
	if res.ReplyChannel == "" {
		return entities.ErrInvalidChannel
	}

	if strings.HasSuffix(res.ReplyChannel, ".") {
		return entities.ErrInvalidEndChannelSeparator
	}
	if strings.Contains(res.ReplyChannel, " ") {
		return entities.ErrInvalidWhitespace
	}
	if strings.Contains(res.ReplyChannel, "*") || strings.Contains(res.ReplyChannel, ">") {
		return entities.ErrInvalidWildcards
	}
	if res.RequestID == "" {
		return entities.ErrInvalidRequestID
	}

	return nil
}

func ValidateSubscriptionToRequests(subReq *pb.Subscribe) error {

	if subReq.ClientID == "" {
		return entities.ErrInvalidClientID
	}
	if subReq.Channel == "" {
		return entities.ErrInvalidChannel
	}
	if strings.HasSuffix(subReq.Channel, ".") {
		return entities.ErrInvalidEndChannelSeparator
	}
	if strings.Contains(subReq.Channel, " ") {
		return entities.ErrInvalidWhitespace
	}
	return nil
}
func ValidatePollRequest(subRequest *pb.QueuesDownstreamRequest) error {

	if subRequest.ClientID == "" {
		return entities.ErrInvalidClientID
	}
	if subRequest.Channel == "" {
		return entities.ErrInvalidQueueName
	}
	if strings.HasSuffix(subRequest.Channel, ".") {
		return entities.ErrInvalidQueueName
	}
	if strings.Contains(subRequest.Channel, " ") {
		return entities.ErrInvalidQueueName
	}
	if strings.Contains(subRequest.Channel, "*") || strings.Contains(subRequest.Channel, ">") {
		return entities.ErrInvalidQueueName
	}

	return nil
}
