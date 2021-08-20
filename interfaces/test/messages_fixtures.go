package test

import (
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	pb "github.com/kubemq-io/protobuf/go"
)

var SendEventsErrorFixtures = []struct {
	Name              string
	Msg               *pb.Event
	ExpectedErrorText string
}{
	{
		Name: "invalid_channel_id",
		Msg: &pb.Event{
			EventID:  "",
			ClientID: "some-client-id",
			Channel:  "",
			Metadata: "some-metadata",
			Body:     []byte("some-body"),
			Store:    false,
		},
		ExpectedErrorText: entities.ErrInvalidChannel.Error(),
	},
	{
		Name: "invalid_channel_string",
		Msg: &pb.Event{
			EventID:  "",
			ClientID: "some-client-id",
			Channel:  "some-channel-with_*_and_>",
			Metadata: "some-metadata",
			Body:     []byte("some-body"),
			Store:    false,
		},
		ExpectedErrorText: entities.ErrInvalidWildcards.Error(),
	},
	{
		Name: "invalid_channel_with_whitespaces",
		Msg: &pb.Event{
			EventID:  "",
			ClientID: "some-client-id",
			Channel:  "some-channel with whitespaces",
			Metadata: "some-metadata",
			Body:     []byte("some-body"),
			Store:    false,
		},
		ExpectedErrorText: entities.ErrInvalidWhitespace.Error(),
	},
	{
		Name: "invalid_client_id",
		Msg: &pb.Event{
			EventID:  "",
			ClientID: "",
			Channel:  "some-client-id",
			Metadata: "some-metadata",
			Body:     []byte("some-body"),
			Store:    false,
		},
		ExpectedErrorText: entities.ErrInvalidClientID.Error(),
	},
	{
		Name: "invalid_message_empty",
		Msg: &pb.Event{
			EventID:  "",
			ClientID: "some-client-id",
			Channel:  "some-channel",
			Metadata: "",
			Body:     nil,
		},
		ExpectedErrorText: entities.ErrMessageEmpty.Error(),
	},
}

var SendEventsStoreReportFixtures = []struct {
	Name             string
	Msg              *pb.Event
	IsMessageIDExist bool
}{
	{
		Name: "send_to_non_queue",
		Msg: &pb.Event{
			EventID:  "",
			ClientID: "some-client-id",
			Channel:  "some-channel",
			Metadata: "some-metadata",
			Body:     []byte("some-body"),
			Store:    false,
		},
	},
	{
		Name: "send_to_non_persistence_with_message_id",
		Msg: &pb.Event{
			EventID:  "some-message-id",
			ClientID: "some-client-id",
			Channel:  "some-channel",
			Metadata: "some-metadata",
			Body:     []byte("some-body"),
			Store:    false,
		},
		IsMessageIDExist: true,
	},
	{
		Name: "send_to_store",
		Msg: &pb.Event{
			ClientID: "some-client-id",
			Channel:  "some-channel",
			Metadata: "some-metadata",
			Body:     []byte("some-body"),
			Store:    true,
		},
	},
}

var SendEventSubscribeMessagesFixtures = []struct {
	Name                string
	Msg                 *pb.Event
	SubRequest          *pb.Subscribe
	ExpectedResult      *pb.Result
	ExpectedMsgReceived *pb.EventReceive
}{
	{
		Name: "send_without_timeout_with_delvery_report",
		Msg: &pb.Event{
			EventID:              "msgid-1",
			ClientID:             "sender-1",
			Channel:              "SendMessagesSubscribeMessages.1",
			Metadata:             "some-metadata",
			Body:                 []byte("some-body"),
			Store:                false,
			Tags:                 map[string]string{"tag1": "tag1"},
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
		SubRequest: &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Events,
			ClientID:             "receiver-1",
			Channel:              "SendMessagesSubscribeMessages.1",
			Group:                "",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
		},
		ExpectedResult: &pb.Result{
			EventID: "msgid-1",
			Sent:    true,
			Error:   "",
		},
		ExpectedMsgReceived: &pb.EventReceive{
			EventID:   "msgid-1",
			Channel:   "SendMessagesSubscribeMessages.1",
			Metadata:  "some-metadata",
			Body:      []byte("some-body"),
			Tags:      map[string]string{"tag1": "tag1"},
			Timestamp: 0,
			Sequence:  0,
		},
	},

	{
		Name: "send_without_timeout_with_group",
		Msg: &pb.Event{
			EventID:  "msgid-1",
			ClientID: "sender-1",
			Channel:  "SendMessagesSubscribeMessages.2",
			Metadata: "some-metadata",
			Body:     []byte("some-body"),
			Store:    false,
			Tags:     map[string]string{"tag1": "tag1"},
		},
		SubRequest: &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Events,
			ClientID:             "receiver-1",
			Channel:              "SendMessagesSubscribeMessages.2",
			Group:                "q1",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
		ExpectedResult: &pb.Result{
			EventID: "msgid-1",
			Sent:    true,
			Error:   "",
		},
		ExpectedMsgReceived: &pb.EventReceive{
			EventID:              "msgid-1",
			Channel:              "SendMessagesSubscribeMessages.2",
			Metadata:             "some-metadata",
			Body:                 []byte("some-body"),
			Timestamp:            0,
			Sequence:             0,
			Tags:                 map[string]string{"tag1": "tag1"},
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
	},
	{
		Name: "send_without_timeout_and_channel_wildcard",
		Msg: &pb.Event{
			EventID:              "msgid-1",
			ClientID:             "sender-1",
			Channel:              "SendMessagesSubscribeMessages.wildcard1.1",
			Metadata:             "some-metadata",
			Body:                 []byte("some-body"),
			Store:                false,
			Tags:                 map[string]string{"tag1": "tag1"},
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
		SubRequest: &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Events,
			ClientID:             "receiver-1",
			Channel:              "SendMessagesSubscribeMessages.wildcard1.*",
			Group:                "q1",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
		ExpectedResult: &pb.Result{
			EventID:              "msgid-1",
			Sent:                 true,
			Error:                "",
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
		ExpectedMsgReceived: &pb.EventReceive{
			EventID:              "msgid-1",
			Channel:              "SendMessagesSubscribeMessages.wildcard1.1",
			Metadata:             "some-metadata",
			Body:                 []byte("some-body"),
			Timestamp:            0,
			Sequence:             0,
			Tags:                 map[string]string{"tag1": "tag1"},
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
	},
	{
		Name: "send_without_timeout_and_channel_wildcard_2",
		Msg: &pb.Event{
			EventID:              "msgid-1",
			ClientID:             "sender-1",
			Channel:              "SendMessagesSubscribeMessages.sub.sub.sub.1",
			Metadata:             "some-metadata",
			Body:                 []byte("some-body"),
			Store:                false,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
		SubRequest: &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_Events,
			ClientID:             "receiver-1",
			Channel:              "SendMessagesSubscribeMessages.sub.sub.>",
			Group:                "q1",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
		ExpectedResult: &pb.Result{
			EventID:              "msgid-1",
			Sent:                 true,
			Error:                "",
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
		ExpectedMsgReceived: &pb.EventReceive{
			EventID:              "msgid-1",
			Channel:              "SendMessagesSubscribeMessages.sub.sub.sub.1",
			Metadata:             "some-metadata",
			Body:                 []byte("some-body"),
			Timestamp:            0,
			Sequence:             0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
	},
}

var SendMessagesSubscribePersistentFixtures = []struct {
	Name                string
	Msg                 *pb.Event
	SubRequest          *pb.Subscribe
	ExpectedResult      *pb.Result
	ExpectedMsgReceived *pb.EventReceive
}{
	{
		Name: "send_start_from_new_with_delivery_report",
		Msg: &pb.Event{
			EventID:              "msgid-1",
			ClientID:             "sender-1",
			Channel:              "SendMessagesSubscribeMessages.1",
			Metadata:             "some-metadata",
			Body:                 []byte("some-body"),
			Store:                true,
			Tags:                 map[string]string{"tag1": "tag1"},
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
		SubRequest: &pb.Subscribe{
			SubscribeTypeData:    pb.Subscribe_EventsStore,
			ClientID:             "receiver-1",
			Channel:              "SendMessagesSubscribeMessages.1",
			Group:                "",
			EventsStoreTypeData:  0,
			EventsStoreTypeValue: 0,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
		ExpectedResult: &pb.Result{
			EventID:              "msgid-1",
			Sent:                 true,
			Error:                "",
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
		ExpectedMsgReceived: &pb.EventReceive{
			EventID:              "msgid-1",
			Channel:              "SendMessagesSubscribeMessages.1",
			Metadata:             "some-metadata",
			Body:                 []byte("some-body"),
			Timestamp:            0,
			Sequence:             1,
			Tags:                 map[string]string{"tag1": "tag1"},
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_sizecache:        0,
		},
	},
}
