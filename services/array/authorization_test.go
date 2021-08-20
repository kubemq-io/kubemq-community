package array

import (
	"context"
	"fmt"
	"github.com/fortytw2/leaktest"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"github.com/kubemq-io/kubemq-community/services/authorization"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const testAuthorizePolicy = `[{"ClientID":"authorise","Events":true,"Channel":".*","Read":true,"Write":true},
{"ClientID":"authorise","EventsStore":true,"Channel":".*","Read":true,"Write":true},
{"ClientID":"authorise","Commands":true,"Channel":".*","Read":true,"Write":true},
{"ClientID":"authorise","Queries":true,"Channel":".*","Read":true,"Write":true},
{"ClientID":"authorise","Queues":true,"Channel":".*","Read":true,"Write":true}]
`

var (
	authorizedEvent = &pb.Event{
		EventID:  "",
		ClientID: "authorise",
		Channel:  "authorise",
		Metadata: "some-meta-data",
	}
	unAuthorizedEvent = &pb.Event{
		EventID:  "",
		ClientID: "bad-client",
		Channel:  "authorise",
		Metadata: "some-meta-data",
	}
	authorizedEventStore = &pb.Event{
		EventID:  "",
		ClientID: "authorise",
		Channel:  "authorise",
		Metadata: "some-meta-data",
	}
	unAuthorizedEventStore = &pb.Event{
		EventID:  "",
		ClientID: "bad-client",
		Channel:  "authorise",
		Metadata: "some-meta-data",
	}
	authorizedCommand = &pb.Request{

		RequestTypeData: 1,
		ClientID:        "authorise",
		Channel:         "authorise",
		Metadata:        "some-meta-data",
	}
	unAuthorizedCommand = &pb.Request{
		RequestTypeData: 1,
		ClientID:        "bad-client",
		Channel:         "authorise",
		Metadata:        "some-meta-data",
	}
	authorizedQuery = &pb.Request{

		RequestTypeData: 2,
		ClientID:        "authorise",
		Channel:         "authorise",
		Metadata:        "some-meta-data",
	}
	unAuthorizedQuery = &pb.Request{
		RequestTypeData: 2,
		ClientID:        "bad-client",
		Channel:         "authorise",
		Metadata:        "some-meta-data",
	}
	authorizedQueue = &pb.QueueMessage{
		ClientID: "authorise",
		Channel:  "authorise",
		Metadata: "some-meta-data",
	}
	unAuthorizedQueue = &pb.QueueMessage{
		ClientID: "bad-client",
		Channel:  "authorise",
		Metadata: "some-meta-data",
	}
	authorizedAckAll = &pb.AckAllQueueMessagesRequest{
		ClientID: "authorise",
		Channel:  "authorise",
	}
	unAuthorizedAckAll = &pb.AckAllQueueMessagesRequest{
		ClientID: "bad-client",
		Channel:  "authorise",
	}
	authorizedSubEvent = &pb.Subscribe{
		SubscribeTypeData: 1,
		ClientID:          "authorise",
		Channel:           "authorise",
		Group:             "",
	}
	unAuthorizedSubEvent = &pb.Subscribe{
		SubscribeTypeData: 1,
		ClientID:          "bad-client",
		Channel:           "authorise",
	}
	authorizedSubEventStore = &pb.Subscribe{
		SubscribeTypeData:    2,
		ClientID:             "authorise",
		Channel:              "authorise",
		Group:                "",
		EventsStoreTypeData:  1,
		EventsStoreTypeValue: 0,
	}
	unAuthorizedSubEventStore = &pb.Subscribe{
		SubscribeTypeData:    2,
		ClientID:             "bad-client",
		Channel:              "authorise",
		EventsStoreTypeData:  1,
		EventsStoreTypeValue: 0,
	}
	authorizedSubCommand = &pb.Subscribe{
		SubscribeTypeData: 3,
		ClientID:          "authorise",
		Channel:           "authorise",
		Group:             "",
	}
	unAuthorizedSubCommand = &pb.Subscribe{
		SubscribeTypeData: 3,
		ClientID:          "bad-client",
		Channel:           "authorise",
	}
	authorizedSubQuery = &pb.Subscribe{
		SubscribeTypeData: 4,
		ClientID:          "authorise",
		Channel:           "authorise",
		Group:             "",
	}
	unAuthorizedSubQuery = &pb.Subscribe{
		SubscribeTypeData: 4,
		ClientID:          "bad-client",
		Channel:           "authorise",
	}
	authorizedSubQueue = &pb.ReceiveQueueMessagesRequest{
		ClientID: "authorise",
		Channel:  "authorise",
	}
	unAuthorizedSubQueue = &pb.ReceiveQueueMessagesRequest{
		ClientID: "bad-client",
		Channel:  "authorise",
	}
	authorizedStreamQueue = &pb.StreamQueueMessagesRequest{

		ClientID:              "authorise",
		StreamRequestTypeData: 1,
		Channel:               "authorise",
		VisibilitySeconds:     1,
		WaitTimeSeconds:       1,
		RefSequence:           0,
		ModifiedMessage:       nil,
		XXX_NoUnkeyedLiteral:  struct{}{},
		XXX_sizecache:         0,
	}
	unAuthorizedStreamQueue = &pb.StreamQueueMessagesRequest{
		ClientID:              "bad-client",
		StreamRequestTypeData: 1,
		Channel:               "authorise",
		VisibilitySeconds:     1,
		WaitTimeSeconds:       1,
		RefSequence:           0,
		ModifiedMessage:       nil,
		XXX_NoUnkeyedLiteral:  struct{}{},
		XXX_sizecache:         0,
	}
)

func TestArray_Authorize(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	appConfig := getAppConfig()
	authConfig := &config.AuthorizationConfig{
		Enable:     true,
		PolicyData: testAuthorizePolicy,
		FilePath:   "",
		Url:        "",
		AutoReload: 0,
	}

	appConfig.Authorization = authConfig
	na, ns := setup(ctx, t, appConfig)
	defer tearDown(na, ns)
	var err error
	na.authorizationService, err = authorization.CreateAuthorizationService(ctx, appConfig)
	require.NoError(t, err)
	//Grant Event
	_, err = na.SendEvents(ctx, authorizedEvent)
	require.NoError(t, err)
	//Deny Event
	_, err = na.SendEvents(ctx, unAuthorizedEvent)
	require.Error(t, err)

	//Grant Event Store
	_, err = na.SendEventsStore(ctx, authorizedEventStore)
	require.NoError(t, err)
	//Deny Event Store
	_, err = na.SendEventsStore(ctx, unAuthorizedEventStore)
	require.Error(t, err)

	//Grant Commands
	_, err = na.SendCommand(ctx, authorizedCommand)
	require.EqualValues(t, err, entities.ErrInvalidSetTimeout)
	//Deny Commands
	_, err = na.SendCommand(ctx, unAuthorizedCommand)
	require.Error(t, err)

	//Grant pb.Subscribe_Queries
	_, err = na.SendQuery(ctx, authorizedQuery)
	require.EqualValues(t, err, entities.ErrInvalidSetTimeout)
	//Deny pb.Subscribe_Queries
	_, err = na.SendQuery(ctx, unAuthorizedQuery)
	require.Error(t, err)

	//Grant Queue
	_, err = na.SendQueueMessage(ctx, authorizedQueue)
	require.NoError(t, err)
	//Deny Queue
	_, err = na.SendQueueMessage(ctx, unAuthorizedQueue)
	require.Error(t, err)

	//Grant QueueBatch
	_, err = na.SendQueueMessagesBatch(ctx, &pb.QueueMessagesBatchRequest{
		BatchID:  "",
		Messages: []*pb.QueueMessage{authorizedQueue},
	})
	require.NoError(t, err)
	//Deny QueueBatch
	_, err = na.SendQueueMessagesBatch(ctx, &pb.QueueMessagesBatchRequest{
		BatchID:  "",
		Messages: []*pb.QueueMessage{unAuthorizedQueue},
	})
	require.Error(t, err)

	//Grant Queue Ack All
	_, err = na.AckAllQueueMessages(ctx, authorizedAckAll)
	require.NoError(t, err)

	//Deny Queue Ack All
	_, err = na.AckAllQueueMessages(ctx, unAuthorizedAckAll)
	require.Error(t, err)

	//Grant SubEvent
	id, err := na.SubscribeEvents(ctx, authorizedSubEvent, make(chan *pb.EventReceive, 1), make(chan error, 1))
	require.NoError(t, err)
	_ = na.DeleteClient(id)

	//Deny SubEvent
	_, err = na.SubscribeEvents(ctx, unAuthorizedSubEvent, make(chan *pb.EventReceive, 1), make(chan error, 1))
	require.Error(t, err)

	//Grant SubEventStore
	id, err = na.SubscribeEventsStore(ctx, authorizedSubEventStore, make(chan *pb.EventReceive, 1), make(chan error, 1))
	require.NoError(t, err)
	_ = na.DeleteClient(id)

	//Deny SubEventStore
	_, err = na.SubscribeEventsStore(ctx, unAuthorizedSubEventStore, make(chan *pb.EventReceive, 1), make(chan error, 1))
	require.Error(t, err)

	//Grant SubCommand
	id, err = na.SubscribeToCommands(ctx, authorizedSubCommand, make(chan *pb.Request, 1), make(chan error, 1))
	require.NoError(t, err)
	_ = na.DeleteClient(id)

	//Deny SubEventStore
	_, err = na.SubscribeToCommands(ctx, unAuthorizedSubCommand, make(chan *pb.Request, 1), make(chan error, 1))
	require.Error(t, err)

	//Grant SubQueries
	id, err = na.SubscribeToQueries(ctx, authorizedSubQuery, make(chan *pb.Request, 1), make(chan error, 1))
	require.NoError(t, err)
	_ = na.DeleteClient(id)

	//Deny SubQueries
	_, err = na.SubscribeToQueries(ctx, unAuthorizedSubQuery, make(chan *pb.Request, 1), make(chan error, 1))
	require.Error(t, err)

	//Grant Sub Queue
	_, err = na.ReceiveQueueMessages(ctx, authorizedSubQueue)
	require.NoError(t, err)

	//Deny Sub Queue
	_, err = na.ReceiveQueueMessages(ctx, unAuthorizedSubQueue)
	require.Error(t, err)

	//Grant Sub Queue
	streamReqCh := make(chan *pb.StreamQueueMessagesRequest, 1)
	streamResCh := make(chan *pb.StreamQueueMessagesResponse, 1)
	doneCh := make(chan bool, 1)
	id2, err := na.StreamQueueMessage(ctx, streamReqCh, streamResCh, doneCh)
	require.NoError(t, err)
	streamReqCh <- authorizedStreamQueue
	select {
	case res := <-streamResCh:
		require.False(t, res.IsError)
	case <-time.After(time.Second):

	}
	<-doneCh
	_ = na.DeleteClient(id2)

	streamReqCh = make(chan *pb.StreamQueueMessagesRequest, 1)
	streamResCh = make(chan *pb.StreamQueueMessagesResponse, 1)
	doneCh = make(chan bool, 1)

	id3, err := na.StreamQueueMessage(ctx, streamReqCh, streamResCh, doneCh)
	require.NoError(t, err)
	streamReqCh <- unAuthorizedStreamQueue
	select {
	case res := <-streamResCh:
		require.True(t, res.IsError)
	case <-time.After(2 * time.Second):
		require.NoError(t, fmt.Errorf("timeout"))
	}

	<-doneCh
	_ = na.DeleteClient(id3)
}
