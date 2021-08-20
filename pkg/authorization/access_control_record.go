package authorization

import (
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
	"regexp"
	"strings"
)

type AccessControlRecord struct {
	Resource string
	ClientID string
	Channel  string
	Read     bool
	Write    bool
}

func NewAccessControlRecord(obj interface{}) *AccessControlRecord {
	cr := &AccessControlRecord{
		ClientID: "",
		Resource: "",
		Channel:  "",
		Read:     false,
		Write:    false,
	}
	switch v := obj.(type) {
	case *pb.Event:
		cr.ClientID = v.ClientID
		cr.Write = true
		cr.Channel = v.Channel
		if v.Store {
			cr.Resource = "events_store"

		} else {
			cr.Resource = "events"
		}
	case *pb.Subscribe:
		cr.ClientID = v.ClientID
		cr.Read = true
		cr.Channel = v.Channel
		switch v.SubscribeTypeData {
		case pb.Subscribe_Events:
			cr.Resource = "events"
		case pb.Subscribe_EventsStore:
			cr.Resource = "events_store"
		case pb.Subscribe_Commands:
			cr.Resource = "commands"
		case pb.Subscribe_Queries:
			cr.Resource = "queries"
		}
	case *pb.Request:
		cr.ClientID = v.ClientID
		cr.Write = true
		cr.Channel = v.Channel
		switch v.RequestTypeData {
		case pb.Request_Command:
			cr.Resource = "commands"
		case pb.Request_Query:
			cr.Resource = "queries"
		}
	case *pb.Response:
		cr.ClientID = v.ClientID
		cr.Write = true
		cr.Resource = "responses"
		cr.Channel = v.ReplyChannel
	case *pb.QueueMessage:
		cr.ClientID = v.ClientID
		cr.Resource = "queues"
		cr.Write = true
		cr.Channel = v.Channel
	case *pb.ReceiveQueueMessagesRequest:
		cr.ClientID = v.ClientID
		cr.Resource = "queues"
		cr.Read = true
		cr.Channel = v.Channel
	case *pb.AckAllQueueMessagesRequest:
		cr.ClientID = v.ClientID
		cr.Resource = "queues"
		cr.Write = true
		cr.Channel = v.Channel
	case *pb.StreamQueueMessagesRequest:
		cr.ClientID = v.ClientID
		cr.Resource = "queues"
		cr.Read = true
		cr.Channel = v.Channel
	case *pb.QueuesDownstreamRequest:
		cr.ClientID = v.ClientID
		cr.Resource = "queues"
		cr.Read = true
		cr.Channel = v.Channel
	default:

	}
	return cr
}

func (a *AccessControlRecord) Validate() error {
	var err error
	_, err = regexp.Compile(a.ClientID)
	if err != nil {
		return fmt.Errorf("invalid clinet_id regular expression, error: %s", err.Error())
	}
	_, err = regexp.Compile(a.Channel)
	if err != nil {
		return fmt.Errorf("invalid channel regular expression, error: %s", err.Error())
	}

	if a.Resource != "events" && a.Resource != "events_store" && a.Resource != "commands" && a.Resource != "queries" && a.Resource != "queues" {
		return fmt.Errorf("invalid resource, should be events or events_store or commands, or pb.Subscribe_Queries or queues")
	}
	return nil
}

func (a *AccessControlRecord) Rule() string {
	fields := []string{}
	fields = append(fields, "p", a.Resource, a.ClientID, a.Channel)
	var action string
	if a.Read && !a.Write {
		action = "read"
	}
	if a.Write && !a.Read {
		action = "write"
	}
	if a.Write && a.Read {
		action = ".*"
	}
	if !a.Write && !a.Read {
		action = "deny"
	}
	fields = append(fields, action)

	return strings.Join(fields, ",")
}
func (a *AccessControlRecord) Parameters() []interface{} {
	fields := []interface{}{}
	fields = append(fields, a.Resource, a.ClientID, a.Channel)

	if a.Read {
		fields = append(fields, "read")
	}

	if a.Write {
		fields = append(fields, "write")
	}

	return fields
}
