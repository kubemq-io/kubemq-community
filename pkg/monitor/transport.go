package monitor

import (
	"encoding/json"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	pb "github.com/kubemq-io/protobuf/go"
)

var TransportPool = NewReferenceCountedPool(
	func(counter ReferenceCounter) ReferenceCountable {
		br := new(Transport)
		br.ReferenceCounter = counter
		return br
	}, ResetTransport)

func NewTransport() *Transport {
	//return &Transport{}
	return TransportPool.Get().(*Transport)
}

type TransportBatch struct {
	Item      *Transport
	IsErr     bool
	ClientIds []string
}

type Transport struct {
	ReferenceCounter `sql:"-"`
	ClientID         string  `json:"client_id"`
	Channel          string  `json:"channel"`
	Kind             string  `json:"kind"`
	SubKind          string  `json:"sub_kind"`
	Error            error   `json:"error"`
	Payload          []byte  `json:"payload"`
	BodySize         float32 `json:"payload_size"`
	BodySize2        float32 `json:"body_size_2"`
	CacheKeyExist    bool    `json:"cache_key_exist"`
	CacheHit         bool    `json:"cache_hit"`
	ResponseEmpty    bool    `json:"response_empty"`
	ClientID2        string  `json:"client_id_2"`
	Latency          float64 `json:"latency"`
}

func ResetTransport(i interface{}) error {
	obj, ok := i.(*Transport)
	if !ok {
		return entities.ErrInvalidObjectCasting
	}
	obj.Reset()
	return nil
}

func (t *Transport) Reset() {
	t.ClientID = ""
	t.Channel = ""
	t.Kind = ""
	t.SubKind = ""
	t.Error = nil
	t.Payload = nil
	t.BodySize = 0
	t.ClientID2 = ""
	t.CacheHit = false
	t.ResponseEmpty = false
	t.CacheKeyExist = false
	t.Latency = 0
}
func (t *Transport) Finish() {
	t.DecrementReferenceCount()
}

func NewTransportFromMessage(v *pb.Event) *Transport {
	t := NewTransport()
	t.Channel = v.Channel
	if v.Store {
		t.Kind = "event"
	} else {
		t.Kind = "event_store"
	}

	t.BodySize = float32(len(v.Body)) / 1e3
	return t
}
func NewTransportFromMessageReceived(v *pb.EventReceive) *Transport {
	t := NewTransport()
	t.Channel = v.Channel
	t.Kind = "event_receive"
	t.BodySize = float32(len(v.Body)) / 1e3
	return t
}
func NewTransportFromQueueMessage(v *pb.QueueMessage) *Transport {
	t := NewTransport()
	t.Channel = v.Channel
	t.Kind = "queue"
	t.BodySize = float32(len(v.Body)) / 1e3
	return t
}

func NewTransportFromRequest(v *pb.Request) *Transport {
	t := NewTransport()
	t.Channel = v.Channel
	if v.RequestTypeData == pb.Request_Command {
		t.Kind = "command"
	} else {
		t.Kind = "query"
	}
	t.BodySize = float32(len(v.Body)) / 1e3
	return t
}
func NewTransportFromRequestAndResponse(v1 *pb.Request, v2 *pb.Response) *Transport {
	t := NewTransport()
	t.Channel = v1.Channel
	t.Kind = "request_response"
	t.BodySize = float32(len(v1.Body) / 1e3)
	if v2 == nil {
		t.ResponseEmpty = true
	} else {
		t.BodySize2 = float32(len(v2.Body)) / 1e3
		if v1.CacheKey != "" {
			t.CacheKeyExist = true
			t.CacheHit = v2.CacheHit
		} else {
			t.CacheKeyExist = false
		}
	}

	return t
}
func NewTransportFromResponse(v *pb.Response) *Transport {
	t := NewTransport()
	t.Kind = "response"
	t.BodySize = float32(len(v.Body)) / 1e3
	return t
}

func (t *Transport) SetClient(clientID string) *Transport {
	if clientID == "" {
		t.ClientID = "unknown"
	} else {
		t.ClientID = clientID
	}
	return t
}
func (t *Transport) SetClient2(clientID string) *Transport {
	if clientID == "" {
		t.ClientID2 = "unknown"
	} else {
		t.ClientID2 = clientID
	}
	return t
}

func (t *Transport) SetLatency(value float64) *Transport {
	t.Latency = value
	return t
}

func (t *Transport) SetError(err error) *Transport {
	t.Error = err
	return t
}

func (t *Transport) SetSubKind(v string) *Transport {
	t.SubKind = v
	return t
}

func (t *Transport) Unmarshal(v interface{}) error {
	switch val := v.(type) {
	case *pb.Event:
		return val.Unmarshal(t.Payload)
	case *pb.Result:
		return val.Unmarshal(t.Payload)
	case *pb.EventReceive:
		return val.Unmarshal(t.Payload)
	case *pb.Request:
		return val.Unmarshal(t.Payload)
	case *pb.Response:
		return val.Unmarshal(t.Payload)
	case *ResponseError:
		return json.Unmarshal(t.Payload, val)
	case *pb.QueueMessage:
		return val.Unmarshal(t.Payload)
	}
	return entities.ErrUnmarshalUnknownType
}
func (t *Transport) String() string {
	switch t.Kind {
	case "event_receive":
		msg := &pb.EventReceive{}
		err := t.Unmarshal(msg)
		if err != nil {
			return "error parsing object"
		}
		data, err := json.Marshal(msg)
		if err != nil {
			return "error parsing object"
		}
		return string(data)
	case "command", "query":
		req := &pb.Request{}
		err := t.Unmarshal(req)
		if err != nil {
			return "error parsing object"
		}
		data, err := json.Marshal(req)
		if err != nil {
			return "error parsing object"
		}
		return string(data)
	case "response":
		res := &pb.Response{}
		err := t.Unmarshal(res)
		if err != nil {
			return "error parsing object"
		}
		data, err := json.Marshal(res)
		if err != nil {
			return "error parsing object"
		}
		return string(data)
	case "response_error":
		res := &ResponseError{}
		err := t.Unmarshal(res)
		if err != nil {
			return "error parsing object"
		}
		data, err := json.Marshal(res)
		if err != nil {
			return "error parsing object"
		}
		return string(data)
	case "queue":
		qm := &pb.QueueMessage{}
		err := t.Unmarshal(qm)
		if err != nil {
			return "error parsing object"
		}
		data, err := json.Marshal(qm)
		if err != nil {
			return "error parsing object"
		}
		return string(data)
	default:
		return entities.ErrInvalidTransportObject.Error()

	}

}
func (t *Transport) marshal(v interface{}) ([]byte, error) {
	var buffer []byte
	switch val := v.(type) {
	case *pb.Event:
		buffer, _ = val.Marshal()
	case *pb.Result:
		buffer, _ = val.Marshal()
	case *pb.EventReceive:
		buffer, _ = val.Marshal()
	case *pb.Request:
		buffer, _ = val.Marshal()
	case *pb.Response:
		buffer, _ = val.Marshal()
	case *ResponseError:
		buffer, _ = json.Marshal(val)
	case *pb.QueueMessage:
		buffer, _ = val.Marshal()
	default:
		return nil, entities.ErrMarshalUnknownType
	}
	return buffer, nil
}
func (t *Transport) SetPayload(v interface{}) *Transport {
	var err error
	t.Payload, err = t.marshal(v)
	if err != nil {
		t.Payload = nil
	}
	return t
}
