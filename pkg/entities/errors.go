package entities

import "github.com/pkg/errors"

var (
	ErrInvalidClientID              = errors.New("Error 101: invalid clientID value, cannot be empty")
	ErrInvalidChannel               = errors.New("Error 102: invalid channel value, cannot be empty")
	ErrInvalidObjectCasting         = errors.New("Error 104: invalid object casting")
	ErrUnmarshalUnknownType         = errors.New("Error 105: error on unmarshal of unknown type")
	ErrMarshalUnknownType           = errors.New("Error 106: error on marshal of unknown type")
	ErrInvalidWildcards             = errors.New("Error 107: invalid channel value, no wildcards are allowed")
	ErrInvalidWhitespace            = errors.New("Error 108: invalid channel value, no white spaces are allowed")
	ErrInvalidSetTimeout            = errors.New("Error 109: invalid set timeout value, cannot be 0 or negative")
	ErrMessageEmpty                 = errors.New("Error 110: invalid message, cannot be empty")
	ErrInvalidSubscriptionType      = errors.New("Error 111: invalid subscription type, cannot be undefined or 0")
	ErrInvalidStartSequenceValue    = errors.New("Error 112: invalid subscription type start at sequence value, cannot by 0 or negative")
	ErrInvalidStartAtTimeValue      = errors.New("Error 113: invalid subscription type start at time value, cannot by 0 or negative")
	ErrInvalidStartAtTimeDeltaValue = errors.New("Error 114: invalid subscription type start at time delta value, cannot by 0 or negative")
	ErrRequestEmpty                 = errors.New("Error 115: invalid request, cannot be empty")
	ErrInvalidCacheTTL              = errors.New("Error 116: invalid cache ttl value, cannot be 0 or negative")
	ErrInvalidRequestID             = errors.New("Error 117: invalid request id, cannot be empty")
	ErrInvalidEventStoreType        = errors.New("Error 118: event store parameters were set but subscribe type is not event store")
	ErrInvalidEndChannelSeparator   = errors.New("Error 119: invalid channel value, cannot end with `.`")
	ErrInvalidQueueName             = errors.New("Error 120: invalid queue name value, cannot be empty, contains wildcards ,white spaces and end with dot")
	ErrRegisterQueueSubscription    = errors.New("Error 121: cannot subscribe to a queue")
	ErrInvalidMaxMessages           = errors.New("Error 122: max messages should be between 1 and max message setting in kubemq config  ")
	ErrAckQueueMsg                  = errors.New("Error 123: cannot ack the current message")
	ErrNoCurrentMsgToAck            = errors.New("Error 124: no active message to ack")
	ErrInvalidAckSeq                = errors.New("Error 125: ack sequence cannot be 0 or negative")
	ErrNoCurrentMsgToSend           = errors.New("Error 126: no active message to send")
	ErrInvalidVisibility            = errors.New("Error 127: invalid visibility time, cannot be negative or exceed setting in kubemq config")
	ErrSubscriptionIsActive         = errors.New("Error 128: subscription to queue is already active")
	ErrVisibilityExpired            = errors.New("Error 129: current visibility timer expired")
	ErrSendingQueueMessage          = errors.New("Error 130: error during send message to queue")
	ErrInvalidQueueMessage          = errors.New("Error 131: invalid queue message")
	ErrInvalidStreamRequestType     = errors.New("Error 132: invalid stream queue messages request type")
	ErrInvalidExpiration            = errors.New("Error 133: invalid expiration seconds, exceed max expiration seconds setting in kubemq config")
	ErrInvalidMaxReceiveCount       = errors.New("Error 134: invalid max received count, cannot be negative or exceed max receive count setting in kubemq config")
	ErrInvalidDelay                 = errors.New("Error 135: invalid delay seconds, exceed max delay seconds setting in kubemq config")
	ErrInvalidWaitTimeout           = errors.New("Error 136: invalid wait timeout, value cannot be negative or exceed max wait timeout seconds setting in kubemq config")
	ErrNoActiveMessageToReject      = errors.New("Error 137: no active message to reject")
	ErrNoNewMessageQueue            = errors.New("Error 138: no new message in queue, wait time expired")
)

//From Connectors
var (
	ErrGRPCSServerCrushed          = errors.New("Error 200: server crushed and recovered")
	ErrLoadTLSCertificate          = errors.New("Error 201: error on loading TLS keys")
	ErrMarshalError                = errors.New("Error 202: marshal object error")
	ErrOnWriteToWebSocket          = errors.New("Error 203: error on write to web-socket")
	ErrInvalidWebSocketMessageType = errors.New("Error 204: invalid web-socket data type received, must be 1")
	ErrConnectionClosed            = errors.New("Error 205: connection was closed by server")
	ErrInvalidRequestType          = errors.New("Error 206: invalid request type")
	ErrInvalidSubscribeType        = errors.New("Error 207: invalid subscribe type")
)

// pkg
var (
	ErrRequestTimeout         = errors.New("Error 301: timeout for request message")
	ErrConnectionNoAvailable  = errors.New("Error 302: connection not available, please retry")
	ErrInvalidResponseFormat  = errors.New("Error 303: invalid response format")
	ErrInvalidTransportObject = errors.New("Error 304: invalid transport object")
	ErrInvalidKind            = errors.New("Error 305: invalid kind, should be events, events_store, commands or pb.Subscribe_Queries")
	ErrInvalidBodySize        = errors.New("Error 306: invalid msg body size length, cannot by negative")
)

// from services

var (
	ErrPersistenceServerNotReady = errors.New("Error 402: server persistence server is not ready")
	ErrServerIsNotReady          = errors.New("Error 405: server server is not ready")
	ErrOnLoadingService          = errors.New("Error 406: error on loading service")
	ErrShutdownMode              = errors.New("Error 409: server is in shutdown mode")

	ErrNoAccessResource             = errors.New("Error 412: access denied to resource")
	ErrInvalidAuthorizationProvider = errors.New("Error 413: invalid or no authorization provider found")

	ErrNoAccessNoEnforcer    = errors.New("Error 414: access denied, no enforcer to validate")
	ErrNoAccessInvalidParams = errors.New("Error 415: access denied, invalid access parameters")
	ErrAuthInvalidAuthToken  = errors.New("Error 421: authentication verification failed, ")
	ErrAuthInvalidKeyFile    = errors.New("Error 423: authentication configuration error, error loading key file, ")
	ErrAuthInvalidNoKey      = errors.New("Error 424: authentication configuration error, no verify key provided")
)
