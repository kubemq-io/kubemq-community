package entities

type SubKindType int
type KindType int

const (
	KindTypeBlank KindType = iota
	KindTypeEvent
	KindTypeEventStore
	KindTypeCommand
	KindTypeQuery
	KindTypeQueue
)

var KindNames = map[KindType]string{
	KindTypeBlank:      "none",
	KindTypeEvent:      "Events",
	KindTypeEventStore: "Events Store",
	KindTypeCommand:    "Commands",
	KindTypeQuery:      "Queries",
	KindTypeQueue:      "Queues",
}

const (
	SubKindTypeBlank SubKindType = iota
	SubKindTypeEventSender
	SubKindTypeEventReceiver
	SubKindTypeEventStoreSender
	SubKindTypeEventStoreReceiver
	SubKindTypeCommandSender
	SubKindTypeCommandReceiver
	SubKindTypeQuerySender
	SubKindTypeQueryReceiver
	SubKindTypeQueueSender
	SubKindTypeQueueReceiver
)

var SubKindNames = map[SubKindType]string{
	SubKindTypeBlank:              "none",
	SubKindTypeEventSender:        "Events Senders",
	SubKindTypeEventReceiver:      "Events Receivers",
	SubKindTypeEventStoreSender:   "Events Store Senders",
	SubKindTypeEventStoreReceiver: "Events Store Receivers",
	SubKindTypeCommandSender:      "Command Senders",
	SubKindTypeCommandReceiver:    "Command Receivers",
	SubKindTypeQuerySender:        "Query Senders",
	SubKindTypeQueryReceiver:      "Query Receivers",
	SubKindTypeQueueSender:        "Queue Senders",
	SubKindTypeQueueReceiver:      "Queue Receivers",
}
