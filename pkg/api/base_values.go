package api

import (
	"github.com/dustin/go-humanize"
	"math/big"
)

type BaseValues struct {
	Messages  int64 `json:"messages"`
	Volume    int64 `json:"volume"`
	Errors    int64 `json:"errors"`
	Waiting   int64 `json:"waiting"`
	Clients   int64 `json:"clients"`
	clientMap map[string]string
}

func NewBaseValues() *BaseValues {
	return &BaseValues{
		Messages:  0,
		Volume:    0,
		Errors:    0,
		Waiting:   0,
		Clients:   0,
		clientMap: make(map[string]string),
	}
}

func (b *BaseValues) SetMessages(value int64) *BaseValues {
	b.Messages = value
	return b
}
func (b *BaseValues) SetVolume(value int64) *BaseValues {
	b.Volume = value
	return b
}

func (b *BaseValues) SetErrors(value int64) *BaseValues {
	b.Errors = value
	return b
}
func (b *BaseValues) SetWaiting(value int64) *BaseValues {
	b.Waiting = value
	return b
}
func (b *BaseValues) AddClient(value string) *BaseValues {
	b.clientMap[value] = value
	b.Clients = int64(len(b.clientMap))
	return b
}
func (b *BaseValues) Diff(other *BaseValues) *BaseValues {
	newBase := NewBaseValues()
	newBase.Messages = b.Messages - other.Messages
	newBase.Volume = b.Volume - other.Volume
	newBase.Errors = b.Errors - other.Errors
	newBase.Waiting = b.Waiting - other.Waiting
	newBase.Clients = b.Clients - other.Clients
	return newBase
}
func (b *BaseValues) IsEqual(other *BaseValues) bool {
	return b.Messages == other.Messages &&
		b.Volume == other.Volume &&
		b.Errors == other.Errors &&
		b.Waiting == other.Waiting &&
		b.Clients == other.Clients
}
func (b *BaseValues) AddValues(value *BaseValues) *BaseValues {
	b.Messages += value.Messages
	b.Volume += value.Volume
	b.Errors += value.Errors
	b.Waiting += value.Waiting
	b.Clients += value.Clients
	return b
}

func (b *BaseValues) MessagesString() string {
	return humanize.Comma(b.Messages)
}
func (b *BaseValues) VolumeString() string {
	return humanize.BigBytes(big.NewInt(b.Volume))
}

func (b *BaseValues) ErrorsString() string {
	return humanize.Comma(b.Errors)
}
func (b *BaseValues) WaitingString() string {
	return humanize.Comma(b.Waiting)
}
func (b *BaseValues) ClientsString() string {
	return humanize.Comma(b.Clients)
}
