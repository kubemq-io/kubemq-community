package api

import (
	"github.com/dustin/go-humanize"
	"math/big"
	"time"
)

type BaseValuesDTO struct {
	Messages              int64  `json:"messages"`
	MessagesHumanized     string `json:"messagesHumanized"`
	Volume                int64  `json:"volume"`
	VolumeHumanized       string `json:"volumeHumanized"`
	Errors                int64  `json:"errors"`
	ErrorsHumanized       string `json:"errorsHumanized"`
	Waiting               int64  `json:"waiting"`
	WaitingHumanized      string `json:"waitingHumanized"`
	Clients               int64  `json:"clients"`
	ClientsHumanized      string `json:"clientsHumanized"`
	LastActivity          int64  `json:"lastActivity"`
	LastActivityHumanized string `json:"lastActivityHumanized"`
	Expired               int64  `json:"expired"`
	ExpiredHumanized      string `json:"expiredHumanized"`
	Delayed               int64  `json:"delayed"`
	DelayedHumanized      string `json:"delayedHumanized"`
	clientMap             map[string]string
}

// create a new BaseValuesDTO

func NewBaseValuesDTO() *BaseValuesDTO {
	return &BaseValuesDTO{
		clientMap: make(map[string]string),
	}
}

// create a new BaseValuesDTO from a BaseValues

func NewBaseValuesDTOFromBaseValues(base *BaseValues) *BaseValuesDTO {
	return &BaseValuesDTO{
		Messages:              base.Messages,
		MessagesHumanized:     humanize.Comma(base.Messages),
		Volume:                base.Volume,
		VolumeHumanized:       humanize.BigBytes(big.NewInt(base.Volume)),
		Errors:                base.Errors,
		ErrorsHumanized:       humanize.BigComma(big.NewInt(base.Errors)),
		Waiting:               base.Waiting,
		WaitingHumanized:      humanize.BigComma(big.NewInt(base.Waiting)),
		Clients:               base.Clients,
		ClientsHumanized:      humanize.BigComma(big.NewInt(base.Clients)),
		LastActivity:          base.LastSeen,
		LastActivityHumanized: humanize.Time(time.UnixMilli(base.LastSeen)),
		Expired:               base.Expired,
		ExpiredHumanized:      humanize.BigComma(big.NewInt(base.Expired)),
		Delayed:               base.Delayed,
		DelayedHumanized:      humanize.BigComma(big.NewInt(base.Delayed)),
		clientMap:             base.ClientMap,
	}
}
