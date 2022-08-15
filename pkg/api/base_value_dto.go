package api

import (
	"github.com/dustin/go-humanize"
	"math/big"
	"time"
)

type BaseValuesDTO struct {
	Messages              int64  `json:"messages"`
	MessagesHumanized     string `json:"messages_humanized"`
	Volume                int64  `json:"volume"`
	VolumeHumanized       string `json:"volume_humanized"`
	Errors                int64  `json:"errors"`
	ErrorsHumanized       string `json:"errors_humanized"`
	Waiting               int64  `json:"waiting"`
	WaitingHumanized      string `json:"waiting_humanized"`
	Clients               int64  `json:"clients"`
	ClientsHumanized      string `json:"clients_humanized"`
	LastActivity          int64  `json:"last_activity"`
	LastActivityHumanized string `json:"last_activity_humanized"`
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
		clientMap:             base.ClientMap,
	}
}
