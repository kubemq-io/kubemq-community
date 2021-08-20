package config

import (
	"github.com/kubemq-io/broker/pkg/pipe"
	"github.com/nats-io/nuid"
	"net"
)

// set Global max size for messagego
var globalMaxBodySizeDefault = 1024 * 1024 * 100

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:0")
	if err != nil {
		return -1, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return -1, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

type BrokerConfig struct {
	Port               int        `json:"-"`
	MaxPayload         int        `json:"maxPayload"`
	WriteDeadline      int        `json:"writeDeadline"`
	MaxConn            int        `json:"maxConn"`
	MonitoringPort     int        `json:"-"`
	MemoryPipe         *pipe.Pipe `json:"-"`
	WriteBufferSize    int        `json:"writeBufferSize"`
	ReadBufferSize     int        `json:"readBufferSize"`
	DiskSyncSeconds    int        `json:"diskSyncSeconds"`
	SliceMaxMessages   int        `json:"sliceMaxMessages"`
	SliceMaxBytes      int64      `json:"sliceMaxBytes"`
	SliceMaxAgeSeconds int        `json:"sliceMaxAgeSeconds"`
	ParallelRecovery   int        `json:"parallelRecovery"`
}

func defaultBrokerConfig() *BrokerConfig {
	bindViperEnv(
		"Broker.MaxPayload",
		"Broker.WriteDeadline",
		"Broker.MaxConn",
		"Broker.WriteBufferSize",
		"Broker.ReadBufferSize",
		"Broker.DiskSyncSeconds",
		"Broker.SliceMaxMessages",
		"Broker.SliceMaxBytes",
		"Broker.SliceMaxAgeSeconds",
		"Broker.ParallelRecovery",
	)
	b := &BrokerConfig{
		Port:               0,
		MaxPayload:         globalMaxBodySizeDefault,
		WriteDeadline:      2000,
		MaxConn:            0,
		MonitoringPort:     0,
		MemoryPipe:         pipe.NewPipe(nuid.Next()),
		WriteBufferSize:    2,
		ReadBufferSize:     2,
		DiskSyncSeconds:    60,
		SliceMaxMessages:   0,
		SliceMaxBytes:      64,
		SliceMaxAgeSeconds: 0,
		ParallelRecovery:   2,
	}
	b.Port, _ = getFreePort()
	b.MonitoringPort, _ = getFreePort()
	return b
}

func (b *BrokerConfig) Validate() error {
	//if err := validatePort(b.Port); err != nil {
	//	return NewConfigurationErrorf("bad broker configuration: %s", err.Error())
	//}
	if err := validatePort(b.MonitoringPort); err != nil {
		return NewConfigurationErrorf("bad broker configuration: %s", err.Error())
	}
	if b.MaxPayload < 0 {
		return NewConfigurationError("bad broker configuration: MaxPayload can not be negative")
	}
	if b.WriteDeadline < 0 {
		return NewConfigurationError("bad broker configuration: WriteDeadline can not be negative")
	}
	if b.MaxConn < 0 {
		return NewConfigurationError("bad broker configuration: MaxConn can not be negative")
	}
	if b.WriteBufferSize < 0 {
		return NewConfigurationError("bad broker configuration: WriteBufferSize can not be negative")
	}
	if b.ReadBufferSize < 0 {
		return NewConfigurationError("bad broker configuration: ReadBufferSize can not be negative")
	}
	if b.DiskSyncSeconds < 0 {
		return NewConfigurationError("bad broker configuration: DiskSyncSeconds can not be negative")
	}
	if b.SliceMaxMessages < 0 {
		return NewConfigurationError("bad broker configuration: SliceMaxMessages can not be negative")
	}
	if b.SliceMaxBytes < 0 {
		return NewConfigurationError("bad broker configuration: SliceMaxBytes can not be negative")
	}
	if b.SliceMaxAgeSeconds < 0 {
		return NewConfigurationError("bad broker configuration: SliceMaxAgeSeconds can not be negative")
	}
	if b.ParallelRecovery <= 0 {
		return NewConfigurationError("bad broker configuration: ParallelRecovery can not less than 1")
	}
	return nil
}

func BrokerName() string {
	return "kubemq"
}
