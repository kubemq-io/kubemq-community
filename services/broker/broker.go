package broker

import (
	"context"
	"fmt"
	"github.com/kubemq-io/broker/client/nats"
	"github.com/kubemq-io/broker/server/stan/stores"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"github.com/kubemq-io/kubemq-community/pkg/logging"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"

	"github.com/kubemq-io/kubemq-community/config"

	natsd "github.com/kubemq-io/broker/server/gnatsd/server"
	snats "github.com/kubemq-io/broker/server/stan/server"
)

var defaultFileStoreOptions = stores.FileStoreOptions{
	BufferSize:            2 * 1024 * 1024, // 2MB
	CompactEnabled:        false,
	CompactInterval:       5 * 60, // 5 minutes
	CompactFragmentation:  50,
	CompactMinFileSize:    1024 * 1024,
	DoCRC:                 false,
	CRCPolynomial:         int64(crc32.IEEE),
	DoSync:                false,
	SliceMaxMsgs:          0,
	SliceMaxBytes:         64 * 1024 * 1024, // 64MB
	SliceMaxAge:           0,
	SliceArchiveScript:    "",
	FileDescriptorsLimit:  0,
	ParallelRecovery:      2,
	TruncateUnexpectedEOF: false,
	ReadBufferSize:        2 * 1024 * 1024, // 2MB
	AutoSync:              1 * time.Minute,
}

func getFileStoreOptions(brokerOpts *config.BrokerConfig) stores.FileStoreOptions {

	opts := defaultFileStoreOptions
	opts.BufferSize = brokerOpts.WriteBufferSize * 1024 * 1024
	opts.ReadBufferSize = brokerOpts.ReadBufferSize * 1024 * 1024
	opts.AutoSync = time.Duration(brokerOpts.DiskSyncSeconds) * time.Second
	opts.SliceMaxMsgs = brokerOpts.SliceMaxMessages
	opts.SliceMaxBytes = brokerOpts.SliceMaxBytes * 1024 * 1024
	opts.SliceMaxAge = time.Duration(brokerOpts.SliceMaxAgeSeconds) * time.Second
	opts.ParallelRecovery = brokerOpts.ParallelRecovery
	return opts
}

func (s *Service) getBrokerOptions(appConfig *config.Config) (natsOpts *natsd.Options, snatsOpts *snats.Options) {

	natsOpts = &natsd.Options{}
	natsOpts.Host = "0.0.0.0"
	natsOpts.Port = appConfig.Broker.Port
	natsOpts.WriteDeadline = time.Duration(appConfig.Broker.WriteDeadline) * time.Millisecond
	natsOpts.MaxConn = appConfig.Broker.MaxConn
	natsOpts.MaxPayload = int32(appConfig.Broker.MaxPayload)
	natsOpts.MaxPending = 200 * 1024 * 1024
	natsOpts.NoSigs = true

	snatsOpts = snats.GetDefaultOptions()
	snatsOpts.CustomLogger = logging.GetLogFactory().NewLogger("broker-store-server")
	snatsOpts.EnableLogging = true

	snatsOpts.ID = "kubemq"
	switch appConfig.Log.Level {
	case config.LogLevelTypeTrace:
		snatsOpts.Debug = true
		snatsOpts.Trace = true
	case config.LogLevelTypeDebug:
		snatsOpts.Debug = true
	}
	snatsOpts.NATSServerURL = fmt.Sprintf("nats://0.0.0.0:%d", appConfig.Broker.Port)
	snatsOpts.NATSClientOpts = append(snatsOpts.NATSClientOpts, nats.SetCustomDialer(appConfig.Broker.MemoryPipe))

	snatsOpts.MaxAge = time.Duration(appConfig.Store.MaxRetention) * time.Minute
	snatsOpts.MaxBytes = appConfig.Store.MaxQueueSize
	snatsOpts.MaxChannels = appConfig.Store.MaxQueues
	snatsOpts.MaxMsgs = appConfig.Store.MaxMessages
	snatsOpts.MaxInactivity = time.Duration(appConfig.Store.MaxPurgeInactive) * time.Minute
	snatsOpts.MaxSubscriptions = appConfig.Store.MaxSubscribers
	snatsOpts.StoreType = stores.TypeFile
	snatsOpts.FilestoreDir = filepath.Join(appConfig.Store.StorePath, appConfig.Host)
	snatsOpts.FileStoreOpts = getFileStoreOptions(appConfig.Broker)
	snatsOpts.ClientHBInterval = 24 * time.Hour
	snatsOpts.ClientHBTimeout = 180 * time.Second
	snatsOpts.ClientHBFailCount = 120

	return

}
func (s *Service) startAsBroker(ctx context.Context, appConfig *config.Config) error {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Fatalf("recovering from panic, %v", r)
		}
	}()
	if appConfig.Store.CleanStore {
		_ = os.RemoveAll(appConfig.Store.StorePath)
	}

	s.NatsOptions, s.SnatsOptions = s.getBrokerOptions(appConfig)
	if s.loadStatus.HasRecoveryErrors() {
		s.logger.Infof("last attempt to load ended with error, removing the store to recover, error: %s", s.loadStatus.LastError)
		err := os.RemoveAll(appConfig.Store.StorePath)
		if err != nil {
			return err
		}
	}
	ns, err := natsd.NewServer(s.NatsOptions)
	if err != nil {
		return err
	}
	if appConfig.Log.Level == "debug" {
		ns.SetLogger(s.logger, true, false)
	} else {
		ns.SetLogger(s.logger, false, false)
	}

	go func() {
		ns.StartWithPipe(s.appConfig.Broker.MemoryPipe)

	}()
	if !ns.ReadyForConnections(readyTimeout) {
		return entities.ErrServerIsNotReady
	}
	ss, err := snats.RunServerWithOpts(s.SnatsOptions, &natsd.Options{
		Port:     appConfig.Broker.Port,
		NoSigs:   true,
		HTTPPort: appConfig.Broker.MonitoringPort,
	})

	if err != nil {
		return err
	}
	s.Nats = ns
	s.Snats = ss

	s.isHealthy.Store(true)
	s.isReady.Store(true)
	go s.runReportWorker(ctx)
	return nil
}
