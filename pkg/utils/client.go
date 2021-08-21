package utils

import (
	"context"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/uuid"
	"github.com/kubemq-io/kubemq-go"
	"time"
)

func GetKubeMQClient(ctx context.Context, cfg *config.Config) (*kubemq.Client, error) {
	clientID := cfg.Client.ClientID
	if clientID == "" {
		clientID = uuid.New()
	}
	return kubemq.NewClient(ctx,
		kubemq.WithAddress(cfg.Client.GrpcHost, cfg.Client.GrpcPort),
		kubemq.WithClientId(clientID),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithAutoReconnect(true),
		kubemq.WithReconnectInterval(time.Second),
		kubemq.WithMaxReconnects(0),
		kubemq.WithAuthToken(cfg.Client.AuthToken))
}
