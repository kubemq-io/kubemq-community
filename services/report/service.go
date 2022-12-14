package report

import (
	"context"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	"net/http"
	"os"
	"time"
)

type Service struct {
	signature *Signature
	client    *resty.Client
}

func NewService() *Service {
	return &Service{
		client: resty.New(),
	}
}

func (s *Service) Init(ctx context.Context) error {
	var err error
	s.signature, err = CreateSignature()
	if err != nil {
		return err
	}
	go s.run(ctx)
	return nil
}
func (s *Service) run(ctx context.Context) {
	reportInterval := time.Minute * 5
	if isDev := os.Getenv("DEV"); isDev == "true" {
		reportInterval = time.Second * 5
	}
	if err := s.sendStart(ctx); err != nil {
		fmt.Printf("failed to send start report, error: %s", err.Error())
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(reportInterval):
			err := s.sendMetrics(ctx)
			if err != nil {
				fmt.Printf("failed to send metrics report, error: %s", err.Error())
				return
			}
		}
	}
}
func (s *Service) sendMetrics(ctx context.Context) error {
	exporter := metrics.GetExporter()
	if exporter == nil {
		return fmt.Errorf("metrics exporter not initialized")
	}
	counters, err := exporter.CountersSummery()
	if err != nil {
		return err
	}
	report := NewReport().
		SetVersion(config.Version).
		SetFingerprint(s.signature.Fingerprint).
		SetVolume(int64(counters.Volume)).
		SetMessages(int64(counters.Messages)).
		SetUpdateCounter(1).
		SetOriginEnv(s.signature.Env).
		SetOriginHost(s.signature.Host)
	return s.report(ctx, report)

}

func (s *Service) sendStart(ctx context.Context) error {
	return s.report(ctx, NewReport().
		SetVersion(config.Version).
		SetFingerprint(s.signature.Fingerprint).
		SetStartCounter(1).
		SetOriginEnv(s.signature.Env).
		SetOriginHost(s.signature.Host))
}

func (s *Service) report(ctx context.Context, report *Report) error {
	baseUrl := "https://api.kubemq.io/usage/report/community"
	if isDev := os.Getenv("DEV"); isDev == "true" {
		baseUrl = "https://dev.kubemq.io/usage/report/community"
	}
	resp, err := s.client.R().SetContext(ctx).SetBody(report).Post(baseUrl)
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("failed to send report, status code: %d", resp.StatusCode())
	}
	return nil
}
