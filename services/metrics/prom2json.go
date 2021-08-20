package metrics

import (
	"fmt"
	"io"
	"strings"

	"github.com/prometheus/common/expfmt"

	dto "github.com/prometheus/client_model/go"
)

// Family mirrors the MetricFamily proto message.
type Family struct {
	//Time    time.Time
	Name    string        `json:"name"`
	Help    string        `json:"help"`
	Type    string        `json:"type"`
	Metrics []interface{} `json:"metrics,omitempty"` // Either metric or summary.
}

// Metric is for all "single value" metrics, i.e. Counter, Gauge, and Untyped.
type Metric struct {
	Labels      map[string]string `json:"labels,omitempty"`
	TimestampMs string            `json:"timestamp_ms,omitempty"`
	Value       string            `json:"value"`
}

// Summary mirrors the Summary proto message.
type Summary struct {
	Labels      map[string]string `json:"labels,omitempty"`
	TimestampMs string            `json:"timestamp_ms,omitempty"`
	Quantiles   map[string]string `json:"quantiles,omitempty"`
	Count       string            `json:"count"`
	Sum         string            `json:"sum"`
}

// Histogram mirrors the Histogram proto message.
type Histogram struct {
	Labels      map[string]string `json:"labels,omitempty"`
	TimestampMs string            `json:"timestamp_ms,omitempty"`
	Buckets     map[string]string `json:"buckets,omitempty"`
	Count       string            `json:"count"`
	Sum         string            `json:"sum"`
}

// NewFamily consumes a MetricFamily and transforms it to the local Family type.
func NewFamily(dtoMF *dto.MetricFamily) *Family {
	mf := &Family{
		//Time:    time.Now(),
		Name:    dtoMF.GetName(),
		Help:    dtoMF.GetHelp(),
		Type:    dtoMF.GetType().String(),
		Metrics: make([]interface{}, len(dtoMF.Metric)),
	}
	for i, m := range dtoMF.Metric {
		if dtoMF.GetType() == dto.MetricType_SUMMARY {
			mf.Metrics[i] = Summary{
				Labels:      makeLabels(m),
				TimestampMs: makeTimestamp(m),
				Quantiles:   makeQuantiles(m),
				Count:       fmt.Sprint(m.GetSummary().GetSampleCount()),
				Sum:         fmt.Sprint(m.GetSummary().GetSampleSum()),
			}
		} else if dtoMF.GetType() == dto.MetricType_HISTOGRAM {
			mf.Metrics[i] = Histogram{
				Labels:      makeLabels(m),
				TimestampMs: makeTimestamp(m),
				Buckets:     makeBuckets(m),
				Count:       fmt.Sprint(m.GetHistogram().GetSampleCount()),
				Sum:         fmt.Sprint(m.GetHistogram().GetSampleSum()),
			}
		} else {
			mf.Metrics[i] = Metric{
				Labels:      makeLabels(m),
				TimestampMs: makeTimestamp(m),
				Value:       fmt.Sprint(getValue(m)),
			}
		}
	}
	return mf
}

func getValue(m *dto.Metric) float64 {
	if m.Gauge != nil {
		return m.GetGauge().GetValue()
	}
	if m.Counter != nil {
		return m.GetCounter().GetValue()
	}
	if m.Untyped != nil {
		return m.GetUntyped().GetValue()
	}
	return 0.
}

func makeLabels(m *dto.Metric) map[string]string {
	result := map[string]string{}
	for _, lp := range m.Label {
		result[lp.GetName()] = lp.GetValue()
	}
	return result
}

func makeTimestamp(m *dto.Metric) string {
	if m.TimestampMs == nil {
		return ""
	}
	return fmt.Sprint(m.GetTimestampMs())
}

func makeQuantiles(m *dto.Metric) map[string]string {
	result := map[string]string{}
	for _, q := range m.GetSummary().Quantile {
		result[fmt.Sprint(q.GetQuantile())] = fmt.Sprint(q.GetValue())
	}
	return result
}

func makeBuckets(m *dto.Metric) map[string]string {
	result := map[string]string{}
	for _, b := range m.GetHistogram().Bucket {
		result[fmt.Sprint(b.GetUpperBound())] = fmt.Sprint(b.GetCumulativeCount())
	}
	return result
}

// parseReader consumes an io.Reader and pushes it to the MetricFamily
// channel. It returns when all MetricFamilies are parsed and put on the
// channel.
func parseReader(in io.Reader, ch chan<- *dto.MetricFamily) error {
	defer close(ch)
	// We could do further content-type checks here, but the
	// fallback for now will anyway be the text format
	// version 0.0.4, so just go for it and see if it works.
	var parser expfmt.TextParser
	metricFamilies, err := parser.TextToMetricFamilies(in)
	if err != nil {
		return fmt.Errorf("reading text format failed: %v", err)
	}
	for _, mf := range metricFamilies {
		ch <- mf
	}
	return nil
}

func parse(in string) ([]*Family, error) {
	mfChan := make(chan *dto.MetricFamily, 1024)
	reader := strings.NewReader(in)
	go func() {
		if err := parseReader(reader, mfChan); err != nil {
			fmt.Println(err.Error())
		}
	}()
	result := []*Family{}
	for mf := range mfChan {
		result = append(result, NewFamily(mf))
	}
	return result, nil
}
