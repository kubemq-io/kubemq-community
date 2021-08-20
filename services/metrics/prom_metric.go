package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type promGaugeMetric struct {
	metric *prometheus.GaugeVec
}

func newPromGaugeMetric(subsystem, name, help string, labels ...string) *promGaugeMetric {
	opts := prometheus.GaugeOpts{
		Namespace:   "kubemq",
		Subsystem:   subsystem,
		Name:        name,
		Help:        help,
		ConstLabels: nil,
	}

	c := &promGaugeMetric{}
	c.metric = prometheus.NewGaugeVec(opts, labels)
	return c
}

func (c *promGaugeMetric) add(value float64, labels prometheus.Labels) {
	c.metric.With(labels).Add(value)
}
func (c *promGaugeMetric) set(value float64, labels prometheus.Labels) {
	c.metric.With(labels).Set(value)
}

type promCounterMetric struct {
	metric *prometheus.CounterVec
}

func newPromCounterMetric(subsystem, name, help string, labels ...string) *promCounterMetric {
	opts := prometheus.CounterOpts{
		Namespace:   "kubemq",
		Subsystem:   subsystem,
		Name:        name,
		Help:        help,
		ConstLabels: nil,
	}

	c := &promCounterMetric{}
	c.metric = prometheus.NewCounterVec(opts, labels)
	return c
}

func (c *promCounterMetric) add(value float64, labels prometheus.Labels) {
	if value > 0 {
		c.metric.With(labels).Add(value)
	}

}

func (c *promCounterMetric) inc(labels prometheus.Labels) {
	c.metric.With(labels).Inc()
}
