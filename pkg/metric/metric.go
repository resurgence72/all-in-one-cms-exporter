package metric

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	CMSMetricsTotalCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cms_metrics_total_counter",
		Help: "cms metrics total counter",
	}, []string{"provider", "namespace"})

	CMSMetricsDiscardCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cms_metrics_discard_counter",
		Help: "cms metrics discard counter",
	})
)

func init() {
	prometheus.MustRegister(CMSMetricsTotalCounter)
	prometheus.MustRegister(CMSMetricsDiscardCounter)
}
