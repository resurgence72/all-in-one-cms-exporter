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

	CMSRemoteWriteSuccessCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cms_remote_write_success_counter",
		Help: "cms remote write success counter",
	})

	CMSRemoteWriteFailedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cms_remote_write_failed_counter",
		Help: "cms remote write failed counter",
	})
)

func init() {
	prometheus.MustRegister(CMSMetricsTotalCounter)
	prometheus.MustRegister(CMSMetricsDiscardCounter)
	prometheus.MustRegister(CMSRemoteWriteSuccessCounter)
	prometheus.MustRegister(CMSRemoteWriteFailedCounter)
}
