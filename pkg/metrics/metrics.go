package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	AliEventCollectorCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test",
		Help: "test",
	}, []string{"test"})
)

func init() {
	prometheus.MustRegister(AliEventCollectorCounter)
}
