package ali

import (
	"context"

	"watcher4metrics/pkg/common"
)

type SLS struct {
	meta
}

func (s *SLS) Inject(params ...any) common.MetricsGetter {
	return &SLS{meta: newMeta(params...)}
}

func (s *SLS) GetMetrics() error {
	metrics, err := s.op.getMetrics(
		s.client,
		s.namespace,
		newMetricsBuilder(),
	)
	if err != nil {
		return err
	}
	s.metrics = metrics
	return nil
}

func (s *SLS) GetNamespace() string {
	return s.namespace
}

func (s *SLS) Collector() {
	s.op.getMetricLastData(
		s.client,
		s.metrics,
		s.namespace,
		s.push,
		nil,
		nil,
	)
}

func (s *SLS) AsyncMeta(ctx context.Context) {}

func (s *SLS) push(transfer *transferData) {
	for _, point := range transfer.points {
		var (
			project  string
			logstore string
			cg       string
			status   string
			method   string
		)

		if _, ok := point["project"]; ok {
			project = point["project"].(string)
		}

		if _, ok := point["logstore"]; ok {
			logstore = point["logstore"].(string)
		}

		if _, ok := point["consumerGroup"]; ok {
			cg = point["consumerGroup"].(string)
		}

		if _, ok := point["status"]; ok {
			status = point["status"].(string)
		}

		if _, ok := point["method"]; ok {
			method = point["method"].(string)
		}

		series := &common.MetricValue{
			Metric:       common.BuildMetric("sls", transfer.metric),
			Endpoint:     project,
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			ValueUntyped: point.Value(),
			TagsMap: map[string]string{
				"project":       project,
				"logstore":      logstore,
				"status":        status,
				"method":        method,
				"consumerGroup": cg,
			},
		}
		series.BuildAndShift()
	}
}

func init() {
	registers[ACS_SLS_DASHBOARD] = new(SLS)
}
