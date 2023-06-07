package google

import (
	"context"

	"watcher4metrics/pkg/common"
)

type Lb struct {
	meta
}

func init() {
	registers[GOOGLE_LB] = new(Lb)
}

func (l *Lb) Inject(params ...any) common.MetricsGetter { return &Lb{meta: newMeta(params...)} }

func (l *Lb) GetNamespace() string {
	return l.namespace
}

func (l *Lb) GetMetrics() error {
	metrics, err := l.op.getMetrics(l.client, "loadbalancing.googleapis.com")
	if err != nil {
		return err
	}
	l.metrics = metrics
	return nil
}

func (l *Lb) Collector() {
	l.op.listTimeSeries(
		l.client,
		l.metrics,
		5,
		l.push,
		[]string{
			"resource.url_map_name",
			"resource.project_id",
			"metric.response_code",
			"metric.response_code_class",
		},
	)
}

func (l *Lb) push(transfer *transferData) {
	for _, series := range transfer.points {
		po, err := l.op.newPointOperator(series)
		if err != nil {
			continue
		}

		ep, ok := po.resourceLabels["url_map_name"]
		if !ok {
			return
		}

		switch v := po.value.(type) {
		case quantileContainer:
			quantileIdx := []string{"p50", "p90", "p99", "mean"}
			for i, quantile := range v.qs {
				series := &common.MetricValue{
					Metric:       common.BuildMetric("lb", po.metricName),
					Endpoint:     ep,
					Timestamp:    po.ts,
					ValueUntyped: quantile,
					TagsMap: map[string]string{
						"metric_kind":  transfer.metric.MetricKind.String(),
						"value_type":   transfer.metric.ValueType.String(),
						"unit":         transfer.metric.Unit,
						"launch_stage": transfer.metric.LaunchStage.String(),

						"provider":  ProviderName,
						"iden":      l.op.req.Iden,
						"namespace": l.namespace,
					},
				}

				if pid, ok := po.resourceLabels["project_id"]; ok {
					series.TagsMap["project_id"] = pid

					if pname, ok := l.op.projects.Load(pid); ok {
						series.TagsMap["project_mark"] = pname.(string)
					}
				}
				if ccode, ok := po.metricLabels["response_code"]; ok {
					series.TagsMap["response_code"] = ccode
				}
				if cc, ok := po.metricLabels["response_code_class"]; ok {
					series.TagsMap["response_code_class"] = cc
				}

				series.TagsMap["quantile"] = quantileIdx[i]
				series.BuildAndShift()
			}
			continue

		default:
		}

		series := &common.MetricValue{
			Metric:       common.BuildMetric("lb", po.metricName),
			Endpoint:     ep,
			Timestamp:    po.ts,
			ValueUntyped: po.value,
		}

		series.TagsMap = map[string]string{
			"metric_kind":  transfer.metric.MetricKind.String(),
			"value_type":   transfer.metric.ValueType.String(),
			"unit":         transfer.metric.Unit,
			"launch_stage": transfer.metric.LaunchStage.String(),

			"provider":  ProviderName,
			"iden":      l.op.req.Iden,
			"namespace": l.namespace,
		}

		if pid, ok := po.resourceLabels["project_id"]; ok {
			series.TagsMap["project_id"] = pid

			if pname, ok := l.op.projects.Load(pid); ok {
				series.TagsMap["project_mark"] = pname.(string)
			}
		}

		if ccode, ok := po.metricLabels["response_code"]; ok {
			series.TagsMap["response_code"] = ccode
		}

		if cc, ok := po.metricLabels["response_code_class"]; ok {
			series.TagsMap["response_code_class"] = cc
		}

		series.BuildAndShift()
	}
}

func (l *Lb) AsyncMeta(ctx context.Context) {}
