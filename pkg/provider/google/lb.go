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
	transfer.m.Lock()
	defer transfer.m.Unlock()

	for _, series := range transfer.points {
		metricName := l.op.buildMetric(series.Metric.Type)

		points := series.GetPoints()
		if len(points) == 0 {
			return
		}

		point := points[len(points)-1]
		ts := point.Interval.EndTime.GetSeconds()
		value := l.op.getPointValue(series.GetValueType(), point)
		
		metricLabels := series.Metric.Labels
		resourceLabels := series.Resource.Labels
		ep, ok := resourceLabels["url_map_name"]
		if !ok {
			return
		}

		quantileIdx := []string{"p50", "p90", "p99", "mean"}
		switch v := value.(type) {
		case quantileContainer:
			for i, quantile := range v.qs {
				series := &common.MetricValue{
					Metric:       common.BuildMetric("lb", metricName),
					Endpoint:     ep,
					Timestamp:    ts,
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

				if pid, ok := resourceLabels["project_id"]; ok {
					series.TagsMap["project_id"] = pid

					if pname, ok := l.op.projects.Load(pid); ok {
						series.TagsMap["project_mark"] = pname.(string)
					}
				}
				if ccode, ok := metricLabels["response_code"]; ok {
					series.TagsMap["response_code"] = ccode
				}
				if cc, ok := metricLabels["response_code_class"]; ok {
					series.TagsMap["response_code_class"] = cc
				}

				series.TagsMap["quantile"] = quantileIdx[i]
				series.BuildAndShift()
			}
			continue

		default:
		}

		series := &common.MetricValue{
			Metric:       common.BuildMetric("lb", metricName),
			Endpoint:     ep,
			Timestamp:    ts,
			ValueUntyped: value,
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

		if pid, ok := resourceLabels["project_id"]; ok {
			series.TagsMap["project_id"] = pid

			if pname, ok := l.op.projects.Load(pid); ok {
				series.TagsMap["project_mark"] = pname.(string)
			}
		}

		if ccode, ok := metricLabels["response_code"]; ok {
			series.TagsMap["response_code"] = ccode
		}

		if cc, ok := metricLabels["response_code_class"]; ok {
			series.TagsMap["response_code_class"] = cc
		}

		series.BuildAndShift()
	}
}

func (l *Lb) AsyncMeta(ctx context.Context) {}
