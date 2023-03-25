package google

import (
	"context"

	"watcher4metrics/pkg/common"
)

type InterConnect struct {
	meta
}

func (i *InterConnect) Inject(params ...any) common.MetricsGetter {
	return &InterConnect{meta: newMeta(params...)}
}

func (i *InterConnect) GetMetrics() error {
	metrics, err := i.op.getMetrics(i.client, "interconnect.googleapis.com")
	if err != nil {
		return err
	}
	i.metrics = metrics
	return nil
}

func (i *InterConnect) GetNamespace() string {
	return i.namespace
}

func (i *InterConnect) Collector() {
	i.op.listTimeSeries(
		i.client,
		i.metrics,
		5,
		i.push,
		nil,
	)
}

func (i *InterConnect) push(transfer *transferData) {
	transfer.m.Lock()
	defer transfer.m.Unlock()

	for _, series := range transfer.points {
		metricName := i.op.buildMetric(series.Metric.Type)
		points := series.GetPoints()
		if points == nil {
			return
		}

		point := points[len(points)-1]
		ts := point.Interval.EndTime.GetSeconds()
		value := i.op.getPointValue(series.GetValueType(), point)

		resourceLabels := series.Resource.Labels

		series := &common.MetricValue{
			Metric:       common.BuildMetric("interconnect", metricName),
			Endpoint:     resourceLabels["attachment"],
			Timestamp:    ts,
			ValueUntyped: value,
		}
		tagsMap := map[string]string{
			"metric_kind":  transfer.metric.MetricKind.String(),
			"value_type":   transfer.metric.ValueType.String(),
			"unit":         transfer.metric.Unit,
			"launch_stage": transfer.metric.LaunchStage.String(),

			"provider":  ProviderName,
			"iden":      i.op.req.Iden,
			"namespace": i.namespace,
		}

		for k, v := range resourceLabels {
			tagsMap[k] = v
		}

		series.BuildAndShift(tagsMap)
	}
}

func (i *InterConnect) AsyncMeta(context.Context) {}

func init() {
	registers[GOOGLE_INTERCONNECT] = new(InterConnect)
}
