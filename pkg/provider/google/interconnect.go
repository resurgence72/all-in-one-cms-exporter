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
	for _, series := range transfer.points {
		po, err := i.op.newPointOperator(series)
		if err != nil {
			continue
		}

		series := &common.MetricValue{
			Metric:       common.BuildMetric("interconnect", po.metricName),
			Endpoint:     po.resourceLabels["attachment"],
			Timestamp:    po.ts,
			ValueUntyped: po.value,
		}
		series.TagsMap = map[string]string{
			"metric_kind":  transfer.metric.MetricKind.String(),
			"value_type":   transfer.metric.ValueType.String(),
			"unit":         transfer.metric.Unit,
			"launch_stage": transfer.metric.LaunchStage.String(),

			"provider":  ProviderName,
			"iden":      i.op.req.Iden,
			"namespace": i.namespace,
		}

		for k, v := range po.resourceLabels {
			series.TagsMap[k] = v
		}

		series.BuildAndShift()
	}
}

func (i *InterConnect) AsyncMeta(context.Context) {}

func init() {
	registers[GOOGLE_INTERCONNECT] = new(InterConnect)
}
