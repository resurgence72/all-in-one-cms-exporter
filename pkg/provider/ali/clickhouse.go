package ali

import (
	"context"

	"watcher4metrics/pkg/common"
)

type ClickHouse struct {
	meta
}

func init() {
	registers[ACS_CLICKHOUSE] = new(ClickHouse)
}

func (c *ClickHouse) Inject(params ...any) common.MetricsGetter {
	return &ClickHouse{meta: newMeta(params...)}
}

func (c *ClickHouse) GetMetrics() error {
	metrics, err := c.op.getMetrics(
		c.client,
		c.namespace,
		nil,
		nil,
	)
	if err != nil {
		return err
	}
	c.metrics = metrics
	return nil
}

func (c *ClickHouse) GetNamespace() string {
	return c.namespace
}

func (c *ClickHouse) Collector() {
	c.op.getMetricLastData(
		c.client,
		c.metrics,
		c.namespace,
		c.push,
		nil,
		[]string{"logic_name"},
	)
}

func (c *ClickHouse) AsyncMeta(ctx context.Context) {}

func (c *ClickHouse) push(transfer *transferData) {
	for _, point := range transfer.points {
		logicName := point["logic_name"].(string)

		series := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("clickhouse", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     logicName,
		}

		series.TagsMap = map[string]string{
			"provider":  ProviderName,
			"iden":      c.op.req.Iden,
			"namespace": c.namespace,
			"unit_name": transfer.unit,
		}

		series.BuildAndShift()
	}
}
