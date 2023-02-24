package ali

import (
	"context"
	"sync"

	"watcher4metrics/pkg/common"

	ck "github.com/aliyun/alibaba-cloud-sdk-go/services/clickhouse"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
)

type ClickHouse struct {
	op        *operator
	namespace string
	metrics   []*cms.Resource
	client    *cms.Client
	// 保存eip的实例id对应的eip对象
	ckMap map[string]*ck.DBCluster

	m sync.RWMutex
}

func init() {
	registers[ACS_CLICKHOUSE] = new(ClickHouse)
}

func (c *ClickHouse) Inject(params ...interface{}) common.MetricsGetter {
	return &ClickHouse{
		op:        params[0].(*operator),
		client:    params[1].(*cms.Client),
		namespace: params[2].(string),
	}
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
		5,
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

		n9e := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("clickhouse", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     logicName,
		}

		tagsMap := map[string]string{
			"provider":  ProviderName,
			"iden":      c.op.req.Iden,
			"namespace": c.namespace,
			"unit_name": transfer.unit,
		}

		n9e.BuildAndShift(tagsMap)
	}
}
