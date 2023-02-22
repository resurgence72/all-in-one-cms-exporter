package google

import (
	monitoring "cloud.google.com/go/monitoring/apiv3"
	"context"
	"watcher4metrics/pkg/common"
)

type InterConnect struct {
	op        *operator
	client    *monitoring.MetricClient
	namespace string
	metrics   []string
}

func (i *InterConnect) Inject(params ...interface{}) common.MetricsGetter {
	return &InterConnect{
		op:        params[0].(*operator),
		client:    params[1].(*monitoring.MetricClient),
		namespace: params[2].(string),
	}
}

func (i *InterConnect) GetMetrics() error {
	i.metrics = []string{
		// 互联带宽上限
		"interconnect.googleapis.com/network/attachment/capacity",
		// 互联接受到的字节数
		"interconnect.googleapis.com/network/attachment/received_bytes_count",
		// 互联接收到的数据包
		"interconnect.googleapis.com/network/attachment/received_packets_count",
		// 发送的字节数
		"interconnect.googleapis.com/network/attachment/sent_bytes_count",
		//发送的数据包
		"interconnect.googleapis.com/network/attachment/sent_packets_count",
	}
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
		value := i.op.getPointValue(series.ValueType.String(), point)

		resourceLabels := series.Resource.Labels

		n9e := &common.MetricValue{
			Metric:       common.BuildMetric("interconnect", metricName),
			Endpoint:     resourceLabels["attachment"],
			Timestamp:    ts,
			ValueUntyped: value,
		}
		tagsMap := map[string]string{
			"provider":  ProviderName,
			"iden":      i.op.req.Iden,
			"namespace": i.namespace,
		}

		for k, v := range resourceLabels {
			tagsMap[k] = v
		}

		i.op.pushTo(n9e, tagsMap)
	}
}

func (i *InterConnect) AsyncMeta(context.Context) {}

func init() {
	registers[GOOGLE_INTERCONNECT] = new(InterConnect)
}
