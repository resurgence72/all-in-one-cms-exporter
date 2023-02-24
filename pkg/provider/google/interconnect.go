package google

import (
	"context"

	"watcher4metrics/pkg/common"

	monitoring "cloud.google.com/go/monitoring/apiv3"
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
