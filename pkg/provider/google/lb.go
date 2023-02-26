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

func (l *Lb) Inject(params ...interface{}) common.MetricsGetter { return &Lb{meta: newMeta(params)} }

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

		metricLabels := series.Metric.Labels
		resourceLabels := series.Resource.Labels
		ep, ok := resourceLabels["url_map_name"]
		if !ok {
			return
		}

		for _, point := range points {
			ts := point.Interval.EndTime.GetSeconds()
			value := l.op.getPointValue(series.ValueType.String(), point)

			n9e := &common.MetricValue{
				Metric:       common.BuildMetric("lb", metricName),
				Endpoint:     ep,
				Timestamp:    ts,
				ValueUntyped: value,
			}

			tagsMap := map[string]string{
				"provider":  ProviderName,
				"iden":      l.op.req.Iden,
				"namespace": l.namespace,
			}

			if pid, ok := resourceLabels["project_id"]; ok {
				tagsMap["project_id"] = pid

				if pname, ok := l.op.projects.Load(pid); ok {
					tagsMap["project_mark"] = pname.(string)
				}
			}

			if ccode, ok := metricLabels["response_code"]; ok {
				tagsMap["response_code"] = ccode
			}

			if cc, ok := metricLabels["response_code_class"]; ok {
				tagsMap["response_code_class"] = cc
			}

			l.op.pushTo(n9e, tagsMap)
		}
	}
}

func (l *Lb) AsyncMeta(ctx context.Context) {}
