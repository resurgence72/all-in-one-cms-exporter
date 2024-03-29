package ali

import (
	"context"
	"strings"

	"watcher4metrics/pkg/common"
)

type Waf struct {
	meta
}

func init() {
	registers[ACS_WAF] = new(Waf)
}

func (w *Waf) Inject(params ...any) common.MetricsGetter {
	return &Waf{meta: newMeta(params...)}
}

func (w *Waf) GetNamespace() string {
	return w.namespace
}

func (w *Waf) GetMetrics() error {
	metrics, err := w.op.getMetrics(
		w.client,
		w.namespace,
		newMetricsBuilder(),
	)
	if err != nil {
		return err
	}

	// 过滤无用指标
	for _, metric := range metrics {
		mn := strings.ToLower(metric.MetricName)
		if !strings.HasPrefix(mn, "pvv") && !strings.HasPrefix(mn, "rrv") && !strings.HasPrefix(mn, "rv") && !strings.HasPrefix(mn, "v_") {
			w.metrics = append(w.metrics, metric)
		}
	}
	return nil
}

func (w *Waf) Collector() {
	w.op.getMetricLastData(
		w.client,
		w.metrics,
		w.namespace,
		w.push,
		nil,
		[]string{"instanceId", "domain", "resource"},
	)
}

func (w *Waf) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["instanceId"]
		if !ok {
			continue
		}

		series := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("waf", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     point["domain"].(string),
		}

		if r, ok := point["domain"]; ok && len(r.(string)) > 0 {
			series.Endpoint = r.(string)
		} else if r, ok := point["resource"]; ok && len(r.(string)) > 0 {
			series.Endpoint = r.(string)
		} else {
			series.Endpoint = p.(string)
		}

		series.TagsMap = map[string]string{
			"provider":    ProviderName,
			"iden":        w.op.req.Iden,
			"namespace":   w.namespace,
			"unit_name":   transfer.unit,
			"instance_id": p.(string),
		}

		series.BuildAndShift()
	}
}

func (w *Waf) AsyncMeta(ctx context.Context) {}
