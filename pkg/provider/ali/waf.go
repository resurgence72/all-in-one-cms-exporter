package ali

import (
	"context"

	"watcher4metrics/pkg/common"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
)

type Waf struct {
	op        *operator
	namespace string
	metrics   []*cms.Resource
	client    *cms.Client
}

func init() {
	registers[ACS_WAF] = new(Waf)
}

func (w *Waf) Inject(params ...interface{}) common.MetricsGetter {
	return &Waf{
		op:        params[0].(*operator),
		client:    params[1].(*cms.Client),
		namespace: params[2].(string),
	}
}

func (w *Waf) GetNamespace() string {
	return w.namespace
}

func (w *Waf) GetMetrics() error {
	metrics, err := w.op.getMetrics(
		w.client,
		w.namespace,
		nil,
		nil,
	)
	if err != nil {
		return err
	}
	w.metrics = metrics
	return nil
}

func (w *Waf) Collector() {
	w.op.getMetricLastData(
		w.client,
		w.metrics,
		5,
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

		n9e := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("waf", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     point["domain"].(string),
		}

		if r, ok := point["domain"]; ok && len(r.(string)) > 0 {
			n9e.Endpoint = r.(string)
		} else if r, ok := point["resource"]; ok && len(r.(string)) > 0 {
			n9e.Endpoint = r.(string)
		} else {
			n9e.Endpoint = p.(string)
		}

		tagsMap := map[string]string{
			"provider":    ProviderName,
			"iden":        w.op.req.Iden,
			"namespace":   w.namespace,
			"unit_name":   transfer.unit,
			"instance_id": p.(string),
		}

		n9e.BuildAndShift(tagsMap)
	}
}

func (w *Waf) AsyncMeta(ctx context.Context) {}
