package ali

import (
	"context"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	"watcher4metrics/pkg/common"
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
		map[string]struct{}{
			"4XX_ratio":   {},
			"5XX_ratio":   {},
			"acl_rate_5m": {},
			"cc_rate_5m":  {},
			"qps":         {},
			"waf_rate_5m": {},
		},
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
	)
}

func (w *Waf) push(transfer *transferData) {
	for _, point := range transfer.points {
		instanceID := point["instanceId"].(string)
		n9e := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("waf", transfer.metric),
			ValueUntyped: point["Maximum"],
			Endpoint:     point["domain"].(string),
		}

		tagsMap := map[string]string{
			"provider":    ProviderName,
			"iden":        w.op.req.Iden,
			"namespace":   ACS_WAF.toString(),
			"unit_name":   transfer.unit,
			"instance_id": instanceID,
		}

		n9e.BuildAndShift(tagsMap)
	}
}

func (w *Waf) AsyncMeta(ctx context.Context) {}
