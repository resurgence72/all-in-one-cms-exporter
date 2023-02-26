package ali

import (
	"context"
	"fmt"

	"watcher4metrics/pkg/common"
)

type OpenAPI struct {
	meta
}

var uidMap = map[string]string{
	"boke":     "1604567917092864",
	"baixiang": "1851451288891972",
}

func (o *OpenAPI) Inject(params ...interface{}) common.MetricsGetter {
	return &OpenAPI{meta: newMeta(params)}
}

func (o *OpenAPI) GetMetrics() error {
	metrics, err := o.op.getMetrics(
		o.client,
		o.namespace,
		nil,
		[]string{
			// API速率配额使用率
			"QuotaUsage",
			// 调用次数
			"TotalNumber",
		},
	)
	if err != nil {
		return err
	}
	o.metrics = metrics
	return nil
}

func (o *OpenAPI) GetNamespace() string {
	return o.namespace
}

func (o *OpenAPI) Collector() {
	// openAPI 特殊，需要根据iden拿uid 并指定Dimensions参数
	uid, ok := uidMap[o.op.req.Iden]
	if !ok {
		return
	}
	ds := fmt.Sprintf("[{\"userId\":\"%s\"}]", uid)
	o.op.getMetricLastData(
		o.client,
		o.metrics,
		5,
		o.namespace,
		o.push,
		&ds,
		nil,
	)
}

func (o *OpenAPI) push(transfer *transferData) {
	for _, point := range transfer.points {
		pn := point["productName"].(string)
		api := point["API"].(string)

		instanceID := fmt.Sprintf("%s_%s", pn, api)
		n9e := &common.MetricValue{
			Timestamp: int64(point["timestamp"].(float64)) / 1e3,
			Metric:    common.BuildMetric("openapi", transfer.metric),
			Endpoint:  instanceID,
		}

		n9e.ValueUntyped = point.Value()

		tagsMap := map[string]string{
			"provider":     ProviderName,
			"iden":         o.op.req.Iden,
			"namespace":    o.namespace,
			"unit_name":    transfer.unit,
			"api":          api,
			"project_mark": pn,
			"instance_id":  instanceID,
		}

		n9e.BuildAndShift(tagsMap)
	}
}

func (o *OpenAPI) AsyncMeta(context.Context) {}

func init() {
	registers[ACS_OPENAPI] = new(OpenAPI)
}
