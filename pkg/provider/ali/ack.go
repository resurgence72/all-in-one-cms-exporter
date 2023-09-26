package ali

import (
	"context"

	"watcher4metrics/pkg/common"
)

type Ack struct {
	meta
}

func (a *Ack) Inject(params ...any) common.MetricsGetter {
	return &Ack{meta: newMeta(params...)}
}

func (a *Ack) GetMetrics() error {
	metrics, err := a.op.getMetrics(
		a.client,
		a.namespace,
		newMetricsBuilder(),
	)
	if err != nil {
		return err
	}
	a.metrics = metrics
	return nil
}

func (a *Ack) GetNamespace() string {
	return a.namespace
}

func (a *Ack) Collector() {
	a.op.getMetricLastData(
		a.client,
		a.metrics,
		a.namespace,
		a.push,
		nil,
		[]string{"cluster", "namespace", "app", "type", "node", "pod"},
	)
}

func (a *Ack) AsyncMeta(_ context.Context) {}

func (a *Ack) push(transfer *transferData) {
	for _, point := range transfer.points {
		cluster, ok := point["cluster"]
		if !ok {
			continue
		}

		clusterId := cluster.(string)
		series := a.op.buildSeries(clusterId, "ack", transfer.metric, point)
		series.TagsMap = map[string]string{
			"unit_name":  transfer.unit,
			"provider":   ProviderName,
			"iden":       a.op.req.Iden,
			"namespace":  a.namespace,
			"cluster_id": clusterId,
		}

		if node, ok := point["node"]; ok {
			series.TagsMap["node"] = node.(string)
		}
		if pod, ok := point["pod"]; ok {
			series.TagsMap["pod"] = pod.(string)
		}
		if app, ok := point["app"]; ok {
			series.TagsMap["app"] = app.(string)
		}
		if typeStr, ok := point["type"]; ok {
			series.TagsMap["type"] = typeStr.(string)
		}
		if ns, ok := point["namespace"]; ok {
			series.TagsMap["namespace"] = ns.(string)
		}
		series.BuildAndShift()
	}
}

func init() {
	registers[ACS_K8S] = new(Ack)
}
