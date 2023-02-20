package google

import (
	monitoring "cloud.google.com/go/monitoring/apiv3"
	"context"
	"watcher4metrics/pkg/common"
)

type Lb struct {
	op        *operator
	client    *monitoring.MetricClient
	namespace string
	metrics   []string
}

func init() {
	registers[GOOGLE_LB] = new(Lb)
}

func (l *Lb) Inject(params ...interface{}) common.MetricsGetter {
	return &Lb{
		op:        params[0].(*operator),
		client:    params[1].(*monitoring.MetricClient),
		namespace: params[2].(string),
	}
}

func (l *Lb) GetNamespace() string {
	return l.namespace
}

func (l *Lb) GetMetrics() error {
	l.metrics = []string{
		// 从代理将请求发送到后端到代理从后端收到最后一个响应字节为止计算的延迟分布 ms
		"loadbalancing.googleapis.com/https/backend_latencies",
		// 作为请求从外部 HTTP(S) 负载平衡器发送到后端的字节数
		"loadbalancing.googleapis.com/https/backend_request_bytes_count",
		// 外部 HTTP(S) 负载平衡器后端服务的请求数
		"loadbalancing.googleapis.com/https/backend_request_count",
		// 作为响应从后端（或缓存）发送到外部 HTTP(S) 负载平衡器的字节数
		"loadbalancing.googleapis.com/https/backend_response_bytes_count",
		// 从代理将请求发送到后端到代理从后端收到最后一个响应字节为止计算的延迟分布 ms
		//"https/external/regional/backend_latencies",
		// 作为请求从客户端发送到 HTTP/S 负载均衡器的字节数
		//"https/external/regional/request_bytes_count",
		// HTTP/S 负载均衡器服务的请求数
		//"https/external/regional/request_count",
		// 作为响应从 HTTP/S 负载平衡器发送到客户端的字节数
		//"https/external/regional/response_bytes_count",
		// 从代理收到请求到代理在最后一个响应字节从客户端收到 ACK 计算的延迟分布
		//"https/external/regional/total_latencies",
		//为客户端和代理之间的每个连接测量的 RTT 分布
		"loadbalancing.googleapis.com/https/frontend_tcp_rtt",
		// 从内部 HTTP(S) 负载平衡器代理向后端发送请求到代理从后端收到最后一个响应字节之间计算的延迟分布
		//"https/internal/backend_latencies",
		// 作为请求从客户端发送到内部 HTTP(S) 负载平衡器的字节数
		//"https/internal/request_bytes_count",
		// 内部 HTTP(S) 负载平衡器处理的请求数
		//"https/internal/request_count",
		// 从内部 HTTP(S) 负载平衡器作为响应发送到客户端的字节数
		//"https/internal/response_bytes_count",
		// 从内部 HTTP(S) 负载平衡器代理收到请求到代理在最后一个响应字节从客户端收到 ACK 计算的延迟分布
		//"https/internal/total_latencies",
		// 作为请求从客户端发送到外部 HTTP(S) 负载平衡器的字节数
		"loadbalancing.googleapis.com/https/request_bytes_count",
		// 外部 HTTP(S) 负载平衡器处理的请求数
		"loadbalancing.googleapis.com/https/request_count",
		// 作为响应从外部 HTTP(S) 负载平衡器发送到客户端的字节数
		"loadbalancing.googleapis.com/https/response_bytes_count",
		// 从外部 HTTP(S) 负载平衡器代理收到请求到代理在最后一个响应字节从客户端收到 ACK 计算的延迟分布
		"loadbalancing.googleapis.com/https/total_latencies",
	}
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
			value := point.Value.GetInt64Value()

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
