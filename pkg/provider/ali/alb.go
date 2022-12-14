package ali

import (
	"context"
	"fmt"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/alb"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	"github.com/sirupsen/logrus"
	"sync"
	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"
)

type Alb struct {
	op        *operator
	namespace string
	metrics   []*cms.Resource
	albMap    map[string]*alb.LoadBalancer
	client    *cms.Client

	m sync.Mutex
}

func init() {
	registers[ACS_ALB] = new(Alb)
}

func (a *Alb) Inject(params ...interface{}) common.MetricsGetter {
	return &Alb{
		op:        params[0].(*operator),
		client:    params[1].(*cms.Client),
		namespace: params[2].(string),
	}
}

func (a *Alb) GetMetrics() error {
	metrics, err := a.op.getMetrics(
		a.client,
		a.namespace,
		nil,
		[]string{
			// 活跃连接数
			"LoadBalancerActiveConnection",
			// 2xx
			"LoadBalancerHTTPCode2XX",
			// 3xx
			"LoadBalancerHTTPCode3XX",
			// 4xx
			"LoadBalancerHTTPCode4XX",
			// 5xx
			"LoadBalancerHTTPCode5XX",
			// qps
			"LoadBalancerQPS",
			// 新建连接数
			"LoadBalancerNewConnection",
			// 不健康的服务器数
			"LoadBalancerUnHealthyHostCount",
		},
	)
	if err != nil {
		return err
	}
	a.metrics = metrics
	return nil
}

func (a *Alb) GetNamespace() string {
	return a.namespace
}

func (a *Alb) Collector() {
	a.op.getMetricLastData(
		a.client,
		a.metrics,
		5,
		a.namespace,
		a.push,
		nil,
		nil,
	)
}

func (a *Alb) AsyncMeta(ctx context.Context) {
	var (
		wg          sync.WaitGroup
		maxPageSize = 100
		parse       = func(region string, pageNum int, container []alb.LoadBalancer) ([]alb.LoadBalancer, int, error) {
			bytes, err := a.op.commonRequest(
				region,
				"alb",
				"2020-06-16",
				"ListLoadBalancers",
				pageNum,
				maxPageSize,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(alb.ListLoadBalancersResponse)
			if err := parser.Parser().Unmarshal(bytes, resp); err != nil {
				return nil, 0, err
			}
			return append(container, resp.LoadBalancers...), len(resp.LoadBalancers), nil
		}
	)

	if a.albMap == nil {
		a.albMap = make(map[string]*alb.LoadBalancer)
	}

	for _, region := range a.op.getRegions() {
		wg.Add(1)
		go func(region string) {
			defer wg.Done()
			var (
				pageNum   = 1
				container []alb.LoadBalancer
			)
			container, currLen, err := parse(region, pageNum, container)
			if err != nil {
				return
			}

			for currLen == maxPageSize {
				pageNum++
				container, currLen, err = parse(region, pageNum, container)
				if err != nil {
					logrus.Errorln("AsyncMeta paging err ", err)
					continue
				}
			}

			for i := range container {
				alb := container[i]
				a.m.Lock()
				a.albMap[alb.LoadBalancerId] = &alb
				a.m.Unlock()
			}
		}(region)
	}

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"albLens": len(a.albMap),
		"iden":    a.op.req.Iden,
	}).Warnln("async loop success, get all alb instance")
}

func (a *Alb) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["loadBalancerId"]
		if !ok {
			continue
		}

		instanceID := p.(string)
		alb, ok := a.albMap[instanceID]
		if !ok {
			fmt.Println("跳过了" ,instanceID)
			continue
		}

		n9e := &common.MetricValue{
			Metric:       common.BuildMetric("alb", transfer.metric),
			Endpoint:     alb.LoadBalancerName,
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			ValueUntyped: point.Value(),
		}

		tagsMap := map[string]string{
			"provider":      ProviderName,
			"iden":          a.op.req.Iden,
			"namespace":     ACS_SLB_DASHBOARD.toString(),
			"unit_name":     transfer.unit,
			"instance_id":   instanceID,
			"instance_name": alb.LoadBalancerName,
			"status":        alb.LoadBalancerStatus,
		}

		for _, tag := range alb.Tags {
			if tag.Value != "" {
				tagsMap[tag.Key] = tag.Value
			}
		}

		n9e.BuildAndShift(tagsMap)
	}
}
