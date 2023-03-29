package ali

import (
	"context"
	"fmt"
	"sync"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/alb"
	"github.com/sirupsen/logrus"
)

type Alb struct {
	meta

	albMap map[string]*alb.LoadBalancer
}

func init() {
	registers[ACS_ALB] = new(Alb)
}

func (a *Alb) Inject(params ...any) common.MetricsGetter {
	return &Alb{meta: newMeta(params...)}
}

func (a *Alb) GetMetrics() error {
	metrics, err := a.op.getMetrics(
		a.client,
		a.namespace,
		nil,
		nil,
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
		a.namespace,
		a.push,
		nil,
		[]string{"loadBalancerId"},
	)
}

func (a *Alb) AsyncMeta(ctx context.Context) {
	var (
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

	a.op.async(a.op.getRegions(), func(region string, wg *sync.WaitGroup) {
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
	})

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
			fmt.Println("跳过了", instanceID)
			continue
		}

		series := &common.MetricValue{
			Metric:       common.BuildMetric("alb", transfer.metric),
			Endpoint:     alb.LoadBalancerName,
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			ValueUntyped: point.Value(),
		}

		series.TagsMap = map[string]string{
			"provider":      ProviderName,
			"iden":          a.op.req.Iden,
			"namespace":     a.namespace,
			"unit_name":     transfer.unit,
			"instance_id":   instanceID,
			"instance_name": alb.LoadBalancerName,
			"status":        alb.LoadBalancerStatus,
		}

		for _, tag := range alb.Tags {
			if tag.Value != "" {
				series.TagsMap[tag.Key] = tag.Value
			}
		}

		series.BuildAndShift()
	}
}
