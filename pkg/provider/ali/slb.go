package ali

import (
	"context"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/slb"
	"github.com/sirupsen/logrus"
	"sync"
	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"
)

type Slb struct {
	op        *operator
	namespace string
	metrics   []*cms.Resource
	slbMap    map[string]*slb.LoadBalancer
	client    *cms.Client

	m sync.Mutex
}

func init() {
	registers[ACS_SLB_DASHBOARD] = new(Slb)
}

func (s *Slb) Inject(params ...interface{}) common.MetricsGetter {
	return &Slb{
		op:        params[0].(*operator),
		client:    params[1].(*cms.Client),
		namespace: params[2].(string),
	}
}

func (s *Slb) GetNamespace() string {
	return s.namespace
}

func (s *Slb) GetMetrics() error {
	metrics, err := s.op.getMetrics(
		s.client,
		s.namespace,
		nil,
		nil,
	)
	if err != nil {
		return err
	}
	s.metrics = metrics
	return nil
}

func (s *Slb) Collector() {
	s.op.getMetricLastData(
		s.client,
		s.metrics,
		5,
		s.namespace,
		s.push,
		nil,
		[]string{"instanceId"},
	)
}

func (s *Slb) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["instanceId"]
		if !ok {
			continue
		}

		instanceID := p.(string)
		slb, ok := s.slbMap[instanceID]
		if !ok {
			continue
		}

		n9e := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("slb", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     slb.Address,
		}

		tagsMap := map[string]string{
			"provider":    ProviderName,
			"iden":        s.op.req.Iden,
			"namespace":   s.namespace,
			"unit_name":   transfer.unit,
			"instance_id": instanceID,
			"status":      slb.LoadBalancerStatus,
			"region":      slb.RegionId,
		}

		for _, tag := range slb.Tags.Tag {
			if tag.TagValue != "" {
				tagsMap[tag.TagKey] = tag.TagValue
			}
		}

		n9e.BuildAndShift(tagsMap)
	}
}

func (s *Slb) AsyncMeta(ctx context.Context) {
	var (
		wg          sync.WaitGroup
		maxPageSize = 100
		parse       = func(region string, pageNum int, container []slb.LoadBalancer) ([]slb.LoadBalancer, int, error) {
			bytes, err := s.op.commonRequest(
				region,
				"slb",
				"2014-05-15",
				"DescribeLoadBalancers",
				pageNum,
				maxPageSize,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(slb.DescribeLoadBalancersResponse)
			if err := parser.Parser().Unmarshal(bytes, resp); err != nil {
				return nil, 0, err
			}
			return append(container, resp.LoadBalancers.LoadBalancer...), len(resp.LoadBalancers.LoadBalancer), nil
		}
	)

	if s.slbMap == nil {
		s.slbMap = make(map[string]*slb.LoadBalancer)
	}

	for _, region := range s.op.getRegions() {
		wg.Add(1)
		go func(region string) {
			defer wg.Done()
			var (
				pageNum   = 1
				container []slb.LoadBalancer
			)

			container, currLen, err := parse(region, pageNum, container)
			if err != nil {
				logrus.Errorln("AsyncMeta err ", err, region)
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
				slb := container[i]

				s.m.Lock()
				s.slbMap[slb.LoadBalancerId] = &slb
				s.m.Unlock()
			}
		}(region)
	}

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"slbLens": len(s.slbMap),
		"iden":    s.op.req.Iden,
	}).Warnln("async loop success, get all slb instance")
}
