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
		map[string]struct{}{
			// 最大连接数使用率
			"InstanceMaxConnectionUtilization": {},
			// 新建连接数使用率
			"InstanceNewConnectionUtilization": {},
			// 7层协议实例QPS
			"InstanceQps": {},
			// QPS使用率
			"InstanceQpsUtilization": {},
			// 7层协议实例4XX状态码
			"InstanceStatusCode4xx": {},
			// 7层协议实例5XX状态码
			"InstanceStatusCode5xx": {},
			// 端口并发连接数
			"MaxConnection": {},
			// TCP新建连接数
			"NewConnection": {},
			// 7层协议端口QPS
			"Qps": {},
			// 7层协议端口4XX状态码
			"StatusCode4xx": {},
			// 7层协议端口5XX状态码
			"StatusCode5xx": {},
			// 后端异常ECS实例个数
			"UnhealthyServerCount": {},
			// 7层协议端口Upstream4XX状态码
			"UpstreamCode4xx": {},
			// 7层协议端口Upstream5XX状态码
			"UpstreamCode5xx": {},
		},
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
	)
}

func (s *Slb) push(transfer *transferData) {
	for _, point := range transfer.points {
		instanceID := point["instanceId"].(string)
		slb, ok := s.slbMap[instanceID]
		if !ok {
			continue
		}

		n9e := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("slb", transfer.metric),
			ValueUntyped: point["Average"],
			Endpoint:     slb.Address,
		}

		tagsMap := map[string]string{
			"provider":    ProviderName,
			"iden":        s.op.req.Iden,
			"namespace":   ACS_SLB_DASHBOARD.toString(),
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
