package ali

import (
	"context"
	"sync"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/slb"
	"github.com/sirupsen/logrus"
)

type Slb struct {
	meta
}

func init() {
	registers[ACS_SLB_DASHBOARD] = new(Slb)
}

func (s *Slb) Inject(params ...any) common.MetricsGetter {
	return &Slb{meta: newMeta(params...)}
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
		s.namespace,
		s.push,
		nil,
		[]string{"instanceId"},
	)
}

func (s *Slb) loadSLB(id string) *slb.LoadBalancer {
	if sb, ok := s.mp.Load(id); ok {
		return sb.(*slb.LoadBalancer)
	} else {
		return nil
	}
}

func (s *Slb) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["instanceId"]
		if !ok {
			continue
		}

		slb := s.loadSLB(p.(string))
		if slb == nil {
			continue
		}

		series := s.op.buildSeries(slb.Address, "slb", transfer.metric, point)
		series.TagsMap = map[string]string{
			"provider":    ProviderName,
			"iden":        s.op.req.Iden,
			"namespace":   s.namespace,
			"unit_name":   transfer.unit,
			"instance_id": slb.LoadBalancerId,
			"status":      slb.LoadBalancerStatus,
			"region":      slb.RegionId,
		}

		for _, tag := range slb.Tags.Tag {
			if tag.TagValue != "" {
				series.TagsMap[tag.TagKey] = tag.TagValue
			}
		}

		series.BuildAndShift()
	}
}

func (s *Slb) AsyncMeta(ctx context.Context) {
	var (
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

	s.op.async(s.op.getRegions(), func(region string, wg *sync.WaitGroup) {
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
			s.mp.Store(slb.LoadBalancerId, &slb)
		}
	})

	logrus.WithFields(logrus.Fields{
		"slbLens": s.op.mapLens(s.mp),
		"iden":    s.op.req.Iden,
	}).Warnln("async loop success, get all slb instance")
}
