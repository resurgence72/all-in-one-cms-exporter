package tc

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/goccy/go-json"
	"github.com/sirupsen/logrus"
	clb "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/clb/v20180317"
	monitor "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/monitor/v20180724"

	"watcher4metrics/pkg/common"
)

type LbPublic struct {
	meta

	lbpMap map[string]map[string]*clb.LoadBalancer
}

func init() {
	registers[QCE_LB_PUBLIC] = new(LbPublic)
}

func (l *LbPublic) Inject(params ...any) common.MetricsGetter {
	return &LbPublic{meta: newMeta(params...)}
}

func (l *LbPublic) GetMetrics() error {
	metrics, err := l.op.getMetrics(
		l.clients[l.op.endpoint],
		l.namespace,
		nil,
	)
	if err != nil {
		return err
	}

	for _, metric := range metrics {
		mn := strings.ToLower(*metric.MetricName)
		if !strings.HasPrefix(mn, "unhealth") {
			l.metrics = append(l.metrics, metric)
		}
	}
	return nil
}

func (l *LbPublic) GetNamespace() string {
	return l.namespace
}

func (l *LbPublic) Collector() {
	l.op.getMonitorData(
		l.clients,
		l.metrics,
		nil,
		func() InstanceBuilderFunc {
			return func(region string) []*monitor.Instance {
				return l.op.buildInstances(
					[]string{"vip"},
					func() [][]string {
						var (
							out  [][]string
							vips []string
						)
						for _, lb := range l.lbpMap[region] {
							vips = append(vips, *transferVIPs(lb.LoadBalancerVips))
						}
						return append(out, vips)
					}(),
				)
			}
		}(),
		l.namespace,
		l.push,
	)
}

func transferVIPs(vs []*string) *string {
	vips := make([]string, 0, len(vs))
	for _, vip := range vs {
		vips = append(vips, *vip)
	}

	vip := strings.Join(vips, ",")
	return &vip
}

func (l *LbPublic) AsyncMeta(ctx context.Context) {
	var (
		maxPageSize = 100
		parse       = func(region string, offset, limit int, container []*clb.LoadBalancer) ([]*clb.LoadBalancer, int, error) {
			bs, err := l.op.commonRequest(
				region,
				"clb",
				"2018-03-17",
				"DescribeLoadBalancers",
				offset,
				limit,
				map[string]any{"LoadBalancerType": "OPEN"}, // lb_public只获取公网lb
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(clb.DescribeLoadBalancersResponse)
			if err = json.Unmarshal(bs, resp); err != nil {
				return nil, 0, err
			}
			return append(container, resp.Response.LoadBalancerSet...), len(resp.Response.LoadBalancerSet), nil
		}
	)

	if l.lbpMap == nil {
		l.lbpMap = make(map[string]map[string]*clb.LoadBalancer)
	}

	l.op.async(l.op.getRegions(), func(region string, wg *sync.WaitGroup, sem *common.Semaphore) {
		defer func() {
			wg.Done()
			sem.Release()
		}()

		var (
			offset    = 1
			pageNum   = 1
			container []*clb.LoadBalancer
		)

		container, currLen, err := parse(region, offset, maxPageSize, container)
		if err != nil {
			return
		}

		// 分页
		for currLen == maxPageSize {
			offset = pageNum * maxPageSize
			container, currLen, err = parse(region, offset, maxPageSize, container)
			if err != nil {
				continue
			}
			pageNum++
		}

		l.m.Lock()
		if _, ok := l.lbpMap[region]; !ok {
			l.lbpMap[region] = make(map[string]*clb.LoadBalancer)
		}
		l.m.Unlock()

		for i := range container {
			cc := container[i]

			l.m.Lock()
			l.lbpMap[region][*transferVIPs(cc.LoadBalancerVips)] = cc
			l.m.Unlock()
		}
	})

	logrus.WithFields(logrus.Fields{
		"clbPublicLens": len(l.lbpMap),
		"iden":          l.op.req.Iden,
	}).Warnln("async loop get all tc clb public success")
}

func (l *LbPublic) push(transfer *transferData) {
	for _, point := range transfer.points {
		vip := point.Dimensions[0].Value
		lb := l.getLb(transfer.region, vip)
		if lb == nil {
			return
		}

		for i, ts := range point.Timestamps {
			series := &common.MetricValue{
				Timestamp:    int64(*ts),
				Metric:       common.BuildMetric("clb", transfer.metric),
				ValueUntyped: *point.Values[i],
				Endpoint:     *lb.LoadBalancerId,
			}

			series.TagsMap = map[string]string{
				"iden":      l.op.req.Iden,
				"provider":  ProviderName,
				"region":    transfer.region,
				"namespace": l.namespace,

				"lb_id":   *lb.LoadBalancerId,
				"lb_name": *lb.LoadBalancerName,
				"lb_type": *lb.LoadBalancerType,
				"lb_vips": *transferVIPs(lb.LoadBalancerVips),
				"forward": strconv.FormatUint(*lb.Forward, 10),
				"domain":  *lb.Domain,
				"status":  strconv.FormatUint(*lb.Status, 10),
			}

			if pn, ok := l.op.projectMap.Load(*lb.ProjectId); ok {
				series.TagsMap["project_mark"] = pn.(string)
			}

			for _, tag := range lb.Tags {
				if *tag.TagValue != "" {
					series.TagsMap[*tag.TagKey] = *tag.TagValue
				}
			}

			series.BuildAndShift()
			continue
		}
	}
}

func (l *LbPublic) getLb(region string, vip *string) *clb.LoadBalancer {
	l.m.RLock()
	defer l.m.RUnlock()

	if lbM, ok := l.lbpMap[region]; ok {
		if lb, ok := lbM[*vip]; ok {
			return lb
		}
	}
	return nil
}
