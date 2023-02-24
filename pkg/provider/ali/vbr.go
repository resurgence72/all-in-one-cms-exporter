package ali

import (
	"context"
	"sync"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/vpc"
	"github.com/sirupsen/logrus"
)

type Vbr struct {
	op        *operator
	namespace string
	metrics   []*cms.Resource
	vbrMap    map[string]*vpc.VirtualBorderRouterType
	client    *cms.Client

	m sync.Mutex
}

func init() {
	registers[ACS_PHYSICAL_CONNECTION] = new(Vbr)
}

func (v *Vbr) Inject(params ...interface{}) common.MetricsGetter {
	return &Vbr{
		op:        params[0].(*operator),
		client:    params[1].(*cms.Client),
		namespace: params[2].(string),
	}
}

func (v *Vbr) GetNamespace() string {
	return v.namespace
}

func (v *Vbr) AsyncMeta(ctx context.Context) {
	var (
		wg          sync.WaitGroup
		maxPageSize = 50
		parse       = func(region string, pageNum int, container []vpc.VirtualBorderRouterType) ([]vpc.VirtualBorderRouterType, int, error) {
			bytes, err := v.op.commonRequest(
				region,
				"vpc",
				"2016-04-28",
				"DescribeVirtualBorderRouters",
				pageNum,
				maxPageSize,
				// 只获取使用中的vbr
				map[string]string{
					"Filter.1.Key":     "Status",
					"Filter.1.Value.1": "active",
				},
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(vpc.DescribeVirtualBorderRoutersResponse)
			if err := parser.Parser().Unmarshal(bytes, resp); err != nil {
				return nil, 0, err
			}
			return append(container, resp.VirtualBorderRouterSet.VirtualBorderRouterType...), len(resp.VirtualBorderRouterSet.VirtualBorderRouterType), nil
		}
	)

	if v.vbrMap == nil {
		v.vbrMap = make(map[string]*vpc.VirtualBorderRouterType)
	}

	for _, region := range v.op.getRegions() {
		wg.Add(1)
		go func(region string) {
			defer wg.Done()
			var (
				pageNum   = 1
				container []vpc.VirtualBorderRouterType
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
				vbr := container[i]

				v.m.Lock()
				v.vbrMap[vbr.VbrId] = &vbr
				v.m.Unlock()
			}
		}(region)
	}

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"vbrLens": len(v.vbrMap),
		"iden":    v.op.req.Iden,
	}).Warnln("async loop success, get all vbr instance")
}

func (v *Vbr) GetMetrics() error {
	metrics, err := v.op.getMetrics(
		v.client,
		v.namespace,
		nil,
		nil,
	)
	if err != nil {
		return err
	}
	v.metrics = metrics
	return nil
}

func (v *Vbr) Collector() {
	v.op.getMetricLastData(
		v.client,
		v.metrics,
		5,
		v.namespace,
		v.push,
		nil,
		[]string{"instanceId"},
	)
}

func (v *Vbr) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["instanceId"]
		if !ok {
			continue
		}

		instanceID := p.(string)
		vbr, ok := v.vbrMap[instanceID]
		if !ok {
			continue
		}

		n9e := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("vbr", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     vbr.Name,
		}

		tagsMap := map[string]string{
			"provider":         ProviderName,
			"iden":             v.op.req.Iden,
			"namespace":        v.namespace,
			"unit_name":        transfer.unit,
			"instance_id":      instanceID,
			"status":           vbr.Status,
			"type":             vbr.Type,
			"peer_gateway_ip":  vbr.PeerGatewayIp,
			"local_gateway_ip": vbr.LocalGatewayIp,
		}

		n9e.BuildAndShift(tagsMap)
	}
}
