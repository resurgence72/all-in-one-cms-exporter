package ali

import (
	"context"
	"sync"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/vpc"
	"github.com/sirupsen/logrus"
)

// metrics 4 eip 维度对象
type Eip struct {
	meta

	// 保存eip的实例id对应的eip对象
	eipMap map[string]*vpc.EipAddress
}

func init() {
	registers[ACS_VPC_EIP] = new(Eip)
}

func (e *Eip) Inject(params ...interface{}) common.MetricsGetter {
	return &Eip{meta: newMeta(params)}
}

func (e *Eip) GetNamespace() string {
	return e.namespace
}

func (e *Eip) GetMetrics() error {
	metrics, err := e.op.getMetrics(
		e.client,
		e.namespace,
		nil,
		nil,
	)
	if err != nil {
		return err
	}
	e.metrics = metrics
	return nil
}

func (e *Eip) Collector() {
	e.op.getMetricLastData(
		e.client,
		e.metrics,
		5,
		e.namespace,
		e.push,
		nil,
		[]string{"instanceId"},
	)
}

func (e *Eip) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["instanceId"]
		if !ok {
			continue
		}

		instanceID := p.(string)
		eip := e.getEip(instanceID)
		if eip == nil {
			continue
		}

		// 根据eipMap获取tags 和 endpoint
		n9e := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("eip", transfer.metric),
			ValueUntyped: point.Value(),
		}

		n9e.Endpoint = eip.IpAddress
		tagsMap := map[string]string{
			"region": eip.RegionId,
			// eip付费类型
			"internet_charge_type": eip.InternetChargeType,
			/* 当前绑定的实例类型
			EcsInstance：VPC类型的ECS实例。
			SlbInstance：VPC类型的SLB实例。
			Nat：NAT网关。
			HaVip：高可用虚拟IP。
			NetworkInterface：辅助弹性网卡。
			*/
			"instance_type": eip.InstanceType,
			"instance_id":   instanceID,
			/* eip状态
			Associating：绑定中。
			Unassociating：解绑中。
			InUse：已分配。
			Available：可用。
			*/
			"status":    eip.Status,
			"provider":  ProviderName,
			"iden":      e.op.req.Iden,
			"namespace": e.namespace,
			// 指标单位
			"unit_name": transfer.unit,
		}

		for _, tag := range eip.Tags.Tag {
			if tag.Value != "" {
				tagsMap[tag.Key] = tag.Value
			}
		}

		n9e.BuildAndShift(tagsMap)
	}
}

func (e *Eip) getEip(instance string) *vpc.EipAddress {
	e.m.RLock()
	defer e.m.RUnlock()

	if eip, ok := e.eipMap[instance]; ok {
		return eip
	}
	return nil
}

func (e *Eip) AsyncMeta(ctx context.Context) {
	// 并发获取全量region的eip对象，保存到map中
	var (
		wg          sync.WaitGroup
		maxPageSize = 100
		parse       = func(region string, pageNum int, container []vpc.EipAddress) ([]vpc.EipAddress, int, error) {
			bytes, err := e.op.commonRequest(
				region,
				"vpc",
				"2016-04-28",
				"DescribeEipAddresses",
				pageNum,
				maxPageSize,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(vpc.DescribeEipAddressesResponse)
			if err := parser.Parser().Unmarshal(bytes, resp); err != nil {
				return nil, 0, err
			}

			return append(container, resp.EipAddresses.EipAddress...), len(resp.EipAddresses.EipAddress), nil
		}
	)

	if e.eipMap == nil {
		e.eipMap = make(map[string]*vpc.EipAddress)
	}

	// 获取eip同步时使用全量region
	for _, region := range e.op.getRegions() {
		wg.Add(1)
		go func(region string) {
			defer wg.Done()
			var (
				pageNum   = 1
				container []vpc.EipAddress
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

			// eipList拿到当前region下的所有eip实例
			// 保存到 eipMap 中
			for i := range container {
				eip := container[i]

				// 并发写map加锁
				e.m.Lock()
				// AllocationId 是eip的id
				// Instance 是当前绑定的实例id
				e.eipMap[eip.AllocationId] = &eip
				e.m.Unlock()
			}
		}(region)
	}

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"eipLens": len(e.eipMap),
		"iden":    e.op.req.Iden,
	}).Warnln("async loop success, get all eip instance")
}
