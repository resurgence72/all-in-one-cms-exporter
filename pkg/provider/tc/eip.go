package tc

import (
	"context"
	"sync"

	"watcher4metrics/pkg/common"

	"github.com/goccy/go-json"
	"github.com/sirupsen/logrus"
	monitor "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/monitor/v20180724"
	vpc "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/vpc/v20170312"
)

type Eip struct {
	meta

	eipMap map[string]map[string]*vpc.Address
}

func init() {
	registers[QCP_LB] = new(Eip)
}

func (e *Eip) Inject(params ...any) common.MetricsGetter {
	return &Eip{meta: newMeta(params...)}
}

func (e *Eip) GetNamespace() string {
	return e.namespace
}

func (e *Eip) GetMetrics() error {
	// 获取所有ns下metrics指标  默认拿 ap-shanghai 即可
	metrics, err := e.op.getMetrics(
		e.clients[e.op.endpoint],
		e.namespace,
		nil,
	)
	if err != nil {
		return err
	}
	e.metrics = metrics
	return nil
}

func (e *Eip) Collector() {
	e.op.getMonitorData(
		e.clients,
		e.metrics,
		nil,
		func() InstanceBuilderFunc {
			return func(region string) []*monitor.Instance {
				return e.op.buildInstances(
					[]string{"eip"},
					func() [][]string {
						var (
							out  [][]string
							eips []string
						)
						for _, eip := range e.eipMap[region] {
							eips = append(eips, *eip.AddressIp)
						}
						return append(out, eips)
					}(),
				)
			}
		}(),
		e.namespace,
		e.push,
	)
}

func (e *Eip) push(transfer *transferData) {
	for _, point := range transfer.points {
		eipId := point.Dimensions[0].Value

		// 不存在当前eip, 可以直接return
		eip := e.getEip(transfer.region, eipId)
		if eip == nil {
			return
		}

		for i, ts := range point.Timestamps {
			series := &common.MetricValue{
				Timestamp:    int64(*ts),
				Metric:       common.BuildMetric("eip", transfer.metric),
				ValueUntyped: *point.Values[i],
				Endpoint:     *eip.AddressIp,
			}

			// 设置Tags TagsMap
			series.TagsMap = map[string]string{
				"iden":        e.op.req.Iden,
				"provider":    ProviderName,
				"region":      transfer.region,
				"namespace":   e.namespace,
				"unit_name":   transfer.unit,
				"instance_id": *eip.InstanceId,
				"status":      *eip.AddressStatus,
			}
			series.BuildAndShift()
			continue
		}
	}
}

func (e *Eip) getEip(region string, ip *string) *vpc.Address {
	e.m.RLock()
	defer e.m.RUnlock()
	if eipM, ok := e.eipMap[region]; ok {
		if eip, ok := eipM[*ip]; ok {
			return eip
		}
	}
	return nil
}

func (e *Eip) AsyncMeta(ctx context.Context) {
	var (
		maxPageSize = 100
		parse       = func(region string, offset, limit int, container []*vpc.Address) ([]*vpc.Address, int, error) {
			bs, err := e.op.commonRequest(
				region,
				"vpc",
				"2017-03-12",
				"DescribeAddresses",
				offset,
				limit,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(vpc.DescribeAddressesResponse)
			if err = json.Unmarshal(bs, resp); err != nil {
				return nil, 0, err
			}
			return append(container, resp.Response.AddressSet...), len(resp.Response.AddressSet), nil
		}
	)

	if e.eipMap == nil {
		e.eipMap = make(map[string]map[string]*vpc.Address)
	}

	e.op.async(e.op.getRegions(), func(region string, wg *sync.WaitGroup, sem *common.Semaphore) {
		defer func() {
			wg.Done()
			sem.Release()
		}()
		// 同步当前region eip详情
		var (
			offset    = 0
			pageNum   = 1
			container []*vpc.Address
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
				logrus.Errorln("tc loop paging failed", err)
				continue
			}
			pageNum++
		}

		// 保存获取到的所有eip map
		e.m.Lock()
		if _, ok := e.eipMap[region]; !ok {
			e.eipMap[region] = make(map[string]*vpc.Address)
		}
		e.m.Unlock()

		for i := range container {
			eip := container[i]

			e.m.Lock()
			// 只有 BIND eip才是有数据的
			e.eipMap[region][*eip.AddressIp] = eip
			e.m.Unlock()
		}
	})

	logrus.WithFields(logrus.Fields{
		"eipLens": len(e.eipMap),
		"iden":    e.op.req.Iden,
	}).Warnln("async loop get all tc eip success")
}
