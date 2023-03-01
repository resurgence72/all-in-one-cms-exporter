package tc

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/goccy/go-json"

	"watcher4metrics/pkg/common"

	"github.com/sirupsen/logrus"
	cvm "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cvm/v20170312"
	monitor "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/monitor/v20180724"
)

type Ecs struct {
	meta

	ecsMap map[string]map[string]*cvm.Instance
}

func init() {
	registers[QCE_CVM] = new(Ecs)
}

func (e *Ecs) Inject(params ...any) common.MetricsGetter {
	return &Ecs{meta: newMeta(params...)}
}

func (e *Ecs) GetNamespace() string {
	return e.namespace
}

func (e *Ecs) GetMetrics() error {
	metrics, err := e.op.getMetrics(
		e.clients["ap-shanghai"],
		e.namespace,
		nil,
	)
	if err != nil {
		return err
	}
	e.metrics = metrics
	return nil
}

func (e *Ecs) Collector() {
	e.op.getMonitorData(
		e.clients,
		e.metrics,
		nil,
		func() InstanceBuilderFunc {
			return func(region string) []*monitor.Instance {
				return e.op.buildInstances(
					[]string{"InstanceId"},
					func() [][]string {
						var (
							out         [][]string
							instanceIds []string
						)
						for _, ecs := range e.ecsMap[region] {
							instanceIds = append(instanceIds, *ecs.InstanceId)
						}
						return append(out, instanceIds)
					}(),
				)
			}
		}(),
		e.namespace,
		e.push,
	)
}

func (e *Ecs) push(transfer *transferData) {
	for _, point := range transfer.points {
		ecsInstanceId := point.Dimensions[0].Value

		// 不存在当前eip, 可以直接return
		ecs := e.getEcs(transfer.region, ecsInstanceId)
		if ecs == nil {
			return
		}

		for i, ts := range point.Timestamps {
			series := &common.MetricValue{
				Timestamp:    int64(*ts),
				Metric:       common.BuildMetric("ecs", transfer.metric),
				ValueUntyped: *point.Values[i],
			}

			// 存在eip, 赋值tag 及 ip
			var (
				priIPs, pubIPs []string
			)
			for i := range ecs.PublicIpAddresses {
				pubIPs = append(pubIPs, *ecs.PublicIpAddresses[i])
			}
			for i := range ecs.PrivateIpAddresses {
				priIPs = append(priIPs, *ecs.PrivateIpAddresses[i])
			}

			// 设置endpoint
			series.Endpoint = strings.Join(pubIPs, ",")

			// 设置Tags TagsMap
			tagsMap := map[string]string{
				"iden":            e.op.req.Iden,
				"provider":        ProviderName,
				"region":          transfer.region,
				"namespace":       e.namespace,
				"unit_name":       transfer.unit,
				"instance_id":     *ecsInstanceId,
				"instance_name":   *ecs.InstanceName,
				"instance_status": *ecs.InstanceState,
				"cpu":             strconv.Itoa(int(*ecs.CPU)),
				"memory":          strconv.Itoa(int(*ecs.Memory)),
				// cvm的公网私网ip
				"public_ip":  series.Endpoint,
				"private_ip": strings.Join(priIPs, ","),
			}

			if pn, ok := e.op.projectMap.Load(uint64(*ecs.Placement.ProjectId)); ok {
				tagsMap["project_mark"] = pn.(string)
			}

			series.BuildAndShift(tagsMap)
			continue
		}
	}
}

func (e *Ecs) AsyncMeta(ctx context.Context) {
	var (
		wg          sync.WaitGroup
		maxPageSize = 100
		parseECS    = func(region string, offset, limit int, container []*cvm.Instance) ([]*cvm.Instance, int, error) {
			bs, err := e.op.commonRequest(
				region,
				"cvm",
				"2017-03-12",
				"DescribeInstances",
				offset,
				limit,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(cvm.DescribeInstancesResponse)
			if err = json.Unmarshal(bs, resp); err != nil {
				return nil, 0, err
			}
			return append(container, resp.Response.InstanceSet...), len(resp.Response.InstanceSet), nil
		}

		sem = common.NewSemaphore(10)
	)

	if e.ecsMap == nil {
		e.ecsMap = make(map[string]map[string]*cvm.Instance)
	}

	// 获取所有region下的ecs
	for region := range e.clients {
		wg.Add(1)
		sem.Acquire()
		go func(region string) {
			defer func() {
				wg.Done()
				sem.Release()
			}()

			var (
				offset    = 0
				pageNum   = 1
				container []*cvm.Instance
			)

			container, currLen, err := parseECS(region, offset, maxPageSize, container)
			if err != nil {
				return
			}

			// 分页
			for currLen == maxPageSize {
				offset = pageNum * maxPageSize
				container, currLen, err = parseECS(region, offset, maxPageSize, container)
				if err != nil {
					logrus.Errorln("tc loop paging failed", err)
					continue
				}
				pageNum++
			}

			e.m.Lock()
			if _, ok := e.ecsMap[region]; !ok {
				e.ecsMap[region] = make(map[string]*cvm.Instance)
			}
			e.m.Unlock()

			for i := range container {
				ecs := container[i]

				e.m.Lock()
				e.ecsMap[region][*ecs.InstanceId] = ecs
				e.m.Unlock()
			}
		}(region)
	}

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"ecsLens": len(e.ecsMap),
		"iden":    e.op.req.Iden,
	}).Warnln("async loop get all tc ecs success")
}

func (e *Ecs) getEcs(region string, ip *string) *cvm.Instance {
	e.m.RLock()
	defer e.m.RUnlock()
	if ecsM, ok := e.ecsMap[region]; ok {
		if ecs, ok := ecsM[*ip]; ok {
			return ecs
		}
	}
	return nil
}
