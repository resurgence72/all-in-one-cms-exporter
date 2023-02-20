package tc

import (
	"context"
	"github.com/goccy/go-json"
	"strconv"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	cvm "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cvm/v20170312"
	monitor "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/monitor/v20180724"
	tag "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tag/v20180813"
	"watcher4metrics/pkg/common"
)

type EcsIP struct {
	op         *operator
	clients    map[string]*monitor.Client
	ecsMap     map[string]map[string]*cvm.Instance
	projectMap map[uint64]string
	namespace  string
	metrics    []*monitor.MetricSet

	m sync.RWMutex
}

func init() {
	registers[QCE_CVM] = new(EcsIP)
}

func (e *EcsIP) Inject(params ...interface{}) common.MetricsGetter {
	return &EcsIP{
		op:        params[0].(*operator),
		clients:   params[1].(map[string]*monitor.Client),
		namespace: params[2].(string),
	}
}

func (e *EcsIP) GetNamespace() string {
	return e.namespace
}

func (e *EcsIP) GetMetrics() error {
	metrics, err := e.op.getMetrics(
		e.clients["ap-shanghai"],
		e.namespace,
		[]string{
			// 外网平均每秒出流量速率
			"WanOuttraffic",
			// 外网平均每秒入流量速率
			"WanIntraffic",
			// 外网网卡网卡的平均每秒出包量
			"WanOutpkg",
			// 外网网卡网卡的平均每秒入包量
			"WanInpkg",
			// 外网网卡的平均每秒出流量
			"AccOuttraffic",

			// 内网出流量
			"LanOuttraffic",
			// 内网入流量
			"LanIntraffic",

			// cpu利用率
			"CpuUsage",
			// 内存利用率
			"MemUsage",
			// 磁盘利用率
			"CvmDiskUsage",
		},
	)
	if err != nil {
		return err
	}
	e.metrics = metrics
	return nil
}

func (e *EcsIP) Collector() {
	e.op.getMonitorData(
		e.clients,
		e.metrics,
		nil,
		func() InstanceBuilderFunc {
			return func(region string) []*monitor.Instance {
				return e.op.buildInstances(
					"InstanceId",
					func() []*string {
						var vs []*string
						for _, ecs := range e.ecsMap[region] {
							vs = append(vs, ecs.InstanceId)
						}
						return vs
					}(),
					nil,
				)
			}
		}(),
		10,
		e.namespace,
		e.push,
	)
}

func (e *EcsIP) push(transfer *transferData) {
	for _, point := range transfer.points {
		ecsInstanceId := point.Dimensions[0].Value

		// 不存在当前eip, 可以直接return
		ecs := e.getEcs(transfer.region, ecsInstanceId)
		if ecs == nil {
			return
		}

		for i, ts := range point.Timestamps {
			n9e := &common.MetricValue{
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
			n9e.Endpoint = strings.Join(pubIPs, ",")

			// 设置Tags TagsMap
			tagsMap := map[string]string{
				"iden":          e.op.req.Iden,
				"provider":      ProviderName,
				"region":        transfer.region,
				"namespace":     e.namespace,
				"unit_name":     transfer.unit,
				"instance_id":   *ecsInstanceId,
				"instance_name": *ecs.InstanceName,
				"cpu":           strconv.Itoa(int(*ecs.CPU)),
				"memory":        strconv.Itoa(int(*ecs.Memory)),
				// cvm的公网私网ip
				"public_ip":  n9e.Endpoint,
				"private_ip": strings.Join(priIPs, ","),
			}

			if pn, ok := e.projectMap[uint64(*ecs.Placement.ProjectId)]; ok {
				tagsMap["project_mark"] = pn
			}

			n9e.BuildAndShift(tagsMap)
			continue
		}
	}
}

func (e *EcsIP) AsyncMeta(ctx context.Context) {
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

		// 获取projectMap
		parseProjects = func() error {
			bs, err := e.op.commonRequest(
				"ap-shanghai",
				"tag",
				"2018-08-13",
				"DescribeProjects",
				0,
				1000,
				map[string]interface{}{"AllList": 1},
			)
			if err != nil {
				return err
			}

			resp := new(tag.DescribeProjectsResponse)
			if err = json.Unmarshal(bs, resp); err != nil {
				return err
			}

			for _, pro := range resp.Response.Projects {
				e.projectMap[*pro.ProjectId] = *pro.ProjectName
			}
			return nil
		}

		ecsCnt = 0
		sem    = common.Semaphore(10)
	)

	if e.ecsMap == nil {
		e.ecsMap = make(map[string]map[string]*cvm.Instance)
	}
	if e.projectMap == nil {
		e.projectMap = make(map[uint64]string)
	}

	// 获取所有projectMap
	if err := parseProjects(); err != nil {
		logrus.Errorln("tc get project map failed ", err)
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
				ecsCnt++

				// 只保存running状态
				if *ecs.InstanceState != "RUNNING" {
					continue
				}
				e.m.Lock()
				e.ecsMap[region][*ecs.InstanceId] = ecs
				e.m.Unlock()
			}
		}(region)
	}

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"ecsLens": ecsCnt,
		"iden":    e.op.req.Iden,
	}).Warnln("async loop get all tc ecs success")
}

func (e *EcsIP) getEcs(region string, ip *string) *cvm.Instance {
	e.m.RLock()
	defer e.m.RUnlock()
	if ecsM, ok := e.ecsMap[region]; ok {
		if ecs, ok := ecsM[*ip]; ok {
			return ecs
		}
	}
	return nil
}
