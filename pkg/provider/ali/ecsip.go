package ali

import (
	"context"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"
	"github.com/sirupsen/logrus"
	"sort"
	"strconv"
	"strings"
	"sync"
	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"
)

type EcsIP struct {
	op        *operator
	namespace string
	metrics   []*cms.Resource
	client    *cms.Client
	ecsipMap  map[string]*ecs.Instance

	m sync.Mutex
}

func init() {
	registers[ACS_ECS_DASHBOARD] = new(EcsIP)
}

func (e *EcsIP) Inject(params ...interface{}) common.MetricsGetter {
	return &EcsIP{
		op:        params[0].(*operator),
		client:    params[1].(*cms.Client),
		namespace: params[2].(string),
	}
}

func (e *EcsIP) GetNamespace() string {
	return e.namespace
}

func (e *EcsIP) GetMetrics() error {
	metrics, err := e.op.getMetrics(
		e.client,
		e.namespace,
		nil,
		[]string{
			"CPUUtilization",
			"memory_usedutilization",
			"Host.diskusage.utilization",

			"concurrentConnections",
			"VPC_PublicIP_InternetInRate",
			"VPC_PublicIP_InternetOutRate",
			"VPC_PublicIP_InternetOutRate_Percent",

			// 内网带宽
			"IntranetInRate",
			"IntranetOutRate",
			// 内网流量
			"IntranetIn",
			"IntranetOut",

			// 丢包
			"networkcredit_limit_overflow_errorpackets", // 实例网络能力超限丢包数（Count）
			"packetInDropRates", // 入方向丢包率
			"packetOutDropRates", // 出方向丢包率

			"vm.ProcessCount", // 系统进程总数
		},
	)
	if err != nil {
		return err
	}
	e.metrics = metrics
	return nil
}

func (e *EcsIP) Collector() {
	e.op.getMetricLastData(
		e.client,
		e.metrics,
		5,
		e.namespace,
		e.push,
		nil,
		nil,
	)
}

func (e *EcsIP) getIP(id string) *ecs.Instance {
	e.m.Lock()
	defer e.m.Unlock()

	if ip, ok := e.ecsipMap[id]; ok {
		return ip
	}
	return nil
}

func (e *EcsIP) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["InstanceId"]
		if !ok {
			continue
		}

		instanceID := p.(string)
		ip := e.getIP(instanceID)
		if ip == nil {
			continue
		}

		// 获取tags 和 endpoint
		var ips []string
		for i := range ip.PublicIpAddress.IpAddress {
			if len(ip.PublicIpAddress.IpAddress[i]) > 0 {
				ips = append(ips, ip.PublicIpAddress.IpAddress[i])
			}
		}

		if len(ip.EipAddress.IpAddress) > 0 {
			ips = append(ips, ip.EipAddress.IpAddress)
		}
		sort.Strings(ips)

		pubIP := strings.Join(ips, ",")
		n9e := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("ecs", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     pubIP,
		}

		var priIP string
		switch ip.InstanceNetworkType {
		case "classic":
			priIP = strings.Join(ip.InnerIpAddress.IpAddress, ",")
		case "vpc":
			priIP = strings.Join(ip.VpcAttributes.PrivateIpAddress.IpAddress, ",")
		}

		tagsMap := map[string]string{
			"region":        ip.RegionId,
			"provider":      ProviderName,
			"iden":          e.op.req.Iden,
			"namespace":     ACS_ECS_DASHBOARD.toString(),
			"instance_name": ip.InstanceName,
			"instance_id":   ip.InstanceId,
			"public_ip":    pubIP,
			"private_ip":    priIP,
			"cpu":           strconv.Itoa(ip.Cpu),
			"memory":        strconv.Itoa(ip.Memory / 1024),
			// 指标单位
			"unit_name": transfer.unit,
		}

		// 注入ecs的Tags
		for _, tag := range ip.Tags.Tag {
			if tag.TagValue != "" {
				tagsMap[tag.TagKey] = tag.TagValue
			}
		}

		n9e.BuildAndShift(tagsMap)
	}
}

func (e *EcsIP) AsyncMeta(ctx context.Context) {
	var (
		wg          sync.WaitGroup
		maxPageSize = 100
		parse       = func(region string, pageNum int, container []ecs.Instance) ([]ecs.Instance, int, error) {
			bytes, err := e.op.commonRequest(
				region,
				// ecs.aliyuncs.com endpoints的ecs前缀
				"ecs",
				"2014-05-26",
				"DescribeInstances",
				pageNum,
				maxPageSize,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(ecs.DescribeInstancesResponse)
			if err := parser.Parser().Unmarshal(bytes, resp); err != nil {
				return nil, 0, err
			}
			return append(container, resp.Instances.Instance...), len(resp.Instances.Instance), nil
		}
	)

	if e.ecsipMap == nil {
		e.ecsipMap = make(map[string]*ecs.Instance)
	}

	for _, region := range e.op.getRegions() {
		wg.Add(1)
		go func(region string) {
			defer wg.Done()
			var (
				pageNum   = 1
				container []ecs.Instance
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
				ecs := container[i]

				e.m.Lock()
				e.ecsipMap[ecs.InstanceId] = &ecs
				e.m.Unlock()
			}
		}(region)
	}

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"ecsIPLens": len(e.ecsipMap),
		"iden":      e.op.req.Iden,
	}).Warnln("async loop success, get all ecsIP instance")
}
