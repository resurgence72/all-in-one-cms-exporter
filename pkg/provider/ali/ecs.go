package ali

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"
	"github.com/sirupsen/logrus"
)

type Ecs struct {
	meta
}

func init() {
	registers[ACS_ECS_DASHBOARD] = new(Ecs)
}

func (e *Ecs) Inject(params ...any) common.MetricsGetter {
	return &Ecs{meta: newMeta(params...)}
}

func (e *Ecs) GetNamespace() string {
	return e.namespace
}

func (e *Ecs) GetMetrics() error {
	metrics, err := e.op.getMetrics(
		e.client,
		e.namespace,
		map[string]string{"productCategory": "ecs"},
		[]string{
			"CPUUtilization",
			"memory_usedutilization",
			"concurrentConnections",
			"vm.ProcessCount", // 系统进程总数

			// IP维度公网流入带宽
			"VPC_PublicIP_InternetInRate",
			// IP维度公网流出带宽
			"VPC_PublicIP_InternetOutRate",
			// IP维度公网流出带宽使用率
			"VPC_PublicIP_InternetOutRate_Percent",

			// EIP实例维度流入带宽
			"eip_InternetInRate",
			// EIP实例维度流出带宽
			"eip_InternetOutRate",

			// 内网带宽
			"IntranetInRate",
			"IntranetOutRate",
			// 内网流量
			"IntranetIn",
			"IntranetOut",

			// 经典网络公网流入流量
			"InternetIn",
			// 经典网络公网流出流量
			"InternetOut",
			// 经典网络公网流入带宽
			"InternetInRate",
			// 经典网络公网流出带宽
			"InternetOutRate",
			// 经典网络公网流出带宽使用率
			"InternetOutRate_Percent",

			// 丢包
			"networkcredit_limit_overflow_errorpackets", // 实例网络能力超限丢包数（Count）
			"packetInDropRates",                         // 入方向丢包率
			"packetOutDropRates",                        // 出方向丢包率

			// 磁盘读取bps
			"DiskReadBPS",
			// 所有磁盘每秒读取次数
			"DiskReadIOPS",
			// 写入bps
			"DiskWriteBPS",
			// 所有磁盘每秒写入次数
			"DiskWriteIOPS",
			"diskusage_utilization",

			// load
			"load_1m",
			"load_5m",
			"load_15m",

			"net_tcpconnection",
		},
	)
	if err != nil {
		return err
	}
	e.metrics = metrics
	return nil
}

func (e *Ecs) Collector() {
	e.op.getMetricLastData(
		e.client,
		e.metrics,
		e.namespace,
		e.push,
		nil,
		[]string{"instanceId", "state", "device"},
	)
}

func (e *Ecs) loadECS(id string) *ecs.Instance {
	if es, ok := e.mp.Load(id); ok {
		return es.(*ecs.Instance)
	}
	return nil
}

func (e *Ecs) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["instanceId"]
		if !ok {
			continue
		}

		instanceID := p.(string)
		ip := e.loadECS(instanceID)
		if ip == nil {
			continue
		}

		// 获取tags 和 endpoint
		var (
			ips = common.SliceStringPool.Get().([]string)[:0]
		)
		defer func() {
			common.SliceStringPool.Put(ips)
		}()

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

		var priIP string
		switch ip.InstanceNetworkType {
		case "classic":
			priIP = strings.Join(ip.InnerIpAddress.IpAddress, ",")
		case "vpc":
			priIP = strings.Join(ip.VpcAttributes.PrivateIpAddress.IpAddress, ",")
		}

		series := e.op.buildSeries(pubIP, "ecs", transfer.metric, point)
		series.TagsMap = map[string]string{
			"region":                 ip.RegionId,
			"instance_name":          ip.InstanceName,
			"instance_id":            ip.InstanceId,
			"instance_status":        ip.Status,
			"instance_network_type":  ip.InstanceNetworkType,
			"instance_type":          ip.InstanceType,
			"os_name":                ip.OSName,
			"os_type":                ip.OSType,
			"image_id":               ip.ImageId,
			"gpu_spec":               ip.GPUSpec,
			"gpu_amount":             strconv.Itoa(ip.GPUAmount),
			"hostname":               ip.Hostname,
			"internet_max_bw_out":    strconv.Itoa(ip.InternetMaxBandwidthOut),
			"internet_max_bw_in":     strconv.Itoa(ip.InternetMaxBandwidthIn),
			"local_storage_capacity": strconv.FormatInt(ip.LocalStorageCapacity, 10),
			"local_storage_amount":   strconv.Itoa(ip.LocalStorageAmount),

			"public_ip":  pubIP,
			"private_ip": priIP,
			"cpu":        strconv.Itoa(ip.Cpu),

			// 指标单位
			"unit_name": transfer.unit,
			"provider":  ProviderName,
			"iden":      e.op.req.Iden,
			"namespace": e.namespace,
		}
		// 适配不同Metric tag
		for _, tag := range []string{"state", "device"} {
			if _, ok = point[tag]; ok {
				v := point[tag].(string)
				if len(v) > 0 {
					series.TagsMap[tag] = v
				}
			}
		}

		if ip.Memory/1024 == 0 {
			series.TagsMap["memory"] = "0.5"
		} else {
			series.TagsMap["memory"] = strconv.Itoa(ip.Memory / 1024)
		}

		// 注入ecs的Tags
		for _, tag := range ip.Tags.Tag {
			if tag.TagValue != "" {
				series.TagsMap[tag.TagKey] = tag.TagValue
			}
		}

		series.BuildAndShift()
	}
}

func (e *Ecs) AsyncMeta(ctx context.Context) {
	var (
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

	e.op.async(e.op.getRegions(), func(region string, wg *sync.WaitGroup) {
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

			e.mp.Store(ecs.InstanceId, &ecs)
		}
	})

	logrus.WithFields(logrus.Fields{
		"ecsLens": e.op.mapLens(e.mp),
		"iden":    e.op.req.Iden,
	}).Warnln("async loop success, get all ecs instance")
}
