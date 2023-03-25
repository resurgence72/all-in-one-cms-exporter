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

	ecsMap map[string]*ecs.Instance
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
		nil,
		nil,
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
		[]string{"instanceId"},
	)
}

func (e *Ecs) getIP(id string) *ecs.Instance {
	e.m.Lock()
	defer e.m.Unlock()

	if ip, ok := e.ecsMap[id]; ok {
		return ip
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
		series := &common.MetricValue{
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
			"memory":     strconv.Itoa(ip.Memory / 1024),

			// 指标单位
			"unit_name": transfer.unit,
			"provider":  ProviderName,
			"iden":      e.op.req.Iden,
			"namespace": e.namespace,
		}

		// 注入ecs的Tags
		for _, tag := range ip.Tags.Tag {
			if tag.TagValue != "" {
				tagsMap[tag.TagKey] = tag.TagValue
			}
		}

		series.BuildAndShift(tagsMap)
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

	if e.ecsMap == nil {
		e.ecsMap = make(map[string]*ecs.Instance)
	}

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

			e.m.Lock()
			e.ecsMap[ecs.InstanceId] = &ecs
			e.m.Unlock()
		}
	})

	logrus.WithFields(logrus.Fields{
		"ecsLens": len(e.ecsMap),
		"iden":    e.op.req.Iden,
	}).Warnln("async loop success, get all ecs instance")
}
