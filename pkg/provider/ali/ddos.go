package ali

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/ddoscoo"
	"github.com/sirupsen/logrus"
)

type DDos struct {
	meta

	ddosMap map[string]string
}

func init() {
	registers[ACS_DDOS_IP] = new(DDos)
}

func (d *DDos) Inject(params ...interface{}) common.MetricsGetter {
	return &DDos{meta: newMeta(params)}
}

func (d *DDos) GetNamespace() string {
	return d.namespace
}

func (d *DDos) GetMetrics() error {
	metrics, err := d.op.getMetrics(
		d.client,
		d.namespace,
		nil,
		nil,
	)
	if err != nil {
		return err
	}
	d.metrics = metrics
	return nil
}

func (d *DDos) Collector() {
	for _, namespace := range []string{"acs_ddosdip", "acs_newbgpddos"} {
		d.op.getMetricLastData(
			d.client,
			d.metrics,
			5,
			namespace,
			d.push,
			nil,
			[]string{"InstanceId", "domain"},
		)
	}
}

func (d *DDos) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["InstanceId"]
		if !ok {
			continue
		}

		instanceID := p.(string)
		ip, ok := d.ddosMap[instanceID]
		if !ok {
			continue
		}

		n9e := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("ddos", transfer.metric),
			ValueUntyped: point.Value(),
		}

		n9e.Endpoint = ip

		tagsMap := map[string]string{
			"instance_id": instanceID,
			"provider":    ProviderName,
			"iden":        d.op.req.Iden,
			"namespace":   d.namespace,
			"unit_name":   transfer.unit,
		}

		if point["ip"] == nil {
			// 两种 一种有ip
			// 一种有domain
			// 没有ip 需要加入domain标签
			tagsMap["domain"] = point["domain"].(string)
		}

		n9e.BuildAndShift(tagsMap)
	}
}

func (d *DDos) AsyncMeta(ctx context.Context) {
	var (
		wg          sync.WaitGroup
		maxPageSize = 100
		// 构造ddosIds请求
		parseDdosIds = func(
			region string,
			pageNum int,
			container []ddoscoo.Instance,
		) ([]ddoscoo.Instance, int, error) {
			bytes, err := d.op.commonRequest(
				region,
				"ddoscoo",
				"2020-01-01",
				"DescribeInstanceIds",
				pageNum,
				maxPageSize,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(ddoscoo.DescribeInstanceIdsResponse)
			if err := parser.Parser().Unmarshal(bytes, resp); err != nil {
				return nil, 0, err
			}
			return append(container, resp.InstanceIds...), len(resp.InstanceIds), nil
		}
		// 构造ddosDetails请求
		parseDdosDetails = func(
			region string,
			pageNum int,
			container []ddoscoo.InstanceDetail,
			paramsBuilder func() map[string]string,
		) ([]ddoscoo.InstanceDetail, int, error) {
			bytes, err := d.op.commonRequest(
				region,
				"ddoscoo",
				"2020-01-01",
				"DescribeInstanceDetails",
				pageNum,
				maxPageSize,
				paramsBuilder(),
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(ddoscoo.DescribeInstanceDetailsResponse)
			if err := parser.Parser().Unmarshal(bytes, resp); err != nil {
				return nil, 0, err
			}
			return append(container, resp.InstanceDetails...), len(resp.InstanceDetails), nil
		}
	)

	if d.ddosMap == nil {
		d.ddosMap = make(map[string]string)
	}

	// ddos产品只需要指定两个region
	for _, region := range []string{"cn-hangzhou", "ap-southeast-1"} {
		wg.Add(1)
		go func(region string) {
			defer wg.Done()
			var (
				pageNum   = 1
				container []ddoscoo.Instance
			)
			// 1. 获取ddos的instance
			container, currLen, err := parseDdosIds(region, pageNum, container)
			if err != nil {
				logrus.Errorln("AsyncMeta err ", err, region)
				return
			}
			for currLen == maxPageSize {
				pageNum++
				container, currLen, err = parseDdosIds(region, pageNum, container)
				if err != nil {
					logrus.Errorln("AsyncMeta paging err ", err)
					continue
				}
			}

			if len(container) == 0 {
				return
			}

			// 2. 拿着所有id请求DescribeInstanceDetails接口拿id对应的ip
			var (
				detailPageNum   = 1
				detailContainer []ddoscoo.InstanceDetail
				// 设置param 构造Detail接口的 instances.N 参数
				paramsBuilder = func() map[string]string {
					params := make(map[string]string, len(container))
					for i, c := range container {
						params[fmt.Sprintf("InstanceIds.%d", i+1)] = c.InstanceId
					}
					return params
				}
			)
			// 1. 获取ddos的instance
			detailContainer, currLen, err = parseDdosDetails(region, detailPageNum, detailContainer, paramsBuilder)
			if err != nil {
				return
			}
			for currLen == maxPageSize {
				detailPageNum++
				detailContainer, currLen, err = parseDdosDetails(region, detailPageNum, detailContainer, paramsBuilder)
				if err != nil {
					logrus.Errorln("AsyncMeta paging err ", err)
					continue
				}
			}

			for i := range detailContainer {
				ddos := detailContainer[i]
				if len(ddos.EipInfos) == 0 {
					continue
				}

				ips := make([]string, 0, len(ddos.EipInfos))
				for ii := range ddos.EipInfos {
					ips = append(ips, ddos.EipInfos[ii].Eip)
				}

				// 设置ddosmap k 为 ddos instanceID, v 为当前所生效的 ip地址
				d.ddosMap[ddos.InstanceId] = strings.Join(ips, ",")
			}
		}(region)
	}

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"ddosLens": len(d.ddosMap),
		"iden":     d.op.req.Iden,
	}).Warnln("async loop success, get all ddos instance")
}
