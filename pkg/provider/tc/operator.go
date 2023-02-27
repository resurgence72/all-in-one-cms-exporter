package tc

import (
	"fmt"
	"sync"
	"time"

	"watcher4metrics/pkg/common"

	"github.com/sirupsen/logrus"
	api "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/api/v20201106"
	com "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	tchttp "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/http"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	monitor "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/monitor/v20180724"
)

type meta struct {
	op        *operator
	clients   map[string]*monitor.Client
	namespace string
	metrics   []*monitor.MetricSet

	m sync.RWMutex
}

func newMeta(params ...interface{}) meta {
	return meta{
		op:        params[0].(*operator),
		clients:   params[1].(map[string]*monitor.Client),
		namespace: params[2].(string),
	}
}

type operator struct {
	req *TCReq
}

type PushFunc func(*transferData)

type InstanceBuilderFunc func(string) []*monitor.Instance

// 获取起止时间
func (o *operator) getRangeTime() (*string, *string) {
	endTime := time.Now().UTC()
	startTime := endTime.Add(-1 * time.Duration(TC_CMS_DELAY) * time.Second).UTC()

	sFormat, eFormat := startTime.Format(time.RFC3339), endTime.Format(time.RFC3339)
	return &sFormat, &eFormat
}

// 获取所有region
func (o *operator) getRegions() []string {
	defaultRegions := []string{"ap-shanghai"}

	credential := com.NewCredential(o.req.Sid, o.req.Skey)
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = "api.tencentcloudapi.com"
	client, _ := api.NewClient(credential, "", cpf)
	request := api.NewDescribeRegionsRequest()
	request.Product = com.StringPtr("monitor")

	var retry = func(req *api.DescribeRegionsRequest, times int) (resp *api.DescribeRegionsResponse, err error) {
		for i := 0; i < times; i++ {
			resp, err = client.DescribeRegions(req)
			if err == nil {
				return resp, nil
			}
			time.Sleep(200 * time.Millisecond)
		}
		return nil, err
	}

	resp, err := retry(request, 5)
	if err != nil {
		logrus.Errorln("tc client get regions failed ", err)
		return defaultRegions
	}

	regions := make([]string, 0, len(resp.Response.RegionSet))
	for _, region := range resp.Response.RegionSet {
		regions = append(regions, *region.Region)
	}
	return regions
}

// 获取namespace全量metrics
func (o *operator) getMetrics(
	cli *monitor.Client,
	ns string,
	filter []string,
) ([]*monitor.MetricSet, error) {
	var (
		retry = func(req *monitor.DescribeBaseMetricsRequest, times int) (resp *monitor.DescribeBaseMetricsResponse, err error) {
			for i := 0; i < times; i++ {
				resp, err = cli.DescribeBaseMetrics(req)
				if err == nil {
					return resp, nil
				}
				time.Sleep(time.Duration((i+1)*200) * time.Millisecond)
			}
			return nil, err
		}
	)

	// 获取所有ns下metrics指标
	request := monitor.NewDescribeBaseMetricsRequest()
	request.Namespace = com.StringPtr(ns)

	// 返回的resp是一个DescribeBaseMetricsResponse的实例，与请求对象对应
	resp, err := retry(request, 5)
	if err != nil {
		logrus.Errorln("tc DescribeBaseMetrics failed ", err)
		return nil, err
	}

	isFilter := filter != nil
	fm := make(map[string]struct{}, len(filter))
	for _, f := range filter {
		fm[f] = struct{}{}
	}

	metrics := make([]*monitor.MetricSet, 0, len(resp.Response.MetricSet))
	for i := range resp.Response.MetricSet {
		ms := resp.Response.MetricSet[i]
		if isFilter {
			if _, ok := fm[*ms.MetricName]; !ok {
				continue
			}
		}
		metrics = append(metrics, ms)
	}
	return metrics, nil
}

// 获取监控数据
func (o *operator) getMonitorData(
	clients map[string]*monitor.Client,
	metrics []*monitor.MetricSet,
	allowRegion []string,
	buildFunc InstanceBuilderFunc,
	batch int,
	ns string,
	push PushFunc,
) {
	var (
		retry = func(cli *monitor.Client, req *monitor.GetMonitorDataRequest, times int) (resp *monitor.GetMonitorDataResponse, err error) {
			for i := 0; i < times; i++ {
				resp, err = cli.GetMonitorData(req)
				if err == nil {
					return resp, nil
				}
				time.Sleep(time.Duration((i+1)*200) * time.Millisecond)
			}
			return nil, err
		}
		apiInstancesN = 200
		sem           = common.Semaphore(batch)
	)

	regions := make(map[string]struct{})
	for _, r := range allowRegion {
		regions[r] = struct{}{}
	}

	for region, cli := range clients {
		if len(regions) > 0 {
			if _, ok := regions[region]; !ok {
				continue
			}
		}

		instances := buildFunc(region)
		for _, metric := range metrics {
			sem.Acquire()
			go func(cli *monitor.Client, metric *monitor.MetricSet, instances []*monitor.Instance) {
				defer sem.Release()
				if len(instances) == 0 {
					return
				}
				request := monitor.NewGetMonitorDataRequest()
				request.Period = com.Uint64Ptr(60)
				request.StartTime, request.EndTime = o.getRangeTime()
				// tc 指定Namespace需要强制大写，否则报错
				request.Namespace = com.StringPtr(ns)
				request.MetricName = metric.MetricName

				pageSize := len(instances)/apiInstancesN + 1
				for i := 0; i < pageSize; i++ {
					// 每次请求instances.N为200 否则会超时
					// 0-199 200-299
					start, end := i*apiInstancesN, (i+1)*apiInstancesN-1
					if pageSize == 1 || len(instances) < end {
						end = len(instances)
					}
					request.Instances = instances[start:end]
					resp, err := retry(cli, request, 5)
					if err != nil {
						logrus.WithFields(logrus.Fields{
							"err":    err,
							"region": region,
							"metric": *metric.MetricName,
							"total":  len(instances),
						}).Errorln("GetMonitorData failed")
						return
					}

					transfer := &transferData{
						points:    resp.Response.DataPoints,
						metric:    *metric.MetricName,
						unit:      *metric.Unit,
						region:    cli.GetRegion(),
						requestID: *resp.Response.RequestId,
					}

					// push 夜莺
					go push(transfer)
				}
				time.Sleep(time.Duration(200) * time.Millisecond)
			}(cli, metric, instances)
		}
	}
}

func (o *operator) commonRequest(
	region string,
	domain string,
	version string,
	apiName string,
	offset int,
	limit int,
	queryParams map[string]interface{},
) ([]byte, error) {
	var retry = func(cli *com.Client, req *tchttp.CommonRequest, times int) (resp *tchttp.CommonResponse, err error) {
		for i := 0; i < times; i++ {
			resp = tchttp.NewCommonResponse()
			err := cli.Send(req, resp)
			if err == nil {
				return resp, nil
			}
			time.Sleep(time.Duration((i+1)*200) * time.Millisecond)
		}
		return nil, err
	}

	credential := com.NewCredential(
		o.req.Sid,
		o.req.Skey,
	)
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = fmt.Sprintf("%s.tencentcloudapi.com", domain)
	cpf.HttpProfile.ReqMethod = "POST"
	//创建common client
	client := com.NewCommonClient(
		credential,
		region,
		cpf,
	)
	// 创建common request，依次传入产品名、产品版本、接口名称
	request := tchttp.NewCommonRequest(
		domain,
		version,
		apiName,
	)

	params := map[string]interface{}{
		"Offset": offset,
		"Limit":  limit,
	}

	if queryParams != nil && len(queryParams) > 0 {
		for k, v := range queryParams {
			params[k] = v
		}
	}

	// 设置action所需的请求数据
	if err := request.SetActionParameters(params); err != nil {
		return nil, err
	}

	//发送请求
	resp, err := retry(client, request, 5)
	if err != nil || resp == nil {
		return nil, err
	}

	// 获取响应结果
	return resp.GetBody(), nil
}

// dName 腾讯拉取指标需要指定 monitor.Dimension的 Name
// dValue 表示每个实例 Name 的唯一 key
func (o *operator) buildInstances(
	dName string,
	dValues []*string,
	domains map[string]string, // waf 不但需要传入 domain 唯一的dimension，还需要同时传入 edition； 这里去兼容多个 dimension
) []*monitor.Instance {
	instances := make([]*monitor.Instance, 0, len(dValues))
	for _, v := range dValues {
		var d []*monitor.Dimension
		for k, v := range domains {
			d = append(d, &monitor.Dimension{
				Name:  com.StringPtr(k),
				Value: com.StringPtr(v),
			})
		}

		instances = append(instances, &monitor.Instance{
			Dimensions: append(d, &monitor.Dimension{
				Name:  com.StringPtr(dName),
				Value: com.StringPtr(*v),
			}),
		})
	}
	return instances
}
