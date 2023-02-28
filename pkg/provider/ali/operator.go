package ali

import (
	"github.com/alibabacloud-go/tea/tea"
	"strconv"
	"strings"
	"time"
	"watcher4metrics/config"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/goccy/go-json"

	cms_export20211101 "github.com/alibabacloud-go/cms-export-20211101/v2/client"
	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/endpoints"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/responses"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/vpc"
	"github.com/sirupsen/logrus"
)

type batchGetOperator struct {
	openapiCfg *openapi.Config
}

func (b *batchGetOperator) pull( // TODO 待真实账号测试 beta
	_ *cms.Client,
	metrics []*cms.Resource,
	batch int,
	ns string,
	push PushFunc,
	_ *string,
	_ []string,
	period int,
) {
	var (
		sem       = common.NewSemaphore(batch)
		endTime   = time.Now()
		startTime = endTime.Add(time.Duration(-period) * time.Second)
	)

	for _, metric := range metrics {
		sem.Acquire()
		go func(metric *cms.Resource) {
			defer sem.Release()
			// 首次获取 cursor
			cli, err := cms_export20211101.NewClient(b.openapiCfg)
			if err != nil {
				logrus.Errorln("cms exporter newClient failed", err)
				return
			}

			cursorResp, err := cli.Cursor(&cms_export20211101.CursorRequest{
				EndTime:   tea.Int64(endTime.UnixMilli()),
				Metric:    tea.String(metric.MetricName),
				Namespace: tea.String(ns),
				Period:    tea.Int32(int32(period)),
				StartTime: tea.Int64(startTime.UnixMilli()),
			})
			if err != nil || !*cursorResp.Body.Success {
				logrus.Errorln("client get cursor failed", err, cursorResp.String())
				return
			}

			cursor := cursorResp.Body.Data.Cursor
			tmpMap := make(map[string]Points)
			for cursor != nil {
				// TODO retry
				batchResp, err := cli.BatchGet(&cms_export20211101.BatchGetRequest{
					Cursor:    cursor,
					Length:    tea.Int32(3000),
					Metric:    tea.String(metric.MetricName),
					Namespace: tea.String(ns),
				})
				if err != nil || !*batchResp.Body.Success {
					logrus.Errorln("client batchGet failed", err, batchResp.String())
					return
				}

				var tmpPoints Points
				for _, record := range batchResp.Body.Data.Records {

					// 序列化 MeasureLabels
					measureLabels := make([]string, 0)
					ml := *record.MeasureLabels[0]
					if err = json.Unmarshal([]byte(ml), &measureLabels); err != nil {
						continue
					}

					// 序列化 MeasureValues
					measureValues := make([]float64, 0)
					mv := *record.MeasureValues[0]
					if err = json.Unmarshal([]byte(mv), &measureValues); err != nil {
						continue
					}

					// 序列化 Labels
					labels := make([]string, 0)
					l := *record.Labels[0]
					if err = json.Unmarshal([]byte(l), &labels); err != nil {
						continue
					}

					// 序列化 LabelValues
					labelValues := make([]string, 0)
					lv := *record.LabelValues[0]
					if err = json.Unmarshal([]byte(lv), &labelValues); err != nil {
						continue
					}

					// 注入数据
					point := make(Point)
					point["timestamp"] = record.Timestamp
					for i, ml := range measureLabels {
						mv := measureValues[i]
						point[ml] = mv
					}
					for i, l := range labels {
						lv := labelValues[i]
						point[l] = lv
					}
					tmpPoints = append(tmpPoints, point)
				}

				tmpMap[*batchResp.Body.RequestId] = tmpPoints
				cursor = batchResp.Body.Data.Cursor
			}

			for requestId, points := range tmpMap {
				go push(&transferData{
					points:    points,
					metric:    metric.MetricName,
					unit:      metric.Unit,
					requestID: requestId,
				})
			}
			time.Sleep(time.Duration(200) * time.Millisecond)
		}(metric)
	}

}

type operator struct {
	req *AliReq
}

// 获取namespace全量metrics
func (o *operator) getMetrics(
	cli *cms.Client,
	ns string,
	labels map[string]string, // 多个产品公用一个ns,这时需要指定labels(metricCategory) 做子类区分
	filter []string,
) ([]*cms.Resource, error) {
	var retry = func(req *cms.DescribeMetricMetaListRequest, times int) (resp *cms.DescribeMetricMetaListResponse, err error) {
		for i := 0; i < times; i++ {
			resp, err = cli.DescribeMetricMetaList(req)
			if err == nil {
				return resp, nil
			}
			time.Sleep(time.Duration((i+1)*200) * time.Millisecond)
		}
		return nil, err
	}

	request := cms.CreateDescribeMetricMetaListRequest()
	request.Scheme = "https"
	request.Namespace = ns
	request.PageSize = requests.NewInteger(1000)

	if len(labels) > 0 {
		var lbs []map[string]string
		for k, v := range labels {
			lbs = append(lbs, map[string]string{"name": k, "value": v})
		}

		bs, err := json.Marshal(lbs)
		if err == nil {
			request.Labels = string(bs)
		}
	}

	resp, err := retry(request, 5)
	if err != nil {
		return nil, err
	}

	isFilter := filter != nil
	fm := make(map[string]struct{}, len(filter))
	for _, f := range filter {
		fm[f] = struct{}{}
	}

	metrics := make([]*cms.Resource, 0, len(resp.Resources.Resource))
	for i := range resp.Resources.Resource {
		r := &resp.Resources.Resource[i]

		if isFilter {
			if _, ok := fm[r.MetricName]; !ok {
				continue
			}
		}
		metrics = append(metrics, r)
	}
	return metrics, nil
}

// 获取ali每次请求的startTime 和 EndTime
//func (o *operator) getRangeTime() (string, string) {
//	endTime := time.Now()
//	startTime := endTime.Add(-1 * time.Duration(ALI_CMS_DELAY) * time.Second)
//
//	format := "2006-01-02 15:04:05"
//	return startTime.Format(format), endTime.Format(format)
//}

// 获取全量region
func (o *operator) getRegions() []string {
	defaultRegions := []string{"cn-shanghai"}
	credential := credentials.NewAccessKeyCredential(
		o.req.Ak,
		o.req.As,
	)
	client, err := vpc.NewClientWithOptions("cn-shanghai", sdk.NewConfig(), credential)
	if err != nil {
		logrus.Errorln("NewClientWithOptions failed ", err)
		return defaultRegions
	}

	request := vpc.CreateDescribeRegionsRequest()
	request.Scheme = "https"
	response, err := client.DescribeRegions(request)
	if err != nil {
		logrus.Errorln("DescribeRegions failed ", err)
		return defaultRegions
	}

	regions := make([]string, 0, len(response.Regions.Region))
	for _, region := range response.Regions.Region {
		regions = append(regions, region.RegionId)
	}
	return regions
}

func (o *operator) wait() {
	// 随机wait 方式ali-sdk并发超限
	common.JitterWait()
}

func (o *operator) pull(
	cli *cms.Client,
	metrics []*cms.Resource,
	batch int,
	ns string,
	push PushFunc,
	ds *string,
	groupBy []string,
	period int,
) {
	var (
		retry = func(cli *cms.Client, req *cms.DescribeMetricLastRequest, times int) (resp *cms.DescribeMetricLastResponse, err error) {
			for i := 0; i < times; i++ {
				resp, err = cli.DescribeMetricLast(req)
				if err == nil {
					return resp, nil
				}
				time.Sleep(time.Duration((i+1)*200) * time.Millisecond)
			}
			return nil, err
		}
		sem     = common.NewSemaphore(batch)
		endTime = time.Now().Format("2006-01-02 15:04:05")
	)
	for _, metric := range metrics {
		sem.Acquire()
		go func(metric *cms.Resource) {
			defer sem.Release()
			request := cms.CreateDescribeMetricLastRequest()
			request.Scheme = "https"
			request.Namespace = ns
			request.MetricName = metric.MetricName
			request.Period = strconv.Itoa(period)
			request.EndTime = endTime
			request.Length = "1500"
			if ds != nil {
				request.Dimensions = *ds
			}

			if len(groupBy) > 0 {
				bs, err := json.Marshal(map[string][]string{"groupby": groupBy})
				if err == nil {
					request.Express = string(bs)
				}
			}

			resp, err := retry(cli, request, 5)

			if err != nil || !resp.Success {
				logrus.WithFields(logrus.Fields{
					"namespace": ns,
					"metric":    metric.MetricName,
					"err":       err,
					"reason":    resp,
				}).Errorln("DescribeMetricLast failed")
				return
			}

			var points Points
			err = parser.Parser().Unmarshal([]byte(resp.Datapoints), &points)
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"namespace": ns,
					"metric":    metric.MetricName,
					"err":       err,
				}).Errorln("json.Unmarshal failed")
				return
			}

			if len(points) == 0 {
				return
			}

			tmpMap := map[string]Points{resp.RequestId: points}
			nextToken := resp.NextToken
			for nextToken != "" {
				request.NextToken = nextToken
				resp, err := retry(cli, request, 5)
				if err != nil {
					break
				}
				var tmpPoints Points
				if err = parser.Parser().Unmarshal([]byte(resp.Datapoints), &tmpPoints); err == nil {
					if len(tmpPoints) != 0 {
						tmpMap[resp.RequestId] = tmpPoints
					}
				}
				nextToken = resp.NextToken
			}

			// 3. 将数据转换格式推送至rw
			for requestId, points := range tmpMap {
				transfer := &transferData{
					points:    points,
					metric:    metric.MetricName,
					unit:      metric.Unit,
					requestID: requestId,
				}
				go push(transfer)
			}
			time.Sleep(time.Duration(200) * time.Millisecond)
		}(metric)
	}
}

// 获取监控数据
func (o *operator) getMetricLastData(
	cli *cms.Client,
	metrics []*cms.Resource,
	batch int,
	ns string,
	push PushFunc,
	ds *string, // Dimensions 维度
	groupBy []string,
) {
	var puller metricsDataPuller
	if config.Get().Provider.Ali.BatchGetEnabled {
		puller = &batchGetOperator{openapiCfg: &openapi.Config{
			AccessKeyId:     tea.String(o.req.Ak),
			AccessKeySecret: tea.String(o.req.As),
		}}
	} else {
		puller = o
	}

	o.wait()
	// 具体的指标采集动作
	puller.pull(cli, metrics, batch, ns, push, ds, groupBy, o.req.Dur)
}

func (o *operator) commonRequest(
	region string,
	domain string,
	version string,
	apiName string,
	pageNum int,
	pageSize int,
	queryParams map[string]string,
) ([]byte, error) {
	var retry = func(cli *sdk.Client, req *requests.CommonRequest, times int) (resp *responses.CommonResponse, err error) {
		endpoint, err := endpoints.Resolve(&endpoints.ResolveParam{Product: domain, RegionId: region})
		if err != nil {
			endpoint, err = cli.GetEndpointRules(region, domain)
			if err != nil {
				return nil, err
			}
		}
		req.Domain = endpoint

		for i := 0; i < times; i++ {
			resp, err = cli.ProcessCommonRequest(req)
			if err == nil {
				return resp, nil
			} else if strings.HasPrefix(
				err.Error(),
				// 忽略endpoint不匹配的报错，切换ep,重新循环retry
				"SDK.ServerError\nErrorCode: InvalidOperation",
			) {
				endpoint, err = cli.GetEndpointRules(region, domain)
				if err != nil {
					return nil, err
				}
			} else if strings.HasPrefix(
				// 忽略endpoint不匹配的报错，切换ep,重新循环retry
				err.Error(),
				"SDK.ServerError\nErrorCode: OperationFailed",
			) {
				endpoint, err = cli.GetEndpointRules(region, domain)
				if err != nil {
					return nil, err
				}
			}
			req.Domain = endpoint
			time.Sleep(time.Duration((i+1)*200) * time.Millisecond)
		}
		return nil, err
	}

	client, err := sdk.NewClientWithAccessKey(
		region,
		o.req.Ak,
		o.req.As,
	)
	if err != nil {
		return nil, err
	}
	defer client.Shutdown()

	client.SetConnectTimeout(time.Duration(30) * time.Second)
	client.SetReadTimeout(time.Duration(30) * time.Second)
	client.EndpointType = "regional"

	request := requests.NewCommonRequest()
	request.Version = version
	request.ApiName = apiName
	request.QueryParams["PageNumber"] = strconv.Itoa(pageNum)
	request.QueryParams["PageSize"] = strconv.Itoa(pageSize)

	// 设置除分页参数外的自定义参数
	if queryParams != nil && len(queryParams) > 0 {
		for k, v := range queryParams {
			request.QueryParams[k] = v
		}
	}

	resp, err := retry(client, request, 5)
	if err != nil || !resp.IsSuccess() {
		return nil, err
	}
	return resp.BaseResponse.GetHttpContentBytes(), nil
}
