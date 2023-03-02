package ali

import (
	"time"

	"watcher4metrics/pkg/common"

	"github.com/goccy/go-json"

	cms_export20211101 "github.com/alibabacloud-go/cms-export-20211101/v2/client"
	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	"github.com/sirupsen/logrus"
)

type batchGetOperator struct {
	openapiCfg *openapi.Config

	sem *common.Semaphore
}

func (b *batchGetOperator) pull( // TODO 待真实账号测试 beta
	_ *cms.Client,
	metrics []*cms.Resource,
	ns string,
	push PushFunc,
	_ *string,
	_ []string,
	period int,
) {
	var (
		endTime   = time.Now()
		startTime = endTime.Add(time.Duration(-period) * time.Second)
	)

	for _, metric := range metrics {
		b.sem.Acquire()
		go func(metric *cms.Resource) {
			defer b.sem.Release()
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
