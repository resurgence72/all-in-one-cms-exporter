package google

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/duration"
	"google.golang.org/genproto/googleapis/api"
	metricpb "google.golang.org/genproto/googleapis/api/metric"

	"watcher4metrics/pkg/common"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

type operator struct {
	req *GoogleReq

	projects sync.Map
}

type PushFunc func(*transferData)

func (o *operator) getPointValue(valueType metricpb.MetricDescriptor_ValueType, point *monitoringpb.Point) any {
	switch valueType {
	case metricpb.MetricDescriptor_INT64:
		return point.GetValue().GetInt64Value()
	case metricpb.MetricDescriptor_DOUBLE:
		return point.GetValue().GetDoubleValue()
	case metricpb.MetricDescriptor_BOOL:
		if point.GetValue().GetBoolValue() {
			return 1
		} else {
			return 0
		}
	case metricpb.MetricDescriptor_DISTRIBUTION:
		return point.GetValue().GetDistributionValue().Mean
	case metricpb.MetricDescriptor_STRING:
		return 0
	default:
		return 0
	}
}

func (o *operator) getMetrics(
	cli *monitoring.MetricClient,
	metricPrefix string,
) ([]*metricpb.MetricDescriptor, error) {
	var p string
	// 遍历一次即可，拿到任意一个project
	o.projects.Range(func(key, value any) bool {
		p = key.(string)
		return false
	})

	req := &monitoringpb.ListMetricDescriptorsRequest{
		Name:   fmt.Sprintf("projects/%s", p),
		Filter: fmt.Sprintf(`metric.type=starts_with("%s")`, metricPrefix),
	}

	ctx, _ := context.WithTimeout(context.TODO(), 30*time.Second)
	descriptors := cli.ListMetricDescriptors(ctx, req)

	var metrics []*metricpb.MetricDescriptor
	for {
		descriptor, err := descriptors.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			logrus.Errorln("could not ListMetricDescriptors", err)
			return nil, err
		}

		switch *descriptor.LaunchStage.Enum() {
		// TODO 只考虑GA的metrics
		case api.LaunchStage_GA:
			metrics = append(metrics, descriptor)
		case api.LaunchStage_BETA:
		case api.LaunchStage_ALPHA:
		default:
		}
	}
	return metrics, nil
}

func (o *operator) getRangeTime() (int64, int64) {
	now := time.Now().UTC()
	end := now.Add(time.Minute * -4)
	start := end.Add(time.Second * -time.Duration(o.req.Dur))
	return start.Unix(), end.Unix()
}

func (o *operator) listTimeSeries(
	cli *monitoring.MetricClient,
	metrics []*metricpb.MetricDescriptor,
	batch int,
	push PushFunc,
	groupBy []string,
) {
	var (
		wg                 sync.WaitGroup
		sem                = common.NewSemaphore(batch)
		startTime, endTime = o.getRangeTime()
	)

	o.projects.Range(func(k, v any) bool {
		pid := k.(string)

		for _, metric := range metrics {
			sem.Acquire()
			wg.Add(1)

			go func(metric *metricpb.MetricDescriptor, pid string) {
				defer func() {
					sem.Release()
					wg.Done()
				}()
				req := &monitoringpb.ListTimeSeriesRequest{
					Name:   "projects/" + pid,
					Filter: fmt.Sprintf(`metric.type="%s"`, metric.Type),
					Interval: &monitoringpb.TimeInterval{
						StartTime: &timestamp.Timestamp{
							Seconds: startTime,
						},
						EndTime: &timestamp.Timestamp{
							Seconds: endTime,
						},
					},
				}

				if len(groupBy) > 0 {
					req.Aggregation = &monitoringpb.Aggregation{
						AlignmentPeriod: &duration.Duration{
							Seconds: int64(time.Duration(o.req.Dur) * time.Second),
						},
						PerSeriesAligner:   monitoringpb.Aggregation_ALIGN_SUM,
						CrossSeriesReducer: monitoringpb.Aggregation_REDUCE_SUM,
						GroupByFields:      groupBy,
					}

					if *metric.MetricKind.Enum() == metricpb.MetricDescriptor_CUMULATIVE {
						req.Aggregation.PerSeriesAligner = monitoringpb.Aggregation_ALIGN_RATE
					}
				}

				ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
				defer cancel()
				it := cli.ListTimeSeries(ctx, req)

				var points []*monitoringpb.TimeSeries
				for {
					point, err := it.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						logrus.Errorln("could not read time series value ", err)
						return
					}
					points = append(points, point)
				}

				transfer := &transferData{
					points: points,
					metric: metric,
				}
				go push(transfer)
			}(metric, pid)
		}

		return true
	})

	go func() {
		wg.Wait()
		cli.Close()
	}()
}

func (o *operator) buildMetric(source string) string {
	ss := strings.Split(source, "/")
	return strings.Join(ss[len(ss)-2:], "_")
}

func (o *operator) getZones() []string {
	return []string{
		"us-central1-a",
		"us-east4-a",
		"us-west1-b",
		"us-west2-a",
		"asia-east1-a",
		"asia-east1-b",
		"asia-east1-c",
		"asia-east2-a",
		"asia-northeast1-a",
		"asia-southeast1-a",
		"asia-southeast1-b",
		"asia-southeast1-c",
		"asia-southeast2-a",
		"europe-west3-a",
		"southamerica-east1-a",
	}
}
