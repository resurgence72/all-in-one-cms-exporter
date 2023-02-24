package google

import (
	"context"
	"crypto/md5"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/duration"

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

const (
	INT64Type        = "INT64"
	DoubleType       = "DOUBLE"
	BoolType         = "BOOL"
	StringType       = "STRING"
	DistributionType = "DISTRIBUTION"

	GALaunchStage    = "GA"
	BETALaunchStage  = "BETA"
	ALPHALaunchStage = "ALPHA"
)

func (o *operator) getPointValue(valueType string, point *monitoringpb.Point) interface{} {
	switch valueType {
	case INT64Type:
		return point.GetValue().GetInt64Value()
	case DoubleType:
		return point.GetValue().GetDoubleValue()
	case BoolType:
		if point.GetValue().GetBoolValue() {
			return 1
		} else {
			return 0
		}
	case DistributionType:
		return point.GetValue().GetDistributionValue().Mean
	case StringType:
		return 0
	default:
		return 0
	}
}

func (o *operator) getMetrics(
	cli *monitoring.MetricClient,
	metricPrefix string,
) ([]string, error) {
	var p string
	// 遍历一次即可，拿到任意一个project
	o.projects.Range(func(key, value interface{}) bool {
		p = key.(string)
		return false
	})

	req := &monitoringpb.ListMetricDescriptorsRequest{
		Name:   p,
		Filter: fmt.Sprintf(`metric.type=starts_with("%s")`, metricPrefix),
	}

	ctx, _ := context.WithTimeout(context.TODO(), 30*time.Second)
	descriptors := cli.ListMetricDescriptors(ctx, req)

	var metrics []string
	for {
		descriptor, err := descriptors.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			logrus.Errorln("could not ListMetricDescriptors", err)
			return nil, err
		}

		switch descriptor.LaunchStage.String() {
		// TODO 只考虑GA的metrics
		case GALaunchStage:
			metrics = append(metrics, descriptor.Type)
		default:
			continue
		}
	}
	return metrics, nil
}

func (o *operator) getSeriesSum64(m map[string]string) uint64 {
	var buf strings.Builder
	for k, v := range m {
		buf.WriteString(k + v)
	}
	return o.sum64(md5.Sum([]byte(buf.String())))
}

func (o *operator) sum64(hash [md5.Size]byte) uint64 {
	var s uint64
	for i, b := range hash {
		shift := uint64((md5.Size - i - 1) * 8)
		s |= uint64(b) << shift
	}
	return s
}

func (o *operator) getRangeTime() (int64, int64) {
	now := time.Now().UTC()
	return now.Add(time.Minute * -4).Unix(), now.Add(time.Minute * -3).Unix()
}

func (o *operator) listTimeSeries(
	cli *monitoring.MetricClient,
	metrics []string,
	batch int,
	push PushFunc,
	groupBy []string,
) {
	var (
		wg                 sync.WaitGroup
		sem                = common.Semaphore(batch)
		startTime, endTime = o.getRangeTime()
	)

	o.projects.Range(func(k, v interface{}) bool {
		pid := k.(string)

		for _, metric := range metrics {
			sem.Acquire()
			wg.Add(1)

			go func(metric, pid string) {
				defer func() {
					sem.Release()
					wg.Done()
				}()
				req := &monitoringpb.ListTimeSeriesRequest{
					Name:   "projects/" + pid,
					Filter: fmt.Sprintf(`metric.type="%s"`, metric),
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
							// 当前写死
							Seconds: 60,
						},
						PerSeriesAligner:   monitoringpb.Aggregation_ALIGN_SUM,
						CrossSeriesReducer: monitoringpb.Aggregation_REDUCE_SUM,
						GroupByFields:      groupBy,
					}
				}

				ctx, _ := context.WithTimeout(context.TODO(), 30*time.Second)
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
					series: make(map[uint64]struct{}),
					points: points,
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

func (o *operator) pushTo(
	n9e *common.MetricValue,
	tm map[string]string,
) {
	n9e.BuildAndShift(tm)
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
