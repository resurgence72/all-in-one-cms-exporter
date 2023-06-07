package google

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/duration"
	"google.golang.org/genproto/googleapis/api"
	"google.golang.org/genproto/googleapis/api/distribution"
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
		bks, err := generateHistogramBuckets(point.GetValue().GetDistributionValue())
		if err != nil {
			return point.GetValue().GetDistributionValue().Mean
		}
		if quantile := bucketQuantile([]float64{
			.50,
			.90,
			.99,
		}, bks); !quantile.isNan {
			quantile.qs = append(quantile.qs, point.GetValue().GetDistributionValue().Mean)
			return quantile
		}
		return point.GetValue().GetDistributionValue().Mean
	case metricpb.MetricDescriptor_STRING:
		return 0
	default:
		return 0
	}
}

type quantileContainer struct {
	qs    []float64
	isNan bool
}

func (o *operator) getMetrics(
	cli *monitoring.MetricClient,
	metricsPrefix ...string,
) ([]*metricpb.MetricDescriptor, error) {
	var p string
	// 遍历一次即可，拿到任意一个project
	o.projects.Range(func(key, value any) bool {
		p = key.(string)
		return false
	})

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	var metrics []*metricpb.MetricDescriptor
	for _, pre := range metricsPrefix {
		req := &monitoringpb.ListMetricDescriptorsRequest{
			Name:   fmt.Sprintf("projects/%s", p),
			Filter: fmt.Sprintf(`metric.type=starts_with("%s")`, pre),
		}

		descriptors := cli.ListMetricDescriptors(ctx, req)

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

					// 将counter类型转换为类似 promql rate(xxx)
					if *metric.MetricKind.Enum() == metricpb.MetricDescriptor_CUMULATIVE {
						req.Aggregation.PerSeriesAligner = monitoringpb.Aggregation_ALIGN_RATE
					}
					// 如果 valueType 类型为 bool,则不支持Aggregation_ALIGN_SUM
					if *metric.GetValueType().Enum() == metricpb.MetricDescriptor_BOOL {
						req.Aggregation.PerSeriesAligner = monitoringpb.Aggregation_ALIGN_NONE
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

type pointOperator struct {
	metricName     string
	ts             int64
	value          any
	metricLabels   map[string]string
	resourceLabels map[string]string
}

func (o *operator) newPointOperator(series *monitoringpb.TimeSeries) (*pointOperator, error) {
	points := series.GetPoints()
	if len(points) == 0 {
		return nil, errors.New("no point error")
	}

	point := points[len(points)-1]
	return &pointOperator{
		metricName:     o.buildMetric(series.Metric.Type),
		ts:             point.Interval.EndTime.GetSeconds(),
		value:          o.getPointValue(series.GetValueType(), point),
		metricLabels:   series.Metric.Labels,
		resourceLabels: series.Resource.Labels,
	}, nil
}

func generateHistogramBuckets(dist *distribution.Distribution) (buckets, error) {
	opts := dist.BucketOptions.Options

	var bucketKeys []float64
	switch o := opts.(type) {
	case *distribution.Distribution_BucketOptions_ExponentialBuckets:
		exponential := o.ExponentialBuckets
		// 指数桶
		// https://cloud.google.com/monitoring/api/ref_v3/rest/v3/TypedValue#exponential
		num := int(exponential.GetNumFiniteBuckets())
		bucketKeys = make([]float64, num+2)
		for i := 0; i <= num; i++ {
			bucketKeys[i] = exponential.GetScale() * math.Pow(exponential.GetGrowthFactor(), float64(i))
		}
	case *distribution.Distribution_BucketOptions_LinearBuckets:
		linear := o.LinearBuckets
		// 线性桶
		num := int(linear.GetNumFiniteBuckets())
		bucketKeys = make([]float64, num+2)
		for i := 0; i <= num; i++ {
			bucketKeys[i] = linear.GetOffset() + (float64(i) * linear.GetWidth())
		}
	case *distribution.Distribution_BucketOptions_ExplicitBuckets:
		explicit := o.ExplicitBuckets
		// 自定义桶
		bucketKeys = make([]float64, len(explicit.GetBounds())+1)
		copy(bucketKeys, explicit.GetBounds())
	default:
		return nil, errors.New("Unknown distribution bts")
	}

	// 最后一个桶为无穷大
	bucketKeys[len(bucketKeys)-1] = math.Inf(0)

	bs := make(buckets, 0, len(bucketKeys))
	var last float64
	for i, b := range bucketKeys {
		if len(dist.BucketCounts) > i {
			bs = append(bs, bucket{
				upperBound: b,
				count:      float64(dist.BucketCounts[i]) + last,
			})
			last = float64(dist.BucketCounts[i]) + last
		} else {
			bs = append(bs, bucket{
				upperBound: b,
				count:      last,
			})
		}
	}
	return bs, nil
}

type bucket struct {
	upperBound float64
	count      float64
}

type buckets []bucket

func (b buckets) Len() int           { return len(b) }
func (b buckets) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b buckets) Less(i, j int) bool { return b[i].upperBound < b[j].upperBound }

func bucketQuantile(qs []float64, buckets buckets) quantileContainer {
	quantile := quantileContainer{
		qs:    make([]float64, 0, len(qs)),
		isNan: true,
	}

	for _, q := range qs {
		if math.IsNaN(q) {
			return quantile
		}
		if q < 0 {
			return quantile
		}
		if q > 1 {
			return quantile
		}
	}

	sort.Sort(buckets)
	if !math.IsInf(buckets[len(buckets)-1].upperBound, +1) {
		return quantile
	}

	buckets = coalesceBuckets(buckets)
	ensureMonotonic(buckets)

	if len(buckets) < 2 {
		return quantile
	}
	observations := buckets[len(buckets)-1].count
	if observations == 0 {
		return quantile
	}

	for _, q := range qs {
		rank := q * observations
		b := sort.Search(len(buckets)-1, func(i int) bool { return buckets[i].count >= rank })

		if b == len(buckets)-1 {
			quantile.qs = append(quantile.qs, buckets[len(buckets)-2].upperBound)
			continue
		}
		if b == 0 && buckets[0].upperBound <= 0 {
			quantile.qs = append(quantile.qs, buckets[0].upperBound)
			continue
			//return buckets[0].upperBound
		}
		var (
			bucketStart float64
			bucketEnd   = buckets[b].upperBound
			count       = buckets[b].count
		)
		if b > 0 {
			bucketStart = buckets[b-1].upperBound
			count -= buckets[b-1].count
			rank -= buckets[b-1].count
		}
		quantile.qs = append(quantile.qs, bucketStart+(bucketEnd-bucketStart)*(rank/count))
		//return bucketStart + (bucketEnd-bucketStart)*(rank/count)
	}
	quantile.isNan = false
	return quantile
}

func coalesceBuckets(buckets buckets) buckets {
	last := buckets[0]
	i := 0
	for _, b := range buckets[1:] {
		if b.upperBound == last.upperBound {
			last.count += b.count
		} else {
			buckets[i] = last
			last = b
			i++
		}
	}
	buckets[i] = last
	return buckets[:i+1]
}

func ensureMonotonic(buckets buckets) {
	max := buckets[0].count
	for i := 1; i < len(buckets); i++ {
		switch {
		case buckets[i].count > max:
			max = buckets[i].count
		case buckets[i].count < max:
			buckets[i].count = max
		}
	}
}
