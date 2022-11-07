package google

import (
	"context"
	"crypto/md5"
	"fmt"
	"strings"
	"sync"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	monitoringpb "google.golang.org/genproto/googleapis/monitoring/v3"
	"watcher4metrics/pkg/common"
)

type operator struct {
	req *GoogleReq
}

var defalutProject = "bk-common"

type PushFunc func(*transfer)

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
) {
	var (
		wg                 sync.WaitGroup
		sem                = common.Semaphore(batch)
		startTime, endTime = o.getRangeTime()
	)

	for _, metric := range metrics {
		sem.Acquire()
		wg.Add(1)

		go func(metric string) {
			defer func() {
				sem.Release()
				wg.Done()
			}()
			req := &monitoringpb.ListTimeSeriesRequest{
				Name:   "projects/" + defalutProject,
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

			transfer := &transfer{
				series: make(map[uint64]struct{}),
				points: points,
			}
			go push(transfer)
		}(metric)
	}

	go func() {
		wg.Wait()
		cli.Close()
	}()
}

func (o *operator) pushTo(
	n9e *common.MetricValue,
	tm map[string]string,
	filter map[uint64]struct{},
) {
	// 构造临时map m 去所hash索引
	m := tm
	m["endpoint"] = n9e.Endpoint

	sum := o.getSeriesSum64(m)
	if _, ok := filter[sum]; !ok {
		filter[sum] = struct{}{}
		n9e.BuildAndShift(tm)
	}
}

func (o *operator) buildMetric(source string) string {
	ss := strings.Split(source, "/")
	return strings.Join(ss[len(ss)-2:], "_")
}

func (o *operator) getRegions() []string {
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
