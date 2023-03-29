package remote

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"watcher4metrics/pkg/metric"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	wconfig "github.com/prometheus/common/config"

	"github.com/prometheus/prometheus/prompb"

	"watcher4metrics/config"
	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/relabel"

	"github.com/sirupsen/logrus"

	wremote "github.com/prometheus/prometheus/storage/remote"
)

type remoteMgr struct {
	batchSize       int
	batchContainers [][]*common.MetricValue
	autoCommit      <-chan time.Time
	rs              []remote

	shard int
}

type remote struct {
	w        wremote.WriteClient
	relabels []*relabel.Config
}

var reloadCh = make(chan chan error, 1)

func newRemoteMgr() (*remoteMgr, error) {
	conf := config.Get().Report
	report := &remoteMgr{
		batchSize:  conf.Batch,
		autoCommit: time.After(5 * time.Second),
		shard:      common.RemoteShard,
	}

	bcs := make([][]*common.MetricValue, 0, report.shard)
	for shard := 0; shard < report.shard; shard++ {
		bcs = append(bcs, make([]*common.MetricValue, 0, conf.Batch))
	}
	report.batchContainers = bcs

	var rcs []remote
	for i, rw := range conf.RemoteWrites {
		uri, _ := url.Parse(rw.URL)

		hs := make(map[string]string)
		// 设置自定义请求头
		for k, v := range rw.Authorization {
			if len(v) > 0 {
				hs[k] = v
			}
		}

		client, err := wremote.NewWriteClient(fmt.Sprintf("remote_write_%d", i), &wremote.ClientConfig{
			URL: &wconfig.URL{
				URL: uri,
			},
			Timeout:          rw.RemoteTimeout,
			Headers:          hs,
			RetryOnRateLimit: true,
		})
		if err != nil {
			logrus.Errorln("remote mgr init failed ", err)
			continue
		}

		rcs = append(rcs, remote{
			w:        client,
			relabels: rw.WriteRelabelConfigs,
		})
	}

	report.rs = rcs
	return report, nil
}

func NewRemoteWritesClient(ctx context.Context) {
	report, err := newRemoteMgr()
	if err != nil {
		return
	}

	var wg sync.WaitGroup

	for shard := 0; shard < report.shard; shard++ {
		wg.Add(1)

		go func(shard int) {
			defer wg.Done()
			logrus.Warnf("remote write client shard %d is start", shard)

			for {
				select {
				case <-ctx.Done():
					// 确保将当前队列中数据发送完毕
					if report.batchContainers[shard] != nil && len(report.batchContainers[shard]) > 0 {
						report.report(shard)
					}
					return
				case nPoint := <-common.SeriesCh(shard):
					// 攒够 batch的数量再发送
					report.batchContainers[shard] = append(report.batchContainers[shard], nPoint)
					if len(report.batchContainers[shard]) == report.batchSize {
						report.report(shard)
					}
				case <-report.autoCommit:
					// 每5s会检测当前batchContainer中是否存在 < conf.batch的未发送数据进行发送
					if report.batchContainers[shard] != nil && len(report.batchContainers[shard]) > 0 {
						report.report(shard)
					}
				case errCh := <-reloadCh:
					// reload
					report, err = newRemoteMgr()
					errCh <- err
				}
			}
		}(shard)
	}

	wg.Wait()
}

func (r *remoteMgr) buildSeries(tmp []*common.MetricValue, rlbs []*relabel.Config) []prompb.TimeSeries {
	series := make([]prompb.TimeSeries, 0, r.batchSize)
	for _, mv := range tmp {
		metric.CMSMetricsTotalCounter.WithLabelValues(
			mv.TagsMap["provider"],
			mv.TagsMap["namespace"],
		).Inc()

		ts := mv.Convert(rlbs)
		if len(ts.Labels) == 0 {
			continue
		}
		series = append(series, ts)
	}
	return series
}

func (r *remoteMgr) send(rc remote, tmp []*common.MetricValue) {
	series := r.buildSeries(tmp, rc.relabels)
	if len(series) == 0 {
		return
	}

	req := &prompb.WriteRequest{
		Timeseries: series,
	}
	data, err := proto.Marshal(req)
	if err != nil {
		logrus.Errorln("proto marshal failed ", err)
		return
	}

	if err := rc.w.Store(context.TODO(), snappy.Encode(nil, data)); err != nil {
		logrus.Errorln("remote write failed ", err)
		return
	}

	logrus.Warnln("remote write success, send batch size ", len(series))
}

func (r *remoteMgr) report(shard int) {
	// 做深拷贝，防止影响到当前的batchContainer
	dc := make([]*common.MetricValue, len(r.batchContainers[shard]))
	copy(dc, r.batchContainers[shard])
	// 当前批次发送后重置batchContainer
	r.batchContainers[shard] = r.batchContainers[shard][:0]

	for _, rc := range r.rs {
		go r.send(rc, dc)
	}
}

func Reload() error {
	errCh := make(chan error, 1)
	reloadCh <- errCh
	return <-errCh
}
