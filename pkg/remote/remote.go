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
	batchSize      int
	batchContainer []*common.MetricValue
	autoCommit     *time.Ticker
	rs             []remote

	m sync.Mutex
}

type remote struct {
	w        wremote.WriteClient
	relabels []*relabel.Config
}

var reloadCh = make(chan chan error, 1)

func newRemoteMgr() (*remoteMgr, error) {
	conf := config.Get().Report
	report := &remoteMgr{
		batchSize:      conf.Batch,
		batchContainer: make([]*common.MetricValue, 0, conf.Batch),
		autoCommit:     time.NewTicker(time.Duration(5) * time.Second),
	}

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
	defer report.autoCommit.Stop()

	for {
		select {
		case <-ctx.Done():
			// 确保将当前队列中数据发送完毕
			if report.batchContainer != nil && len(report.batchContainer) > 0 {
				report.report()
			}
			return
		case nPoint := <-common.SeriesCh():
			// 攒够 batch的数量再发送
			report.batchContainer = append(report.batchContainer, nPoint)
			if len(report.batchContainer) == report.batchSize {
				report.report()
			}
		case <-report.autoCommit.C:
			// 每10s会检测当前batchContainer中是否存在 < conf.batch的未发送数据进行发送
			if report.batchContainer != nil && len(report.batchContainer) > 0 {
				report.report()
			}
		case errCh := <-reloadCh:
			// reload
			report.autoCommit.Stop()
			report, err = newRemoteMgr()
			errCh <- err
		}
	}
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

func (r *remoteMgr) report() {
	// 做深拷贝，防止影响到当前的batchContainer
	tmp := make([]*common.MetricValue, len(r.batchContainer))
	copy(tmp, r.batchContainer)
	// 当前批次发送后重置batchContainer
	r.batchContainer = r.batchContainer[:0]

	var wg sync.WaitGroup
	for _, rc := range r.rs {
		wg.Add(1)

		go func(rc remote) {
			defer wg.Done()

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
		}(rc)
	}
	wg.Wait()
	logrus.Warnln("current remote write job done")
}

func Reload() error {
	errCh := make(chan error, 1)
	reloadCh <- errCh
	return <-errCh
}
