package tc

import (
	"context"
	"sync"

	"watcher4metrics/pkg/common"

	"github.com/goccy/go-json"
	"github.com/sirupsen/logrus"
	monitor "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/monitor/v20180724"
	waf "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/waf/v20180125"
)

type Waf struct {
	meta

	wafMap map[string]map[string]*waf.DomainInfo
}

func init() {
	registers[QCE_WAF] = new(Waf)
}

func (w *Waf) Inject(params ...any) common.MetricsGetter {
	return &Waf{meta: newMeta(params...)}
}

func (w *Waf) GetMetrics() error {
	metrics, err := w.op.getMetrics(
		w.clients["ap-shanghai"],
		w.namespace,
		nil,
	)
	if err != nil {
		return err
	}
	w.metrics = metrics
	return nil
}

func (w *Waf) GetNamespace() string {
	return w.namespace
}

func (w *Waf) Collector() {
	w.op.getMonitorData(
		w.clients,
		w.metrics,
		// 有些产品只能指定固定region 例如waf 必须指定广州region
		[]string{"ap-guangzhou"},
		func() InstanceBuilderFunc {
			return func(region string) []*monitor.Instance {
				return w.op.buildInstances(
					[]string{"domain", "edition"},
					func() [][]string {
						var (
							out               [][]string
							domains, editions []string
						)
						for _, waf := range w.wafMap[region] {
							domains = append(domains, *waf.Domain)
							editions = append(editions, "1")
						}
						return append(out, domains, editions)
					}(),
				)
			}
		}(),
		w.namespace,
		w.push,
	)
}

func (w *Waf) AsyncMeta(context.Context) {
	var (
		maxPageSize = 100
		parse       = func(region string, offset, limit int, container []*waf.DomainInfo) ([]*waf.DomainInfo, int, error) {
			bs, err := w.op.commonRequest(
				region,
				"waf",
				"2018-01-25",
				"DescribeDomains",
				offset,
				limit,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(waf.DescribeDomainsResponse)
			if err = json.Unmarshal(bs, resp); err != nil {
				return nil, 0, err
			}
			return append(container, resp.Response.Domains...), len(resp.Response.Domains), nil
		}
	)

	if w.wafMap == nil {
		w.wafMap = make(map[string]map[string]*waf.DomainInfo)
	}

	w.op.async(w.op.getRegions(), func(region string, wg *sync.WaitGroup, sem *common.Semaphore) {
		defer func() {
			wg.Done()
			sem.Release()
		}()

		var (
			offset    = 1
			pageNum   = 1
			container []*waf.DomainInfo
		)

		container, currLen, err := parse(region, offset, maxPageSize, container)
		if err != nil {
			return
		}

		// 分页
		for currLen == maxPageSize {
			offset = pageNum * maxPageSize
			container, currLen, err = parse(region, offset, maxPageSize, container)
			if err != nil {
				logrus.Errorln("tc loop paging failed", err)
				continue
			}
			pageNum++
		}

		w.m.Lock()
		if _, ok := w.wafMap[region]; !ok {
			w.wafMap[region] = make(map[string]*waf.DomainInfo)
		}
		w.m.Unlock()

		for i := range container {
			waf := container[i]

			w.m.Lock()
			w.wafMap[region][*waf.Domain] = waf
			w.m.Unlock()
		}
	})

	logrus.WithFields(logrus.Fields{
		"wafLens": len(w.wafMap),
		"iden":    w.op.req.Iden,
	}).Warnln("async loop get all tc waf success")
}

func (w *Waf) push(transfer *transferData) {
	for _, point := range transfer.points {
		domain := point.Dimensions[0].Value

		waf := w.getWaf(transfer.region, domain)
		if waf == nil {
			return
		}

		for i, ts := range point.Timestamps {
			series := &common.MetricValue{
				Timestamp:    int64(*ts),
				Metric:       common.BuildMetric("clb_waf", transfer.metric),
				ValueUntyped: *point.Values[i],
				Endpoint:     *waf.Domain,
			}

			tagsMap := map[string]string{
				"iden":        w.op.req.Iden,
				"provider":    ProviderName,
				"region":      transfer.region,
				"namespace":   w.namespace,
				"unit_name":   transfer.unit,
				"instance_id": *waf.DomainId,
			}
			series.BuildAndShift(tagsMap)
			continue
		}
	}
}

func (w *Waf) getWaf(region string, waf *string) *waf.DomainInfo {
	w.m.RLock()
	defer w.m.RUnlock()
	if wafM, ok := w.wafMap[region]; ok {
		if waf, ok := wafM[*waf]; ok {
			return waf
		}
	}
	return nil
}
