package ali

import (
	"context"
	"sync"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/hitsdb"
	"github.com/sirupsen/logrus"
)

type HiTSDB struct {
	meta

	// 保存eip的实例id对应的eip对象
	tsdbMap map[string]*hitsdb.LindormInstanceSummary
}

func init() {
	registers[ACS_HITSDB] = new(HiTSDB)
}

func (h *HiTSDB) Inject(params ...any) common.MetricsGetter {
	return &HiTSDB{meta: newMeta(params...)}
}

func (h *HiTSDB) GetMetrics() error {
	metrics, err := h.op.getMetrics(
		h.client,
		h.namespace,
		nil,
		nil,
	)
	if err != nil {
		return err
	}
	h.metrics = metrics
	return nil
}

func (h *HiTSDB) GetNamespace() string {
	return h.namespace
}

func (h *HiTSDB) Collector() {
	h.op.getMetricLastData(
		h.client,
		h.metrics,
		5,
		h.namespace,
		h.push,
		nil,
		[]string{"instanceId"},
	)
}

func (h *HiTSDB) AsyncMeta(ctx context.Context) {
	var (
		wg          sync.WaitGroup
		maxPageSize = 100
		parse       = func(region string, pageNum int, container []hitsdb.LindormInstanceSummary) ([]hitsdb.LindormInstanceSummary, int, error) {
			bytes, err := h.op.commonRequest(
				region,
				"hitsdb",
				"2020-06-15",
				"GetLindormInstanceList",
				pageNum,
				maxPageSize,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(hitsdb.GetLindormInstanceListResponse)
			if err := parser.Parser().Unmarshal(bytes, resp); err != nil {
				return nil, 0, err
			}

			return append(container, resp.InstanceList...), len(resp.InstanceList), nil
		}
	)

	if h.tsdbMap == nil {
		h.tsdbMap = make(map[string]*hitsdb.LindormInstanceSummary)
	}

	for _, region := range h.op.getRegions() {
		wg.Add(1)
		go func(region string) {
			defer wg.Done()
			var (
				pageNum   = 1
				container []hitsdb.LindormInstanceSummary
			)

			container, currLen, err := parse(region, pageNum, container)
			if err != nil {
				logrus.Errorln("AsyncMeta err ", err, region)
				return
			}

			for currLen == maxPageSize {
				pageNum++
				container, currLen, err = parse(region, pageNum, container)
				if err != nil {
					logrus.Errorln("AsyncMeta paging err ", err)
					continue
				}
			}

			for i := range container {
				tsdb := container[i]

				h.m.Lock()
				h.tsdbMap[tsdb.InstanceId] = &tsdb
				h.m.Unlock()
			}
		}(region)
	}

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"tsdbLens": len(h.tsdbMap),
		"iden":     h.op.req.Iden,
	}).Warnln("async loop success, get all hitsdb instance")
}

func (h *HiTSDB) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["instanceId"]
		if !ok {
			continue
		}

		instanceID := p.(string)
		tsdb, ok := h.tsdbMap[instanceID]
		if !ok {
			continue
		}

		series := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("hitsdb", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     instanceID,
		}

		tagsMap := map[string]string{
			"engine_type":      tsdb.EngineType,
			"region":           tsdb.RegionId,
			"instance_storage": tsdb.InstanceStorage,
			"instance_id":      tsdb.InstanceId,
			"service_type":     tsdb.ServiceType,
			"instance_name":    tsdb.InstanceAlias,
			"instance_status":  tsdb.InstanceStatus,

			"provider":  ProviderName,
			"iden":      h.op.req.Iden,
			"namespace": h.namespace,
			// 指标单位
			"unit_name": transfer.unit,
		}

		for _, tag := range tsdb.Tags {
			if tag.Value != "" {
				tagsMap[tag.Key] = tag.Value
			}
		}

		series.BuildAndShift(tagsMap)
	}
}
