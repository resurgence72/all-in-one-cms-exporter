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
		h.namespace,
		h.push,
		nil,
		[]string{"instanceId"},
	)
}

func (h *HiTSDB) AsyncMeta(ctx context.Context) {
	var (
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

	h.op.async(h.op.getRegions, func(region string, wg *sync.WaitGroup) {
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
			h.mp.Store(tsdb.InstanceId, &tsdb)
		}
	})

	logrus.WithFields(logrus.Fields{
		"tsdbLens": h.op.mapLens(h.mp),
		"iden":     h.op.req.Iden,
	}).Warnln("async loop success, get all hitsdb instance")
}

func (h *HiTSDB) loadTSDB(id string) *hitsdb.LindormInstanceSummary {
	if tsdb, ok := h.mp.Load(id); ok {
		return tsdb.(*hitsdb.LindormInstanceSummary)
	}
	return nil
}

func (h *HiTSDB) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["instanceId"]
		if !ok {
			continue
		}

		tsdb := h.loadTSDB(p.(string))
		if tsdb == nil {
			continue
		}

		series := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("hitsdb", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     tsdb.InstanceId,
		}

		series.TagsMap = map[string]string{
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
				series.TagsMap[tag.Key] = tag.Value
			}
		}

		series.BuildAndShift()
	}
}
