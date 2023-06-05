package ali

import (
	"context"
	"strconv"
	"sync"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/polardb"
	"github.com/sirupsen/logrus"
)

type PolarDB struct {
	meta
}

func init() {
	registers[ACS_POLARDB] = new(PolarDB)
}

func (p *PolarDB) Inject(params ...any) common.MetricsGetter {
	return &PolarDB{meta: newMeta(params...)}
}

func (p *PolarDB) GetMetrics() error {
	metrics, err := p.op.getMetrics(
		p.client,
		p.namespace,
		newMetricsBuilder(),
	)
	if err != nil {
		return err
	}
	p.metrics = metrics
	return nil
}

func (p *PolarDB) GetNamespace() string {
	return p.namespace
}

func (p *PolarDB) Collector() {
	p.op.getMetricLastData(
		p.client,
		p.metrics,
		p.namespace,
		p.push,
		nil,
		[]string{"clusterId"}, // 根据集群id做聚合
	)
}

func (p *PolarDB) AsyncMeta(ctx context.Context) {
	var (
		maxPageSize = 100
		parse       = func(region string, pageNum int, container []polardb.DBCluster) ([]polardb.DBCluster, int, error) {
			bytes, err := p.op.commonRequest(
				region,
				"polardb",
				"2017-08-01",
				"DescribeDBClusters",
				pageNum,
				maxPageSize,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(polardb.DescribeDBClustersResponse)
			if err := parser.Parser().Unmarshal(bytes, resp); err != nil {
				return nil, 0, err
			}

			return append(container, resp.Items.DBCluster...), len(resp.Items.DBCluster), nil
		}
	)

	p.op.async(p.op.getRegions, func(region string, wg *sync.WaitGroup) {
		defer wg.Done()
		var (
			pageNum   = 1
			container []polardb.DBCluster
		)

		container, currLen, err := parse(region, pageNum, container)
		if err != nil {
			// logrus.Errorln("AsyncMeta err ", err, region)
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
			pdb := container[i]

			p.mp.Store(pdb.DBClusterId, &pdb)
		}
	})

	logrus.WithFields(logrus.Fields{
		"polarDBLens": p.op.mapLens(p.mp),
		"iden":        p.op.req.Iden,
	}).Warnln("async loop success, get all polardb instance")
}

func (p *PolarDB) push(transfer *transferData) {
	for _, point := range transfer.points {
		po, ok := point["clusterId"]
		if !ok {
			continue
		}

		instanceID := po.(string)
		pdb := p.loadPolarDB(instanceID)
		if pdb == nil {
			continue
		}

		series := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("polardb", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     pdb.DBClusterId,
		}

		series.TagsMap = map[string]string{
			"region":         pdb.RegionId,
			"engine":         pdb.Engine,
			"engine_version": pdb.DBVersion,
			"db_class":       pdb.DBNodeClass,
			"db_node_num":    strconv.Itoa(pdb.DBNodeNumber),
			"db_type":        pdb.DBType,
			"cluster_id":     instanceID,
			"cluster_name":   pdb.DBClusterDescription,
			"cluster_status": pdb.DBClusterStatus,

			"provider":  ProviderName,
			"iden":      p.op.req.Iden,
			"namespace": p.namespace,
			// 指标单位
			"unit_name": transfer.unit,
		}
		series.BuildAndShift()
	}

}

func (p *PolarDB) loadPolarDB(id string) *polardb.DBCluster {
	if pdb, ok := p.mp.Load(id); ok {
		return pdb.(*polardb.DBCluster)
	}
	return nil
}
