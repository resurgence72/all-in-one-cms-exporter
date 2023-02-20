package ali

import (
	"context"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/polardb"
	"github.com/sirupsen/logrus"
	"strconv"
	"sync"
	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"
)

type PolarDB struct {
	op        *operator
	namespace string
	metrics   []*cms.Resource
	client    *cms.Client
	pdbMap    map[string]*polardb.DBCluster

	m sync.RWMutex
}

func init() {
	registers[ACS_POLARDB] = new(PolarDB)
}

func (p *PolarDB) Inject(params ...interface{}) common.MetricsGetter {
	return &PolarDB{
		op:        params[0].(*operator),
		client:    params[1].(*cms.Client),
		namespace: params[2].(string),
	}
}

func (p *PolarDB) GetMetrics() error {
	metrics, err := p.op.getMetrics(
		p.client,
		p.namespace,
		nil,
		nil,
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
		5,
		p.namespace,
		p.push,
		nil,
		[]string{"clusterId"}, // 根据集群id做聚合
	)
}

func (p *PolarDB) AsyncMeta(ctx context.Context) {
	var (
		wg          sync.WaitGroup
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

	if p.pdbMap == nil {
		p.pdbMap = make(map[string]*polardb.DBCluster)
	}

	for _, region := range p.op.getRegions() {
		wg.Add(1)
		go func(region string) {
			defer wg.Done()
			var (
				pageNum   = 1
				container []polardb.DBCluster
			)

			container, currLen, err := parse(region, pageNum, container)
			if err != nil {
				//logrus.Errorln("AsyncMeta err ", err, region)
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

				p.m.Lock()
				p.pdbMap[pdb.DBClusterId] = &pdb
				p.m.Unlock()
			}
		}(region)
	}

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"polarDBLens": len(p.pdbMap),
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
		pdb := p.getPolarDB(instanceID)
		if pdb == nil {
			continue
		}

		n9e := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("polardb", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     instanceID,
		}

		n9e.Endpoint = pdb.DBClusterId
		tagsMap := map[string]string{
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
		n9e.BuildAndShift(tagsMap)
	}

}

func (p *PolarDB) getPolarDB(id string) *polardb.DBCluster {
	p.m.RLock()
	defer p.m.RUnlock()

	if pdb, ok := p.pdbMap[id]; ok {
		return pdb
	}
	return nil
}
