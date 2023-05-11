package ali

import (
	"context"
	"sync"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/rds"
	"github.com/sirupsen/logrus"
)

type Rds struct {
	meta
}

func init() {
	registers[ACS_RDS_DASHBOARD] = new(Rds)
}

func (r *Rds) Inject(params ...any) common.MetricsGetter {
	return &Rds{meta: newMeta(params...)}
}

func (r *Rds) GetMetrics() error {
	metrics, err := r.op.getMetrics(
		r.client,
		r.namespace,
		map[string]string{"productCategory": "rds"},
		nil,
	)
	if err != nil {
		return err
	}
	r.metrics = metrics
	return nil
}

func (r *Rds) GetNamespace() string {
	return r.namespace
}

func (r *Rds) Collector() {
	r.op.getMetricLastData(
		r.client,
		r.metrics,
		r.namespace,
		r.push,
		nil,
		[]string{"instanceId"},
	)
}

func (r *Rds) AsyncMeta(ctx context.Context) {
	var (
		maxPageSize = 100
		parse       = func(region string, pageNum int, container []rds.DBInstance) ([]rds.DBInstance, int, error) {
			bytes, err := r.op.commonRequest(
				region,
				"rds",
				"2014-08-15",
				"DescribeDBInstances",
				pageNum,
				maxPageSize,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(rds.DescribeDBInstancesResponse)
			if err := parser.Parser().Unmarshal(bytes, resp); err != nil {
				return nil, 0, err
			}

			return append(container, resp.Items.DBInstance...), len(resp.Items.DBInstance), nil
		}
	)

	r.op.async(r.op.getRegions, func(region string, wg *sync.WaitGroup) {
		defer wg.Done()
		var (
			pageNum   = 1
			container []rds.DBInstance
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
			rds := container[i]

			r.mp.Store(rds.DBInstanceId, &rds)
		}
	})

	logrus.WithFields(logrus.Fields{
		"rdsLens": r.op.mapLens(r.mp),
		"iden":    r.op.req.Iden,
	}).Warnln("async loop success, get all rds instance")
}

func (r *Rds) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["instanceId"]
		if !ok {
			continue
		}

		instanceID := p.(string)
		rds := r.loadRds(instanceID)
		if rds == nil {
			continue
		}

		series := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("rds", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     rds.DBInstanceId,
		}

		series.TagsMap = map[string]string{
			"region":          rds.RegionId,
			"engine":          rds.Engine,
			"engine_version":  rds.EngineVersion,
			"instance_class":  rds.DBInstanceClass,
			"instance_type":   rds.DBInstanceType,
			"instance_id":     instanceID,
			"instance_name":   rds.DBInstanceDescription,
			"instance_status": rds.DBInstanceStatus,

			"provider":  ProviderName,
			"iden":      r.op.req.Iden,
			"namespace": r.namespace,
			// 指标单位
			"unit_name": transfer.unit,
		}

		series.BuildAndShift()
	}
}

func (r *Rds) loadRds(id string) *rds.DBInstance {
	if rs, ok := r.mp.Load(id); ok {
		return rs.(*rds.DBInstance)
	}
	return nil
}
