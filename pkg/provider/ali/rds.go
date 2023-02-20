package ali

import (
	"context"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/rds"
	"github.com/sirupsen/logrus"
	"sync"
	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"
)

type Rds struct {
	op        *operator
	namespace string
	metrics   []*cms.Resource
	client    *cms.Client
	// 保存eip的实例id对应的eip对象
	rdsMap map[string]*rds.DBInstance

	m sync.RWMutex
}

func init() {
	registers[ACS_RDS_DASHBOARD] = new(Rds)
}

func (r *Rds) Inject(params ...interface{}) common.MetricsGetter {
	return &Rds{
		op:        params[0].(*operator),
		client:    params[1].(*cms.Client),
		namespace: params[2].(string),
	}
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
		5,
		r.namespace,
		r.push,
		nil,
		[]string{"instanceId"},
	)
}

func (r *Rds) AsyncMeta(ctx context.Context) {
	var (
		wg          sync.WaitGroup
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

	if r.rdsMap == nil {
		r.rdsMap = make(map[string]*rds.DBInstance)
	}

	for _, region := range r.op.getRegions() {
		wg.Add(1)
		go func(region string) {
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

				r.m.Lock()
				r.rdsMap[rds.DBInstanceId] = &rds
				r.m.Unlock()
			}
		}(region)
	}

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"rdsLens": len(r.rdsMap),
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
		rds := r.getRds(instanceID)
		if rds == nil {
			continue
		}

		n9e := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("rds", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     instanceID,
		}

		n9e.Endpoint = rds.DBInstanceId
		tagsMap := map[string]string{
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

		n9e.BuildAndShift(tagsMap)
	}
}

func (r *Rds) getRds(id string) *rds.DBInstance {
	r.m.RLock()
	defer r.m.RUnlock()

	if rds, ok := r.rdsMap[id]; ok {
		return rds
	}
	return nil
}
