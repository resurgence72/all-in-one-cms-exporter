package ali

import (
	"context"
	"strconv"
	"sync"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/dds"
	"github.com/sirupsen/logrus"
)

type MongoDB struct {
	meta
}

func (m *MongoDB) Inject(params ...any) common.MetricsGetter {
	return &MongoDB{meta: newMeta(params...)}
}

func (m *MongoDB) GetMetrics() error {
	metrics, err := m.op.getMetrics(
		m.client,
		m.namespace,
		nil,
		nil,
	)
	if err != nil {
		return err
	}
	m.metrics = metrics
	return nil
}

func (m *MongoDB) GetNamespace() string {
	return m.namespace
}

func (m *MongoDB) Collector() {
	m.op.getMetricLastData(
		m.client,
		m.metrics,
		m.namespace,
		m.push,
		nil,
		[]string{"instanceId"},
	)
}

func (m *MongoDB) AsyncMeta(ctx context.Context) {
	m.op.async(m.op.getRegions, m.asyncByRegion)
	logrus.WithFields(logrus.Fields{
		"mongoLens": m.op.mapLens(m.mp),
		"iden":      m.op.req.Iden,
	}).Warnln("async loop success, get all redis instance")
}

func (m *MongoDB) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["instanceId"]
		if !ok {
			continue
		}

		mongo := m.loadMongo(p.(string))
		if mongo == nil {
			continue
		}

		n9e := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("mongodb", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     mongo.DBInstanceId,
		}

		n9e.TagsMap = map[string]string{
			"region":           mongo.RegionId,
			"network_type":     mongo.NetworkType,
			"engine_version":   mongo.EngineVersion,
			"instance_name":    mongo.DBInstanceDescription,
			"instance_type":    mongo.DBInstanceType,
			"instance_status":  mongo.DBInstanceStatus,
			"instance_storage": strconv.Itoa(mongo.DBInstanceStorage),
			"engine":           mongo.Engine,
			"replica":          mongo.ReplicationFactor,
			"kind_code":        mongo.KindCode,
			"capacity_unit":    mongo.CapacityUnit,
			"storage_type":     mongo.StorageType,

			"provider":  ProviderName,
			"iden":      m.op.req.Iden,
			"namespace": m.namespace,
			"unit_name": transfer.unit,
		}

		for _, tag := range mongo.Tags.Tag {
			if tag.Value != "" {
				n9e.TagsMap[tag.Key] = tag.Value
			}
		}

		n9e.BuildAndShift()
	}
}

func (m *MongoDB) loadMongo(id string) *dds.DBInstance {
	if mongo, ok := m.mp.Load(id); ok {
		return mongo.(*dds.DBInstance)
	}
	return nil
}

func (m *MongoDB) asyncByRegion(region string, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		pageNum   = 1
		container []dds.DBInstance

		maxPageSize = 100
		parse       = func(region string, pageNum int, container []dds.DBInstance) ([]dds.DBInstance, int, error) {
			bytes, err := m.op.commonRequest(
				region,
				"dds",
				"2015-12-01",
				"DescribeDBInstances",
				pageNum,
				maxPageSize,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(dds.DescribeDBInstancesResponse)
			if err := parser.Parser().Unmarshal(bytes, resp); err != nil {
				return nil, 0, err
			}

			return append(container, resp.DBInstances.DBInstance...), len(resp.DBInstances.DBInstance), nil
		}
	)

	container, currLen, err := parse(region, pageNum, container)
	if err != nil {
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
		mongo := container[i]

		m.mp.Store(mongo.DBInstanceId, &mongo)
	}
}

func init() {
	registers[ACS_MONGODB] = new(MongoDB)
}
