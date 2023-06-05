package ali

import (
	"context"
	"strconv"
	"sync"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/r-kvstore"
	"github.com/sirupsen/logrus"
)

type Redis struct {
	meta
}

func init() {
	registers[ACS_KVSTORE] = new(Redis)
}

func (r *Redis) Inject(params ...any) common.MetricsGetter {
	return &Redis{meta: newMeta(params...)}
}

func (r *Redis) GetMetrics() error {
	metrics, err := r.op.getMetrics(
		r.client,
		r.namespace,
		newMetricsBuilder(),
	)
	if err != nil {
		return err
	}
	r.metrics = metrics
	return nil
}

func (r *Redis) GetNamespace() string {
	return r.namespace
}

func (r *Redis) Collector() {
	r.op.getMetricLastData(
		r.client,
		r.metrics,
		r.namespace,
		r.push,
		nil,
		[]string{"instanceId"},
	)
}

func (r *Redis) AsyncMeta(ctx context.Context) {
	var (
		maxPageSize = 100
		parse       = func(region string, pageNum int, container []r_kvstore.KVStoreInstanceInDescribeInstances) ([]r_kvstore.KVStoreInstanceInDescribeInstances, int, error) {
			bytes, err := r.op.commonRequest(
				region,
				"r-kvstore",
				"2015-01-01",
				"DescribeInstances",
				pageNum,
				maxPageSize,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(r_kvstore.DescribeInstancesResponse)
			if err := parser.Parser().Unmarshal(bytes, resp); err != nil {
				return nil, 0, err
			}

			return append(container, resp.Instances.KVStoreInstance...), len(resp.Instances.KVStoreInstance), nil
		}
	)

	r.op.async(r.op.getRegions, func(region string, wg *sync.WaitGroup) {
		defer wg.Done()
		var (
			pageNum   = 1
			container []r_kvstore.KVStoreInstanceInDescribeInstances
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
			redis := container[i]

			r.mp.Store(redis.InstanceId, &redis)
		}
	})

	logrus.WithFields(logrus.Fields{
		"redisLens": r.op.mapLens(r.mp),
		"iden":      r.op.req.Iden,
	}).Warnln("async loop success, get all redis instance")
}

func (r *Redis) loadRedis(id string) *r_kvstore.KVStoreInstanceInDescribeInstances {
	if redis, ok := r.mp.Load(id); ok {
		return redis.(*r_kvstore.KVStoreInstanceInDescribeInstances)
	}
	return nil
}

func (r *Redis) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["instanceId"]
		if !ok {
			continue
		}

		instanceID := p.(string)
		redis := r.loadRedis(instanceID)
		if redis == nil {
			continue
		}

		series := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("redis", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     redis.InstanceId,
		}

		series.TagsMap = map[string]string{
			"region":            redis.RegionId,
			"engine_version":    redis.EngineVersion,
			"instance_capacity": strconv.FormatInt(redis.Capacity, 10),
			"architecture_type": redis.ArchitectureType,
			"instance_id":       instanceID,
			"instance_name":     redis.InstanceName,
			"instance_type":     redis.InstanceType,
			"instance_status":   redis.InstanceStatus,
			"bandwidth":         strconv.FormatInt(redis.Bandwidth, 10),
			"port":              strconv.FormatInt(redis.Port, 10),
			"qps":               strconv.FormatInt(redis.QPS, 10),
			"connections":       strconv.FormatInt(redis.Connections, 10),

			"provider":  ProviderName,
			"iden":      r.op.req.Iden,
			"namespace": r.namespace,
			// 指标单位
			"unit_name": transfer.unit,
		}

		series.BuildAndShift()
	}
}
