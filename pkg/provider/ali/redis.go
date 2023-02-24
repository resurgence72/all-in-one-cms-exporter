package ali

import (
	"context"
	"strconv"
	"sync"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/r-kvstore"
	"github.com/sirupsen/logrus"
)

type Redis struct {
	op        *operator
	namespace string
	metrics   []*cms.Resource
	client    *cms.Client
	redisMap  map[string]*r_kvstore.KVStoreInstanceInDescribeInstances

	m sync.RWMutex
}

func init() {
	registers[ACS_KVSTORE] = new(Redis)
}

func (r *Redis) Inject(params ...interface{}) common.MetricsGetter {
	return &Redis{
		op:        params[0].(*operator),
		client:    params[1].(*cms.Client),
		namespace: params[2].(string),
	}
}

func (r *Redis) GetMetrics() error {
	metrics, err := r.op.getMetrics(
		r.client,
		r.namespace,
		nil,
		nil,
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
		5,
		r.namespace,
		r.push,
		nil,
		[]string{"instanceId"},
	)
}

func (r *Redis) AsyncMeta(ctx context.Context) {
	var (
		wg          sync.WaitGroup
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

	if r.redisMap == nil {
		r.redisMap = make(map[string]*r_kvstore.KVStoreInstanceInDescribeInstances)
	}

	for _, region := range r.op.getRegions() {
		wg.Add(1)
		go func(region string) {
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

				r.m.Lock()
				r.redisMap[redis.InstanceId] = &redis
				r.m.Unlock()
			}
		}(region)
	}

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"redisLens": len(r.redisMap),
		"iden":      r.op.req.Iden,
	}).Warnln("async loop success, get all redis instance")
}

func (r *Redis) getRedis(id string) *r_kvstore.KVStoreInstanceInDescribeInstances {
	r.m.RLock()
	defer r.m.RUnlock()

	if redis, ok := r.redisMap[id]; ok {
		return redis
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
		redis := r.getRedis(instanceID)
		if redis == nil {
			continue
		}

		n9e := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("redis", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     instanceID,
		}

		n9e.Endpoint = redis.InstanceId
		tagsMap := map[string]string{
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

		n9e.BuildAndShift(tagsMap)
	}
}
