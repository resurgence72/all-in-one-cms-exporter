package ali

import (
	"context"
	"strconv"
	"sync"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/alikafka"
	"github.com/sirupsen/logrus"
)

type Kafka struct {
	meta

	kafkaMap map[string]*alikafka.InstanceVO
}

func (k *Kafka) Inject(params ...interface{}) common.MetricsGetter {
	return &Kafka{meta: newMeta(params)}
}

func (k *Kafka) GetMetrics() error {
	metrics, err := k.op.getMetrics(
		k.client,
		k.namespace,
		nil,
		nil,
	)
	if err != nil {
		return err
	}
	k.metrics = metrics
	return nil
}

func (k *Kafka) GetNamespace() string {
	return k.namespace
}

func (k *Kafka) Collector() {
	k.op.getMetricLastData(
		k.client,
		k.metrics,
		5,
		k.namespace,
		k.push,
		nil,
		[]string{"instanceId", "topic"},
	)
}

func (k *Kafka) AsyncMeta(ctx context.Context) {
	var (
		wg          sync.WaitGroup
		maxPageSize = 100
		parse       = func(region string, pageNum int, container []alikafka.InstanceVO) ([]alikafka.InstanceVO, int, error) {
			bytes, err := k.op.commonRequest(
				region,
				"alikafka",
				"2019-09-16",
				"GetInstanceList",
				pageNum,
				maxPageSize,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(alikafka.GetInstanceListResponse)
			if err := parser.Parser().Unmarshal(bytes, resp); err != nil {
				return nil, 0, err
			}
			return append(container, resp.InstanceList.InstanceVO...), len(resp.InstanceList.InstanceVO), nil
		}
	)

	if k.kafkaMap == nil {
		k.kafkaMap = make(map[string]*alikafka.InstanceVO)
	}

	for _, region := range k.op.getRegions() {
		wg.Add(1)
		go func(region string) {
			defer wg.Done()
			var (
				pageNum   = 1
				container []alikafka.InstanceVO
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
				kafka := container[i]

				k.m.Lock()
				k.kafkaMap[kafka.InstanceId] = &kafka
				k.m.Unlock()
			}
		}(region)
	}

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"kafkaLens": len(k.kafkaMap),
		"iden":      k.op.req.Iden,
	}).Warnln("async loop success, get all kafka instance")
}

func (k *Kafka) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["instanceId"]
		if !ok {
			continue
		}
		instanceID := p.(string)
		kfk, ok := k.kafkaMap[instanceID]
		if !ok {
			continue
		}

		n9e := &common.MetricValue{
			Metric:       common.BuildMetric("alikafka", transfer.metric),
			Endpoint:     kfk.Name,
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			ValueUntyped: point.Value(),
		}

		tagsMap := map[string]string{
			"instance_id":     kfk.InstanceId,
			"instance_name":   kfk.Name,
			"spec_type":       kfk.SpecType,
			"disk_size":       strconv.Itoa(kfk.DiskSize),
			"disk_type":       strconv.Itoa(kfk.DiskType),
			"service_status":  strconv.Itoa(kfk.ServiceStatus),
			"eip_max":         strconv.Itoa(kfk.EipMax),
			"region":          kfk.RegionId,
			"msg_retain":      strconv.Itoa(kfk.MsgRetain),
			"topic_num_limit": strconv.Itoa(kfk.TopicNumLimit),
			"io_max":          strconv.Itoa(kfk.IoMax),

			"provider":  ProviderName,
			"iden":      k.op.req.Iden,
			"namespace": k.namespace,
			"unit_name": transfer.unit,
		}

		if topic, ok := point["topic"]; ok {
			tagsMap["topic"] = topic.(string)
		}

		for _, tag := range kfk.Tags.TagVO {
			if tag.Value != "" {
				tagsMap[tag.Key] = tag.Value
			}
		}

		n9e.BuildAndShift(tagsMap)
	}
}

func init() {
	registers[ACS_KAFKA] = new(Kafka)
}
