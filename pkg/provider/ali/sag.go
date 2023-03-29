package ali

import (
	"context"
	"strconv"
	"sync"

	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali/parser"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/smartag"
	"github.com/sirupsen/logrus"
)

type SAG struct {
	meta

	sagMap map[string]*smartag.SmartAccessGateway
}

func init() {
	registers[ACS_SMARTAG] = new(SAG)
}

func (s *SAG) Inject(params ...any) common.MetricsGetter {
	return &SAG{meta: newMeta(params...)}
}

func (s *SAG) GetMetrics() error {
	metrics, err := s.op.getMetrics(
		s.client,
		s.namespace,
		nil,
		nil,
	)
	if err != nil {
		return err
	}
	s.metrics = metrics
	return nil
}

func (s *SAG) GetNamespace() string {
	return s.namespace
}

func (s *SAG) Collector() {
	s.op.getMetricLastData(
		s.client,
		s.metrics,
		s.namespace,
		s.push,
		nil,
		[]string{"instanceId"},
	)
}

func (s *SAG) push(transfer *transferData) {
	for _, point := range transfer.points {
		p, ok := point["instanceId"]
		if !ok {
			continue
		}

		instanceID := p.(string)
		sag, ok := s.sagMap[instanceID]
		if !ok {
			continue
		}

		series := &common.MetricValue{
			Timestamp:    int64(point["timestamp"].(float64)) / 1e3,
			Metric:       common.BuildMetric("sag", transfer.metric),
			ValueUntyped: point.Value(),
			Endpoint:     instanceID,
		}

		series.TagsMap = map[string]string{
			"instance_id":         instanceID,
			"instance_name":       sag.Name,
			"region":              sag.City,
			"serial_number":       sag.SerialNumber,
			"status":              sag.Status,
			"up_bandwidth_4g":     strconv.Itoa(sag.UpBandwidth4G),
			"up_bandwidth_wan":    strconv.Itoa(sag.UpBandwidthWan),
			"max_bandwidth":       sag.MaxBandwidth,
			"hardware_version":    sag.HardwareVersion,
			"associated_ccn_name": sag.AssociatedCcnName,
			"end_time":            strconv.FormatInt(sag.EndTime, 10),

			"provider":  ProviderName,
			"iden":      s.op.req.Iden,
			"namespace": s.namespace,
			"unit_name": transfer.unit,
		}

		series.BuildAndShift()
	}
}

func (s *SAG) AsyncMeta(ctx context.Context) {
	var (
		maxPageSize = 50
		parse       = func(region string, pageNum int, container []smartag.SmartAccessGateway) ([]smartag.SmartAccessGateway, int, error) {
			bytes, err := s.op.commonRequest(
				region,
				"smartag",
				"2018-03-13",
				"DescribeSmartAccessGateways",
				pageNum,
				maxPageSize,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(smartag.DescribeSmartAccessGatewaysResponse)
			if err := parser.Parser().Unmarshal(bytes, resp); err != nil {
				return nil, 0, err
			}
			return append(container, resp.SmartAccessGateways.SmartAccessGateway...), len(resp.SmartAccessGateways.SmartAccessGateway), nil
		}
	)

	if s.sagMap == nil {
		s.sagMap = make(map[string]*smartag.SmartAccessGateway)
	}

	s.op.async(s.op.getRegions(), func(region string, wg *sync.WaitGroup) {
		defer wg.Done()
		var (
			pageNum   = 1
			container []smartag.SmartAccessGateway
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
			sag := container[i]

			s.m.Lock()
			s.sagMap[sag.SmartAGId] = &sag
			s.m.Unlock()
		}
	})

	logrus.WithFields(logrus.Fields{
		"sagLens": len(s.sagMap),
		"iden":    s.op.req.Iden,
	}).Warnln("async loop success, get all sag instance")
}
