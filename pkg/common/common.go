package common

import (
	"context"
	"fmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"os"
	"strings"
	"sync"
	"watcher4metrics/pkg/relabel"
)

func GetDefaultEnv(key, defaultValue string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultValue
}

type Provider interface {
	// 启动任务定时collector
	Run(context.Context)
}

type MetricsGetter interface {
	// 为 metricsGetter 对象注入值
	Inject(...interface{}) MetricsGetter
	// 获取当前 namespace下的所有metrics指标
	GetMetrics() error
	// 获取当前的 namespace
	GetNamespace() string
	// 采集 metrics
	Collector()
	// 同步对应namespace metrics源数据
	AsyncMeta(context.Context)
}

// 夜莺上报结构
type MetricValue struct {
	Metric       string            `json:"metric" description:"指标"`
	Endpoint     string            `json:"endpoint" description:"通常是ip"`
	Timestamp    int64             `json:"timestamp" description:"时间戳秒"`
	ValueUntyped interface{}       `json:"value" description:"metric指标"`
	TagsMap      map[string]string `json:"tagsMap" description:"tags的map结构"` // {"a":1, "b"=2, "c="3} 保留2种格式，方便后端组件使用
}

func (m *MetricValue) BuildAndShift(tm map[string]string) {
	m.TagsMap = tm
	//fmt.Println(m)
	remoteCh <- m
}

func (m *MetricValue) relabel(rlbs []*relabel.Config) labels.Labels {
	var lbs labels.Labels

	// __name__
	lbs = append(lbs, labels.Label{Name: model.MetricNameLabel, Value: m.Metric})

	// ident
	lbs = append(lbs, labels.Label{Name: "ident", Value: m.Endpoint})

	for k, v := range m.TagsMap {
		lbs = append(lbs, labels.Label{
			Name:  k,
			Value: v,
		})
	}

	return relabel.Process(lbs, rlbs...)
}

func (m *MetricValue) Convert(rlbs []*relabel.Config) prompb.TimeSeries {
	value, err := ToFloat64(m.ValueUntyped)
	if err != nil {
		// If the Labels is empty, it means it is abnormal data
		return prompb.TimeSeries{}
	}

	// relabel
	lbs := m.relabel(rlbs)
	if len(lbs) == 0 {
		return prompb.TimeSeries{}
	}

	pt := prompb.TimeSeries{}
	pt.Samples = append(pt.Samples, prompb.Sample{
		Value:     value,
		Timestamp: m.Timestamp,
	})

	for _, lb := range lbs {
		pt.Labels = append(pt.Labels, prompb.Label{
			Name:  lb.Name,
			Value: lb.Value,
		})
	}
	return pt
}

// once close 模型，只关闭一次; 目前用作程序顺序控制
type CloseOnce struct {
	C     chan struct{}
	once  sync.Once
	Close func()
}

func NewCloseOnce() *CloseOnce {
	cOnce := &CloseOnce{
		C: make(chan struct{}),
	}
	cOnce.Close = func() {
		cOnce.once.Do(func() {
			close(cOnce.C)
		})
	}
	return cOnce
}

// 核心n9e chan 连接 provider和 consumer
var remoteCh = make(chan *MetricValue, 2048)

func GetN9eCh() chan *MetricValue {
	return remoteCh
}

func BuildMetric(mType, metric string) string {
	metric = strings.ReplaceAll(strings.ReplaceAll(metric, ".", "_"), "-", "_")
	return fmt.Sprintf("net_%s_%s", strings.ToLower(mType), strings.ToLower(metric))
}
