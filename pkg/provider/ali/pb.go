package ali

import (
	"sync"

	"watcher4metrics/pkg/common"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
)

type meta struct {
	op        *operator
	namespace string
	metrics   []*cms.Resource
	client    *cms.Client

	m  sync.RWMutex
	mp sync.Map
}

func newMeta(params ...any) meta {
	return meta{
		op:        params[0].(*operator),
		client:    params[1].(*cms.Client),
		namespace: params[2].(string),
	}
}

type metricsDataPuller interface {
	pull(
		cli *cms.Client,
		metrics []*cms.Resource,
		ns string,
		push PushFunc,
		ds *string, // Dimensions 维度
		groupBy []string,
		period int,
	)
}

type AliReq struct {
	MetricNamespace string `json:"metric_namespace"`
	Iden            string `json:"iden"`
	Ak              string `json:"ak"`
	As              string `json:"as"`

	Dur int `json:"_meta_duration,string"`
}

func (a *AliReq) Decode() *AliReq {
	a.As = common.DecodeBase64(a.As)
	a.Ak = common.DecodeBase64(a.Ak)

	// a.Dur = 60
	return a
}

type (
	PushFunc func(*transferData)
	Point    map[string]any
	Points   []Point
)

func (p Point) Value() any {
	for _, key := range []string{
		"Average",
		"Value",
		"value",
		"Count",
		"count",
		"Sum",
		"Maximum",
		"Minimum",
		"deviceNum",
	} {
		if v, ok := p[key]; ok {
			return v
		}
	}
	return -1
}

// point转换为rw结构的中间状态
type transferData struct {
	points Points
	metric string
	unit   string

	requestID string
}

type antFunc = func(string, *sync.WaitGroup)

func warpFunc(region string, wg *sync.WaitGroup, f antFunc) func() {
	return func() {
		f(region, wg)
	}
}
