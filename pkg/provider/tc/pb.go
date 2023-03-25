package tc

import (
	"strings"
	"sync"

	"watcher4metrics/pkg/common"

	monitor "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/monitor/v20180724"
)

type meta struct {
	op        *operator
	clients   map[string]*monitor.Client
	namespace string
	metrics   []*monitor.MetricSet

	m sync.RWMutex
}

func newMeta(params ...any) meta {
	return meta{
		op:        params[0].(*operator),
		clients:   params[1].(map[string]*monitor.Client),
		namespace: params[2].(string),
	}
}

type transferData struct {
	points []*monitor.DataPoint
	metric string
	unit   string
	region string

	requestID string
}

type TCReq struct {
	MetricNamespace string `json:"metric_namespace"`
	Iden            string `json:"iden"`
	Sid             string `json:"sid"`
	Skey            string `json:"skey"`

	Dur int `json:"_meta_duration,string"`
}

func (t *TCReq) Decode() *TCReq {
	t.Sid = common.DecodeBase64(t.Sid)
	t.Skey = common.DecodeBase64(t.Skey)
	t.MetricNamespace = strings.ToUpper(t.MetricNamespace)

	// t.Dur = 60
	return t
}

type antFunc = func(string, *sync.WaitGroup, *common.Semaphore)

func warpFunc(region string, wg *sync.WaitGroup, sem *common.Semaphore, f antFunc) func() {
	return func() {
		f(region, wg, sem)
	}
}
