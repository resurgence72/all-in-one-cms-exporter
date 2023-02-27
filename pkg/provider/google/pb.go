package google

import (
	"sync"

	"watcher4metrics/pkg/common"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
)

type meta struct {
	op        *operator
	client    *monitoring.MetricClient
	namespace string
	metrics   []string
	m         sync.Mutex
}

func newMeta(params ...interface{}) meta {
	return meta{
		op:        params[0].(*operator),
		client:    params[1].(*monitoring.MetricClient),
		namespace: params[2].(string),
	}
}

type GoogleReq struct {
	MetricNamespace string `json:"metric_namespace"`
	Iden            string `json:"iden"`
	GSA             string `json:"gcp_sa"`

	Dur  int    `json:"_meta_duration,string"`
	Expr string `json:"_meta_expr"`
}

type transferData struct {
	series map[uint64]struct{}
	points []*monitoringpb.TimeSeries
	m      sync.Mutex
}

func (g *GoogleReq) Decode() *GoogleReq {
	g.GSA = common.DecodeBase64(g.GSA)
	return g
}
