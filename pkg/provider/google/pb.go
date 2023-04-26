package google

import (
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	metricpb "google.golang.org/genproto/googleapis/api/metric"

	"sync"

	"watcher4metrics/pkg/common"

	monitoring "cloud.google.com/go/monitoring/apiv3"
)

type meta struct {
	op        *operator
	client    *monitoring.MetricClient
	namespace string
	metrics   []*metricpb.MetricDescriptor
	m         sync.Mutex
}

func newMeta(params ...any) meta {
	return meta{
		op:        params[0].(*operator),
		client:    params[1].(*monitoring.MetricClient),
		namespace: params[2].(string),
	}
}

type transferData struct {
	points []*monitoringpb.TimeSeries
	metric *metricpb.MetricDescriptor
	m      sync.Mutex
}

type GoogleReq struct {
	MetricNamespace string `json:"metric_namespace"`
	Iden            string `json:"iden"`
	GSA             string `json:"gcp_sa"`

	Dur  int    `json:"_meta_duration,string"`
	Expr string `json:"_meta_expr"`
}

func (g *GoogleReq) Decode() *GoogleReq {
	g.GSA = common.DecodeBase64(g.GSA)
	//g.Dur = 60
	return g
}
