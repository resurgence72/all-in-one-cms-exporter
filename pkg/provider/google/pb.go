package google

import (
	"sync"

	"watcher4metrics/pkg/common"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
)

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
