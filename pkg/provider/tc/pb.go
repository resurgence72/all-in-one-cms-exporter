package tc

import (
	monitor "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/monitor/v20180724"
	"watcher4metrics/pkg/common"
)

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

	//t.Dur = 60
	return t
}
