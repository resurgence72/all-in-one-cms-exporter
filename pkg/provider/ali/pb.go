package ali

import (
	"watcher4metrics/pkg/common"
)

type AliReq struct {
	MetricNamespace string `json:"metric_namespace"`
	Iden            string `json:"iden"`
	Ak              string `json:"ak"`
	As              string `json:"as"`

	Dur int `json:"_meta_duration,string"`
}

type (
	PushFunc func(*transferData)
	Point    map[string]interface{}
	Points   []Point
)

func (p Point) Value() interface{} {
	for _, key := range []string{
		"Average",
		"Maximum",
		"Minimum",
		"Sum",
		"Value",
		"value",
		"Count",
		"count",
		"deviceNum",
	} {
		if v, ok := p[key]; ok {
			return v
		}
	}
	return -1
}

// point转换为夜莺结构的中间状态
type transferData struct {
	points Points
	metric string
	unit   string

	requestID string
}

func (a *AliReq) Decode() *AliReq {
	a.As = common.DecodeBase64(a.As)
	a.Ak = common.DecodeBase64(a.Ak)

	//a.Dur = 60
	return a
}
