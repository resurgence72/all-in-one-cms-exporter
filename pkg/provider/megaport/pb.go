package megaport

import (
	"watcher4metrics/pkg/common"
)

type transferData struct {
	metric   string
	endpoint string
	tagMap   map[string]string
	ts       int64
	val      float64
}

type MPReq struct {
	MetricNamespace string `json:"metric_namespace"`
	Iden            string `json:"iden"`
	Username        string `json:"username"`
	Password        string `json:"password"`

	Dur  int    `json:"_meta_duration,string"`
	Expr string `json:"_meta_expr"`
}

func (m *MPReq) Decode() *MPReq {
	m.Username = common.DecodeBase64(m.Username)
	m.Password = common.DecodeBase64(m.Password)

	//m.Dur = 500
	return m
}
