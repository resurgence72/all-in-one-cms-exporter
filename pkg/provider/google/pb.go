package google

import (
	"sync"

	monitoringpb "google.golang.org/genproto/googleapis/monitoring/v3"
	"watcher4metrics/pkg/common"
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

var projectIDToName = map[string]string{
	"bk-common":              "bk-common",
	"baloot-343509":          "LudoCafe",
	"balootkingdom":          "BalootKingdom",
	"bcjh-298308":            "hwbcjh",
	"bk-support":             "bk-support",
	"boke-fbaad":             "BoKe",
	"bxdomino":               "bxdomino",
	"data-center-286107":     "data-center",
	"domino-287405":          "domino",
	"dreamland-300701":       "dreamland",
	"dummy-318212":           "dummy",
	"gomaster-310307":        "gomaster",
	"guangfen-339109":        "sshg",
	"hsby-326007":            "hsby",
	"hwh5bkby":               "hwh5bkby",
	"hxxsg-342310":           "hxxsg",
	"jackpotfishing2-310802": "jackpotfishing2",
	"ludo-288906":            "ludo",
	"mdwytwb":                "mdwytwb",
	"mlqj-294706":            "mlqj",
	"oddmanange-347803":      "oddmanange",
	"owol-293606":            "owol",
	"pinballpirates":         "PinballPirates",
	"pokerjourney":           "pokerjourney",
	"romance-285903":         "romance",
	"samsara-299408":         "samsara",
	"shared-host-vpc-285809": "shared-host-vpc",
	"slotsmash":              "slotsmash",
	"solitaire-290802":       "solitaire",
	"spades-292307":          "spades",
	"sshg-hf":                "sshg-hf",
	"stories-285903":         "stories",
	"tilemaster-global":      "TileMaster",
	"tongits-340903":         "tongits",
	"torpedo-337909":         "torpedo",
	"yxlrtwb":                "yxlrtwb",
	"zesenslots":             "zesenslots",
}
