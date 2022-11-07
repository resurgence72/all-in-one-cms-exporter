package google

import (
	"context"
	"strings"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"watcher4metrics/pkg/bus"
	"watcher4metrics/pkg/common"
)

const (
	ProviderName                    = "google"
	GOOGLE_LB           metricsType = "loadbalancing"
	GOOGLE_GCE          metricsType = "gce"
	GOOGLE_INTERCONNECT metricsType = "interconnect"
)

var registers = make(map[metricsType]common.MetricsGetter)

type metricsType string

func (m metricsType) toString() string {
	return string(m)
}

type Google struct {
	iden string
	sub  chan interface{}
	op   *operator
}

// 为每个 metricsGetter 分配一个cli, 各自用完独立释放  简化逻辑
func (g *Google) getCli() *monitoring.MetricClient {
	ctx, _ := context.WithTimeout(context.TODO(), time.Duration(30)*time.Second)
	cli, err := monitoring.NewMetricClient(
		ctx,
		option.WithCredentialsJSON([]byte(g.op.req.GSA)),
	)
	if err != nil {
		logrus.Errorln("NewMetricClient failed ", err)
		return nil
	}
	return cli
}

func (g *Google) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case stream, ok := <-g.sub:
			if !ok {
				return
			}
			req, ok := stream.(bus.Stream).Data.(*GoogleReq)
			if !ok {
				logrus.Errorln("GoogleReq assert failed")
				continue
			}

			(&Google{op: &operator{req: req}}).doEvent(ctx)
		}
	}
}

func (g *Google) doEvent(ctx context.Context) {
	for _, ns := range strings.Split(g.op.req.MetricNamespace, ",") {
		ns := metricsType(ns)
		if mg, ok := registers[ns]; ok {
			go g.do(ctx, mg.Inject(
				g.op,
				g.getCli(),
				ns.toString(),
			))
		}
	}
}

func (g *Google) do(ctx context.Context, mg common.MetricsGetter) {
	mg.AsyncMeta(ctx)
	if err := mg.GetMetrics(); err != nil {
		logrus.WithFields(logrus.Fields{
			"err":      err,
			"ns":       mg.GetNamespace(),
			"provider": ProviderName,
		}).Errorln("GetMetrics failed")
		return
	}
	mg.Collector()
}

func New(sub chan interface{}) *Google {
	return &Google{sub: sub}
}
