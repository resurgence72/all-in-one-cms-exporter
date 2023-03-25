package google

import (
	"context"
	"time"

	"watcher4metrics/pkg/bus"
	"watcher4metrics/pkg/common"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	"github.com/panjf2000/ants/v2"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/option"
)

const (
	ProviderName = common.GoogleCloudProvider

	GOOGLE_LB           common.MetricsType = "loadbalancing"
	GOOGLE_GCE          common.MetricsType = "gce"
	GOOGLE_INTERCONNECT common.MetricsType = "interconnect"
)

var registers = make(map[common.MetricsType]common.MetricsGetter)

type Google struct {
	iden string
	sub  chan any
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

	cc, err := cloudresourcemanager.NewService(
		ctx,
		option.WithCredentialsJSON([]byte(g.op.req.GSA)),
	)
	if err != nil {
		logrus.Errorln("gcp asyncMeta get projects failed ", err)
		return nil
	}

	_ = cc.Projects.List().Pages(ctx, func(page *cloudresourcemanager.ListProjectsResponse) error {
		for _, p := range page.Projects {
			g.op.projects.Store(p.ProjectId, p.Name)
		}
		return nil
	})

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
	for ns, mg := range common.MetricsFilter(
		g.op.req.MetricNamespace,
		registers,
	) {
		ants.Submit(func(ns string, mg common.MetricsGetter) func() {
			return func() {
				g.do(ctx, mg.Inject(
					g.op,
					g.getCli(),
					ns,
				))
			}
		}(ns, mg))
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

func New(sub chan any) *Google {
	return &Google{sub: sub}
}
