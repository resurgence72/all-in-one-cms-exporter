package ali

import (
	"context"
	"time"

	"watcher4metrics/pkg/bus"
	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/config"

	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	"github.com/panjf2000/ants/v2"
	"github.com/sirupsen/logrus"
)

const (
	ACS_VPC_EIP             common.MetricsType = "acs_vpc_eip"
	ACS_DDOS_IP             common.MetricsType = "acs_ddosdip"
	ACS_PHYSICAL_CONNECTION common.MetricsType = "acs_physical_connection"
	ACS_ECS_DASHBOARD       common.MetricsType = "acs_ecs_dashboard"
	ACS_SLB_DASHBOARD       common.MetricsType = "acs_slb_dashboard"
	ACS_ALB                 common.MetricsType = "acs_alb"
	ACS_WAF                 common.MetricsType = "waf"
	ACS_OPENAPI             common.MetricsType = "acs_openAPI"
	ACS_RDS_DASHBOARD       common.MetricsType = "acs_rds_dashboard"
	ACS_POLARDB             common.MetricsType = "acs_polardb"
	ACS_KVSTORE             common.MetricsType = "acs_kvstore"
	ACS_SMARTAG             common.MetricsType = "acs_smartag"
	ACS_KAFKA               common.MetricsType = "acs_kafka"
	ACS_CLICKHOUSE          common.MetricsType = "acs_clickhouse"
	ACS_HITSDB              common.MetricsType = "acs_hitsdb"
	ACS_SLS_DASHBOARD       common.MetricsType = "acs_sls_dashboard"

	ProviderName = common.AlibabaCloudProvider
)

var (
	registers     = make(map[common.MetricsType]common.MetricsGetter)
	aliyunLimiter = common.NewSemaphore(50, common.WithLimiter(50))
)

type Ali struct {
	sub chan any
	cli *cms.Client
	op  *operator
}

func New(sub chan any) *Ali {
	return &Ali{sub: sub}
}

func (a *Ali) setCli(req *AliReq) error {
	ep := config.Get().Provider.Ali.Endpoint
	client, err := cms.NewClientWithAccessKey(
		// 阿里相关云监控region仅作接入用,从 config.Endpoint 读取
		// 而获取实例需要使用到传入 regions，例如获取eip实例
		ep,
		req.Ak,
		req.As,
	)
	if err != nil {
		logrus.Errorf("region: %s client err: %s\n", ep, err)
		return err
	}

	// client设置 connect/read timeout
	client.SetConnectTimeout(time.Duration(30) * time.Second)
	client.SetReadTimeout(time.Duration(30) * time.Second)

	a.cli = client
	a.op = &operator{req: req}
	return nil
}

func (a *Ali) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case stream, ok := <-a.sub:
			if !ok {
				return
			}
			req, ok := stream.(bus.Stream).Data.(*AliReq)
			if !ok {
				logrus.Errorln("AliReq assert failed")
				continue
			}

			ali := new(Ali)
			if err := ali.setCli(req); err != nil {
				logrus.WithFields(logrus.Fields{
					"err": err,
				}).Errorln("ali get cli failed")
				return
			}
			ali.doEvent(ctx)
		}
	}
}

func (a *Ali) doEvent(ctx context.Context) {
	for ns, mg := range common.MetricsFilter(
		a.op.req.MetricNamespace,
		registers,
	) {
		ants.Submit(func(ns string, mg common.MetricsGetter) func() {
			return func() {
				a.do(ctx, mg.Inject(
					a.op,
					a.cli,
					ns,
				))
			}
		}(ns, mg))
	}
}

func (a *Ali) do(ctx context.Context, mg common.MetricsGetter) {
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
