package ali

import (
	"context"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	"github.com/sirupsen/logrus"
	"strings"
	"time"
	"watcher4metrics/pkg/bus"
	"watcher4metrics/pkg/common"
)

const (
	ACS_VPC_EIP             metricsType = "acs_vpc_eip"
	ACS_DDOS_IP             metricsType = "acs_ddosdip"
	ACS_PHYSICAL_CONNECTION metricsType = "acs_physical_connection"
	ACS_ECS_DASHBOARD       metricsType = "acs_ecs_dashboard"
	ACS_SLB_DASHBOARD       metricsType = "acs_slb_dashboard"
	ACS_ALB                 metricsType = "acs_alb"
	ACS_WAF                 metricsType = "waf"
	ACS_OPENAPI             metricsType = "acs_openAPI"

	ProviderName = "ali"
	// 云监控api最新数据要在3min后才同步
	ALI_CMS_DELAY = 240
)

var (
	registers = make(map[metricsType]common.MetricsGetter)
)

type metricsType string

type Ali struct {
	sub chan interface{}
	cli *cms.Client
	op  *operator
}

func (m metricsType) toString() string {
	return string(m)
}

func New(sub chan interface{}) *Ali {
	return &Ali{sub: sub}
}

func (a *Ali) setCli(req *AliReq) error {
	client, err := cms.NewClientWithAccessKey(
		// 阿里相关云监控region仅作接入用，只需要指定默认的cn-shanghai即可
		// 而获取实例需要使用到传入 regions，例如获取eip实例
		"cn-shanghai",
		req.Ak,
		req.As,
	)
	if err != nil {
		logrus.Errorf("region: cn-shanghai client err: %s\n", err)
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
	for _, ns := range strings.Split(a.op.req.MetricNamespace, ",") {
		ns := metricsType(ns)
		if mg, ok := registers[ns]; ok {
			go a.do(ctx, mg.Inject(
				a.op,
				a.cli,
				ns.toString(),
			))
		}
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
