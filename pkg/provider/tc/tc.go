package tc

import (
	"context"

	"watcher4metrics/pkg/bus"
	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/config"

	"github.com/panjf2000/ants/v2"
	"github.com/sirupsen/logrus"
	com "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	monitor "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/monitor/v20180724"
)

const (
	ProviderName = common.TencentCloudProvider
	TC_CMS_DELAY = 60

	QCP_LB            common.MetricsType = "QCE/LB"
	QCE_CVM           common.MetricsType = "QCE/CVM"
	QCE_WAF           common.MetricsType = "QCE/WAF"
	QCE_BLOCK_STORAGE common.MetricsType = "QCE/BLOCK_STORAGE"
	QCE_LB_PUBLIC     common.MetricsType = "QCE/LB_PUBLIC"
	QCE_LB_PRIVATE    common.MetricsType = "QCE/LB_PRIVATE"
)

var (
	registers      = make(map[common.MetricsType]common.MetricsGetter)
	tencentLimiter = common.NewSemaphore(20, common.WithLimiter(20))
)

type TC struct {
	clientSet map[string]*monitor.Client
	sub       chan any
	op        *operator
}

func New(sub chan any) *TC {
	return &TC{sub: sub}
}

func (t *TC) setCliSet(req *TCReq) error {
	// 腾讯云拉取 metricData 接口并发限制为20次
	t.op = &operator{req: req, endpoint: config.Get().Provider.Tc.Endpoint}

	credential := com.NewCredential(
		req.Sid,
		req.Skey,
	)
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = "monitor.tencentcloudapi.com"
	cpf.HttpProfile.ReqTimeout = 30

	// 同步项目映射
	if err := t.op.asyncProjectMeta(); err != nil {
		logrus.Errorln("asyncProjectMeta failed", err)
	}

	regions := t.op.getRegions()
	cSet := make(map[string]*monitor.Client, len(regions))
	for _, region := range regions {
		client, err := monitor.NewClient(
			credential,
			region,
			cpf,
		)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"err":       err,
				"tc-region": region,
			}).Errorln("tc newClient failed")
			return err
		}
		cSet[region] = client
	}

	t.clientSet = cSet
	return nil
}

func (t *TC) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case stream, ok := <-t.sub:
			if !ok {
				return
			}
			req, ok := stream.(bus.Stream).Data.(*TCReq)
			if !ok {
				logrus.Errorln("TCReq assert failed")
				continue
			}

			tc := new(TC)
			if err := tc.setCliSet(req); err != nil {
				logrus.WithFields(logrus.Fields{
					"err": err,
				}).Errorln("ali get cli failed")
				return
			}
			tc.doEvent(ctx)
		}
	}
}

func (t *TC) Close() {
	close(t.sub)
}

func (t *TC) doEvent(ctx context.Context) {
	for ns, mg := range common.MetricsFilter(
		t.op.req.MetricNamespace,
		registers,
	) {
		ants.Submit(func(ns string, mg common.MetricsGetter) func() {
			return func() {
				t.do(ctx, mg.Inject(
					t.op,
					t.clientSet,
					ns,
				))
			}
		}(ns, mg))
	}
}

func (t *TC) do(ctx context.Context, mg common.MetricsGetter) {
	mg.AsyncMeta(ctx)
	if err := mg.GetMetrics(); err != nil {
		logrus.WithFields(logrus.Fields{
			"err":      err,
			"ns":       mg.GetNamespace(),
			"provider": ProviderName,
		}).Errorln("GetMetrics failed")
	}
	mg.Collector()
}
