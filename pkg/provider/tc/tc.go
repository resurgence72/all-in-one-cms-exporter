package tc

import (
	"context"
	"watcher4metrics/pkg/bus"
	"watcher4metrics/pkg/common"

	"github.com/sirupsen/logrus"
	com "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	monitor "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/monitor/v20180724"
)

const (
	ProviderName = "tc"
	TC_CMS_DELAY = 60

	QCP_LB            common.MetricsType = "QCE/LB"
	QCE_CVM           common.MetricsType = "QCE/CVM"
	QCE_WAF           common.MetricsType = "QCE/WAF"
	QCE_BLOCK_STORAGE common.MetricsType = "QCE/BLOCK_STORAGE"
)

var registers = make(map[common.MetricsType]common.MetricsGetter)

type TC struct {
	clientSet map[string]*monitor.Client
	sub       chan interface{}
	op        *operator
}

func New(sub chan interface{}) *TC {
	return &TC{sub: sub}
}

func (t *TC) setCliSet(req *TCReq) error {
	t.op = &operator{req: req}

	credential := com.NewCredential(
		req.Sid,
		req.Skey,
	)
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = "monitor.tencentcloudapi.com"
	cpf.HttpProfile.ReqTimeout = 30

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
		go t.do(ctx, mg.Inject(
			t.op,
			t.clientSet,
			ns,
		))
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
