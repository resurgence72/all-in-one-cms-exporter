package megaport

import (
	"context"
	"github.com/goccy/go-json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/sirupsen/logrus"
	"watcher4metrics/pkg/bus"
	"watcher4metrics/pkg/common"
)

const (
	ProviderName = "megaport"
	DOMAIN       = "https://api.megaport.com"

	MEGEPORT_VXC common.MetricsType = "VXC"
)

var registers = make(map[common.MetricsType]common.MetricsGetter)

type MPPort struct {
	sub   chan interface{}
	token string

	req *MPReq
}

func New(sub chan interface{}) *MPPort {
	return &MPPort{sub: sub}
}

func (m *MPPort) setCli(req *MPReq) error {
	postData := url.Values{}
	postData.Add("username", req.Username)
	postData.Add("password", req.Password)

	resp, err := http.Post(
		DOMAIN+"/v2/login",
		"application/x-www-form-urlencoded",
		strings.NewReader(postData.Encode()),
	)

	if err != nil {
		logrus.Errorln("megaport set cli err", err)
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logrus.Errorln("megaport ReadAll err", err)
		return err
	}

	tmp := &struct {
		Data map[string]interface{} `json:"data"`
	}{}

	err = json.Unmarshal(body, tmp)
	if err != nil {
		logrus.Errorln("megaport Unmarshal err", err)
		return err
	}

	token, ok := tmp.Data["session"].(string)
	if !ok {
		errM := errors.New("megaport get token failed")
		logrus.Errorln(errM.Error())
		return errM
	}
	m.token = token
	m.req = req
	return nil
}

func (m *MPPort) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case stream, ok := <-m.sub:
			if !ok {
				return
			}
			req, ok := stream.(bus.Stream).Data.(*MPReq)
			if !ok {
				logrus.Errorln("MPReq assert failed")
				continue
			}

			mp := new(MPPort)
			if err := mp.setCli(req); err != nil {
				logrus.WithFields(logrus.Fields{
					"err": err,
				}).Errorln("mp get cli failed")
				return
			}
			mp.doEvent(ctx)
		}
	}
}

func (m *MPPort) doEvent(ctx context.Context) {
	for ns, mg := range common.MetricsFilter(
		m.req.MetricNamespace,
		registers,
	) {
		go m.do(ctx, mg.Inject(
			m.req,
			m.token,
			ns,
		))
	}
}

func (m *MPPort) do(ctx context.Context, mg common.MetricsGetter) {
	mg.AsyncMeta(ctx)
	if err := mg.GetMetrics(); err != nil {
		logrus.WithFields(logrus.Fields{
			"err":      err,
			"ns":       mg.GetNamespace(),
			"provider": ProviderName,
		}).Errorln("getnsmetrics failed")
		return
	}
	mg.Collector()
}
