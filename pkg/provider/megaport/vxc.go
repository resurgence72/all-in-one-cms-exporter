package megaport

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/panjf2000/ants/v2"

	"watcher4metrics/pkg/common"

	"github.com/gorhill/cronexpr"
	"github.com/sirupsen/logrus"
)

type VXC struct {
	req        *MPReq
	productUid map[string]string
	metrics    []string
	namespace  string

	token string
}

func init() {
	registers[MEGEPORT_VXC] = new(VXC)
}

func (v *VXC) Inject(params ...any) common.MetricsGetter {
	return &VXC{
		req:       params[0].(*MPReq),
		token:     params[1].(string),
		namespace: params[2].(string),
	}
}

func (v *VXC) GetNamespace() string {
	return v.namespace
}

func (v *VXC) AsyncMeta(ctx context.Context) {
	// 获取 productUids
	body, err := v.reqWithRetry(http.MethodGet, DOMAIN+"/v2/products")
	if err != nil {
		logrus.Errorln("asyncloop req err ", err)
		return
	}

	tmp := &struct {
		Data []struct {
			AssociatedVxcs []struct {
				ProductUid  string `json:"productUid"`
				ProductName string `json:"productName"`
			} `json:"associatedVxcs"`
		} `json:"data"`
	}{}
	err = json.Unmarshal(body, tmp)
	if err != nil {
		logrus.Errorln("asyncloop unmarshal resp failed ", err)
		return
	}

	tMap := make(map[string]string)
	for _, data := range tmp.Data {
		for _, vxc := range data.AssociatedVxcs {
			tMap[vxc.ProductUid] = vxc.ProductName
		}
	}
	v.productUid = tMap

	logrus.WithFields(logrus.Fields{
		"productLens": len(tMap),
	}).Warnln("megaport asyncloop succ")
}

func (v *VXC) Collector() {
	// 采集metrics
	sem := common.NewSemaphore(10)

	from, to := v.getFromAndToTS()
	// 只需要前推10min的那个点
	targetTS := cronexpr.MustParse(v.req.Expr).Next(time.Now().Add(-11*time.Minute)).Unix() * 1e3

	for pid, pname := range v.productUid {
		for _, metric := range v.metrics {
			// 并发请求
			sem.Acquire()

			ants.Submit(func(pid, pname, metric string) func() {
				return func() {
					func(pid, pname, metric string) {
						defer sem.Release()
						u := fmt.Sprintf(
							DOMAIN+"/v2/product/%s/%s/telemetry?type=%s&from=%d&to=%d",
							strings.ToLower(v.namespace),
							pid,
							metric,
							from,
							to,
						)

						body, err := v.reqWithRetry(http.MethodGet, u)
						if err != nil {
							logrus.Errorln("Collector err", err)
							return
						}

						tmp := struct {
							Data []struct {
								Type    string  `json:"type"`
								SubType string  `json:"subtype"`
								Samples [][]any `json:"samples"`
								Unit    struct {
									Name     string `json:"name"`
									FullName string `json:"fullName"`
								} `json:"unit"`
							} `json:"data"`
						}{}

						err = json.Unmarshal(body, &tmp)
						if err != nil {
							logrus.Errorln("collector metrics Unmarshal failed", err)
							return
						}

						if len(tmp.Data) == 0 {
							logrus.WithFields(logrus.Fields{
								"pid":    pid,
								"pname":  pname,
								"metric": metric,
							}).Warnln("megaport data nodata")
							return
						}

						for _, data := range tmp.Data {
							points := data.Samples
							if len(points) == 0 {
								logrus.WithFields(logrus.Fields{
									"pid":    pid,
									"pname":  pname,
									"metric": metric,
								}).Warnln("megaport data.samples nodata")
								continue
							}

							// metrics 体现出方向和vxcname和metrics类型
							joinMetrics := fmt.Sprintf(
								"%s_%s",
								metric,
								strings.Replace(data.SubType, " ", "_", 1),
							)

							if point, find := v.findPoint(targetTS, points); find {
								ts, val := int64(point[0].(float64)), point[1].(float64)
								transfer := &transferData{
									endpoint: pname + "|" + pid,
									metric:   joinMetrics,
									tagMap: map[string]string{
										"unit_name":     data.Unit.Name,
										"unit_fullname": data.Unit.FullName,
									},
									ts:  ts,
									val: val,
								}
								ants.Submit(func() { v.push(transfer) })
								continue
							}

							// 如果未找到所需点，说明漏点
							// 1. 日志打印
							logrus.WithFields(logrus.Fields{
								"pid":      pid,
								"pname":    pname,
								"metric":   metric,
								"targetTS": targetTS,
							}).Errorln("vxc nodata")
						}
					}(pid, pname, metric)
				}
			}(pid, pname, metric))
		}
	}
}

func (v *VXC) findPoint(targetTS int64, points [][]any) ([]any, bool) {
	for i := range points {
		tsFloat, ok := points[i][0].(float64)
		// 断言检测，防止panic
		if !ok {
			return nil, false
		}

		ts := int64(tsFloat)
		if targetTS > ts {
			// 如果目标已经大于当前ts了， 说明没必要在寻找了
			// 也表示当前必然找不到targetts
			return nil, false
		}

		if targetTS == ts {
			return points[i], true
		}
	}
	return nil, false
}

func (v *VXC) getFromAndToTS() (int64, int64) {
	// 拿当前时刻 4m 30s 前的 至 当前时刻 15m 30s 的间隔范围内的点
	// 两个 30s 保证每次调用区间都是闭合的
	to := time.Now().Add(-5*time.Minute + 30*time.Second)
	from := to.Add(-15*time.Minute - 30*time.Second)
	// to := time.Now()
	// from := to.Add(-20 * time.Minute)
	return from.UnixMilli(), to.UnixMilli()
}

func (v *VXC) push(transfer *transferData) {
	series := &common.MetricValue{
		Metric:       common.BuildMetric("vxc", transfer.metric),
		Endpoint:     transfer.endpoint,
		Timestamp:    transfer.ts,
		ValueUntyped: transfer.val,
	}

	transfer.tagMap["iden"] = v.req.Iden
	transfer.tagMap["provider"] = ProviderName
	transfer.tagMap["namespace"] = v.namespace

	series.BuildAndShift(transfer.tagMap)
}

func (v *VXC) reqWithRetry(method, url string) ([]byte, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	req, err := http.NewRequestWithContext(ctx,
		method,
		url,
		nil,
	)
	if err != nil {
		logrus.Errorln("NewRequestWithContext failed ", err)
		return nil, err
	}

	req.Header.Add("X-Auth-Token", v.token)
	req.Header.Add("Content-Type", "application/json")

	// retry
	resp, err := common.RetryDo(req, 5)
	if err != nil {
		logrus.Errorln("retry failed ", err)
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logrus.Errorln("readall failed ", err)
		return nil, err
	}

	return body, nil
}

func (v *VXC) GetMetrics() error {
	// 获取当前ns 下的所有metrics
	body, err := v.reqWithRetry(http.MethodGet, DOMAIN+"/v2/products/telemetry")
	if err != nil {
		return err
	}

	var tmp []struct {
		ProductType string `json:"productType"`
		Metrics     []struct {
			Name string `json:"name"`
		}
	}
	err = json.Unmarshal(body, &tmp)
	if err != nil {
		logrus.Errorln("get ns metrics unmarshal failed ", err)
		return err
	}

	for _, data := range tmp {
		pt := data.ProductType
		ms := data.Metrics

		if pt == v.namespace {
			for _, m := range ms {
				metric := m.Name
				v.metrics = append(v.metrics, metric)
			}
			break
		}
	}
	return nil
}
