package google

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
	"watcher4metrics/pkg/common"
)

type Gce struct {
	op        *operator
	client    *monitoring.MetricClient
	namespace string
	metrics   []string
	// 映射 instance_id 和 ip
	gceMap map[uint64]string

	m sync.Mutex
}

func init() {
	registers[GOOGLE_GCE] = new(Gce)
}

func (e *Gce) Inject(params ...interface{}) common.MetricsGetter {
	return &Gce{
		op:        params[0].(*operator),
		client:    params[1].(*monitoring.MetricClient),
		namespace: params[2].(string),
	}
}

func (e *Gce) Collector() {
	e.op.listTimeSeries(
		e.client,
		e.metrics,
		5,
		e.push,
		[]string{
			"metric.instance_name",
			"resource.instance_id",
			"resource.project_id",
			"resource.zone",
		},
	)
}

func (e *Gce) push(transfer *transferData) {
	transfer.m.Lock()
	defer transfer.m.Unlock()

	for _, series := range transfer.points {
		metricName := e.op.buildMetric(series.Metric.Type)

		points := series.GetPoints()
		if points == nil {
			return
		}

		point := points[len(points)-1]
		ts := point.Interval.EndTime.GetSeconds()
		value := e.op.getPointValue(series.ValueType.String(), point)

		metricLabels := series.Metric.Labels
		resourceLabels := series.Resource.Labels

		n9e := &common.MetricValue{
			Metric:       common.BuildMetric("gce", metricName),
			Timestamp:    ts,
			ValueUntyped: value,
		}

		tagsMap := map[string]string{
			"provider":  ProviderName,
			"iden":      e.op.req.Iden,
			"namespace": e.namespace,
		}
		if in, ok := metricLabels["instance_name"]; ok {
			tagsMap["instance_name"] = in
			n9e.Endpoint = in
		}

		if ii, ok := resourceLabels["instance_id"]; ok {
			tagsMap["instance_id"] = ii
			ii, err := strconv.ParseInt(ii, 10, 64)
			if err == nil {
				e.m.Lock()
				if ip, ok := e.gceMap[uint64(ii)]; ok {
					tagsMap["network_ip"] = ip
				}
				e.m.Unlock()
			}
		}

		if pid, ok := resourceLabels["project_id"]; ok {
			tagsMap["project_id"] = pid

			if pn, ok := e.op.projects.Load(pid); ok {
				tagsMap["project_mark"] = pn.(string)
			}
		}

		if region, ok := resourceLabels["zone"]; ok {
			tagsMap["region"] = region
		}

		e.op.pushTo(n9e, tagsMap)
	}
}

func (e *Gce) AsyncMeta(ctx context.Context) {
	timeout, _ := context.WithTimeout(context.TODO(), time.Duration(30)*time.Second)
	cs, err := compute.NewService(
		timeout,
		option.WithCredentialsJSON([]byte(e.op.req.GSA)),
	)

	if err != nil {
		logrus.Errorln("gcp asyncMeta failed ", err)
		return
	}

	var (
		wg  sync.WaitGroup
		sem = common.Semaphore(200)
	)

	if e.gceMap == nil {
		e.gceMap = make(map[uint64]string)
	}

	do := func(pid, zone string) {
		defer func() {
			wg.Done()
			sem.Release()
		}()
		req := cs.Instances.List(pid, zone)
		if err := req.Pages(ctx, func(page *compute.InstanceList) error {
			for _, instance := range page.Items {
				ips := instance.NetworkInterfaces

				if len(ips) == 0 {
					continue
				}

				var ip string
				if len(ips) == 1 {
					ip = ips[0].NetworkIP
				} else {
					var is []string
					for _, i := range ips {
						is = append(is, i.NetworkIP)
					}
					ip = strings.Join(is, ",")
				}

				e.m.Lock()
				e.gceMap[instance.Id] = ip
				e.m.Unlock()
			}
			return nil
		}); err != nil {
			logrus.Errorln("req.Pages failed", err)
			return
		}
	}

	e.op.projects.Range(func(k, v interface{}) bool {
		pid := k.(string)

		for _, zone := range e.op.getZones() {
			wg.Add(1)
			sem.Acquire()
			go do(pid, zone)
		}
		return true
	})

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"gceLens": len(e.gceMap),
		"iden":    e.op.req.Iden,
	}).Warnln("async loop success, get all gce instance")
}

func (e *Gce) GetNamespace() string {
	return e.namespace
}

func (e *Gce) GetMetrics() error {
	metrics, err := e.op.getMetrics(e.client, "compute.googleapis.com")
	if err != nil {
		return err
	}
	e.metrics = metrics
	return nil
}
