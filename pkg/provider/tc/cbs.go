package tc

import (
	"context"
	"strconv"
	"sync"

	"watcher4metrics/pkg/common"

	"github.com/goccy/go-json"
	"github.com/sirupsen/logrus"
	cbs "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cbs/v20170312"
	monitor "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/monitor/v20180724"
)

type Cbs struct {
	meta

	cbsMap map[string]map[string]*cbs.Disk
}

func init() {
	registers[QCE_BLOCK_STORAGE] = new(Cbs)
}

func (c *Cbs) Inject(params ...any) common.MetricsGetter {
	return &Cbs{meta: newMeta(params...)}
}

func (c *Cbs) GetMetrics() error {
	metrics, err := c.op.getMetrics(
		c.clients["ap-shanghai"],
		c.namespace,
		nil,
	)
	if err != nil {
		return err
	}
	c.metrics = metrics
	return nil
}

func (c *Cbs) GetNamespace() string {
	return c.namespace
}

func (c *Cbs) Collector() {
	c.op.getMonitorData(
		c.clients,
		c.metrics,
		nil,
		func() InstanceBuilderFunc {
			return func(region string) []*monitor.Instance {
				return c.op.buildInstances(
					"diskId",
					func() []*string {
						var vs []*string
						for _, disk := range c.cbsMap[region] {
							vs = append(vs, disk.DiskId)
						}
						return vs
					}(),
					nil,
				)
			}
		}(),
		10,
		c.namespace,
		c.push,
	)
}

func (c *Cbs) AsyncMeta(ctx context.Context) {
	var (
		wg          sync.WaitGroup
		maxPageSize = 100
		parse       = func(region string, offset, limit int, container []*cbs.Disk) ([]*cbs.Disk, int, error) {
			bs, err := c.op.commonRequest(
				region,
				"cbs",
				"2017-03-12",
				"DescribeDisks",
				offset,
				limit,
				nil,
			)
			if err != nil {
				return nil, 0, err
			}

			resp := new(cbs.DescribeDisksResponse)
			if err = json.Unmarshal(bs, resp); err != nil {
				return nil, 0, err
			}
			return append(container, resp.Response.DiskSet...), len(resp.Response.DiskSet), nil
		}
		sem = common.Semaphore(10)
	)

	if c.cbsMap == nil {
		c.cbsMap = make(map[string]map[string]*cbs.Disk)
	}

	for region := range c.clients {
		wg.Add(1)
		sem.Acquire()

		go func(region string) {
			defer func() {
				wg.Done()
				sem.Release()
			}()

			var (
				offset    = 1
				pageNum   = 1
				container []*cbs.Disk
			)

			container, currLen, err := parse(region, offset, maxPageSize, container)
			if err != nil {
				return
			}

			// 分页
			for currLen == maxPageSize {
				offset = pageNum * maxPageSize
				container, currLen, err = parse(region, offset, maxPageSize, container)
				if err != nil {
					continue
				}
				pageNum++
			}

			c.m.Lock()
			if _, ok := c.cbsMap[region]; !ok {
				c.cbsMap[region] = make(map[string]*cbs.Disk)
			}
			c.m.Unlock()

			for i := range container {
				cc := container[i]

				c.m.Lock()
				c.cbsMap[region][*cc.DiskId] = cc
				c.m.Unlock()
			}
		}(region)
	}

	wg.Wait()
	logrus.WithFields(logrus.Fields{
		"cbsLens": len(c.cbsMap),
		"iden":    c.op.req.Iden,
	}).Warnln("async loop get all tc cbs success")
}

func (c *Cbs) push(transfer *transferData) {
	for _, point := range transfer.points {
		dID := point.Dimensions[0].Value

		disk := c.getCbs(transfer.region, dID)
		if disk == nil {
			return
		}

		for i, ts := range point.Timestamps {
			series := &common.MetricValue{
				Timestamp:    int64(*ts),
				Metric:       common.BuildMetric("cbs", transfer.metric),
				ValueUntyped: *point.Values[i],
				Endpoint:     *disk.DiskId,
			}

			tagsMap := map[string]string{
				"iden":      c.op.req.Iden,
				"provider":  ProviderName,
				"region":    transfer.region,
				"namespace": c.namespace,
				"unit_name": transfer.unit,

				"disk_id":    *disk.DiskId,
				"disk_name":  *disk.DiskName,
				"disk_size":  strconv.FormatUint(*disk.DiskSize, 10),
				"disk_usage": *disk.DiskUsage,
				"disk_type":  *disk.DiskType,
				"disk_state": *disk.DiskState,

				"instance_id":   *disk.InstanceId,
				"instance_type": *disk.InstanceType,

				"snapshot_count": strconv.FormatInt(*disk.SnapshotCount, 10),
				"snapshot_size":  strconv.FormatUint(*disk.SnapshotSize, 10),
			}

			for _, tag := range disk.Tags {
				if *tag.Value != "" {
					tagsMap[*tag.Key] = *tag.Value
				}
			}

			series.BuildAndShift(tagsMap)
			continue
		}

	}
}

func (c *Cbs) getCbs(region string, dID *string) *cbs.Disk {
	c.m.RLock()
	defer c.m.RUnlock()

	if diskM, ok := c.cbsMap[region]; ok {
		if disk, ok := diskM[*dID]; ok {
			return disk
		}
	}
	return nil
}
