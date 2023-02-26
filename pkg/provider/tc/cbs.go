package tc

import (
	"context"
	"github.com/goccy/go-json"
	"github.com/sirupsen/logrus"
	cbs "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cbs/v20170312"
	monitor "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/monitor/v20180724"
	"sync"
	"watcher4metrics/pkg/common"
)

type Cbs struct {
	op        *operator
	clients   map[string]*monitor.Client
	cbsMap    map[string]map[string]*cbs.Disk
	namespace string
	metrics   []*monitor.MetricSet

	m sync.RWMutex
}

func (c *Cbs) Inject(params ...interface{}) common.MetricsGetter {
	return &Cbs{
		op:        params[0].(*operator),
		clients:   params[1].(map[string]*monitor.Client),
		namespace: params[2].(string),
	}
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
	//TODO implement me
	panic("implement me")
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
