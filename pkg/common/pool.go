package common

import (
	"bytes"
	"strings"
	"sync"
)

var (
	SliceStringPool   = sync.Pool{New: func() any { return []string{} }}
	StringBuilderPool = sync.Pool{New: func() any { return &strings.Builder{} }}
	BytesPool         = sync.Pool{New: func() any { return &bytes.Buffer{} }}
	metricValuePool   = sync.Pool{New: func() any { return &MetricValue{TagsMap: make(map[string]string, 20)} }}
)

func GetMetricValueObj() *MetricValue {
	return metricValuePool.Get().(*MetricValue)
}
func PutMetricValue(mv *MetricValue) {
	mv.Metric = ""
	mv.Timestamp = 0
	mv.ValueUntyped = nil
	mv.Endpoint = ""
	if mv.TagsMap != nil {
		for k := range mv.TagsMap {
			delete(mv.TagsMap, k)
		}
	} else {
		mv.TagsMap = make(map[string]string, 20)
	}

	metricValuePool.Put(mv)
}
