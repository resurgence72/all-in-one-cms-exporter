package bus

import (
	"math/rand"
	"sync"
	"time"
)

var (
	bus    *Bus
	jitter = 1000
)

type (
	// 订阅者为一个管道
	subscriber chan any
	// topic是一个过滤器
	topicFunc func(s Stream) bool
)

type Stream struct {
	Topic string
	Data  any
}

type Bus struct {
	subscribers map[subscriber]topicFunc // 订阅者信息map
	m           sync.RWMutex             // 读写锁
	timeout     time.Duration            // 超时控制
	buffer      int                      // 缓冲队列大小
}

func New(busTimeout time.Duration, buffer int) *Bus {
	bus = &Bus{
		timeout:     busTimeout,
		buffer:      buffer,
		subscribers: make(map[subscriber]topicFunc),
	}
	return bus
}

func Get() *Bus {
	return bus
}

// 添加订阅者，指定topicFunc
func (b *Bus) SubscriberTopic(topic topicFunc) chan any {
	ch := make(chan any, b.buffer)

	b.m.Lock()
	defer b.m.Unlock()
	b.subscribers[ch] = topic
	return ch
}

// 添加订阅者，不指定topic,订阅全部
func (b *Bus) Subscribe() chan any {
	return b.SubscriberTopic(nil)
}

// 发布一个主题
func (b *Bus) Publish(s Stream) {
	b.m.RLock()
	defer b.m.RUnlock()

	var wg sync.WaitGroup
	for sub, topic := range b.subscribers {
		wg.Add(1)
		go b.sendTopic(sub, topic, s, &wg)
	}
	wg.Wait()
}

// 退出订阅
func (b *Bus) Evict(sub chan any) {
	b.m.Lock()
	defer b.m.Unlock()

	delete(b.subscribers, sub)
	close(sub)
}

// 具体发布主题逻辑
func (b *Bus) sendTopic(sub subscriber, topic topicFunc, s Stream, wg *sync.WaitGroup) {
	defer wg.Done()

	// 如果订阅了 topic，必须匹配当前topic才发送
	if topic != nil && !topic(s) {
		return
	}

	// 设置时间抖动，防止同一时间所有go并发请求导致带宽压力大,出现请求timeout
	<-time.After(time.Duration(b.setJitter()) * time.Millisecond)

	select {
	case sub <- s:
	case <-time.After(b.timeout):
	}
}

func (b *Bus) setJitter() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(jitter)
}

// 关闭发布者对象，同时关闭所有订阅者管道
func (b *Bus) Close() {
	b.m.Lock()
	defer b.m.Unlock()

	for sub := range b.subscribers {
		// 1. 在map中删除订阅者
		// 2. 关闭chan
		delete(b.subscribers, sub)
		close(sub)
	}
}
