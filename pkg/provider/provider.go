package provider

import (
	"context"
	"strings"
	ebus "watcher4metrics/pkg/bus"
	"watcher4metrics/pkg/common"
	"watcher4metrics/pkg/provider/ali"
	"watcher4metrics/pkg/provider/google"
	"watcher4metrics/pkg/provider/megaport"
	"watcher4metrics/pkg/provider/tc"
)

type Manager struct {
	providerMap map[string]common.Provider
}

func New(bus *ebus.Bus) *Manager {
	newSub := func(key string) chan any {
		return bus.SubscriberTopic(func(s ebus.Stream) bool {
			if strings.EqualFold(s.Topic, key) {
				return true
			}
			return false
		})
	}

	return &Manager{providerMap: map[string]common.Provider{
		common.AlibabaCloudProvider:  ali.New(newSub(common.AlibabaCloudProvider)),
		common.TencentCloudProvider:  tc.New(newSub(common.TencentCloudProvider)),
		common.MegaPortCloudProvider: megaport.New(newSub(common.MegaPortCloudProvider)),
		common.GoogleCloudProvider:   google.New(newSub(common.GoogleCloudProvider)),
		//common.AWSCloudProvider:  aws.New(newSub(common.AWSCloudProvider)),
	}}
}

func (m *Manager) StartProviders(ctx context.Context) {
	for _, provider := range m.providerMap {
		go provider.Run(ctx)
	}
	<-ctx.Done()
}
