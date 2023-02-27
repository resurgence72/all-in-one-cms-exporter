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
		"ali":      ali.New(newSub("ali")),
		"tc":       tc.New(newSub("tc")),
		"megaport": megaport.New(newSub("megaport")),
		"google":   google.New(newSub("google")),
		//"aws":  aws.New(newSub("aws")),
	}}
}

func (m *Manager) StartProviders(ctx context.Context) {
	for _, provider := range m.providerMap {
		go provider.Run(ctx)
	}
	<-ctx.Done()
}
