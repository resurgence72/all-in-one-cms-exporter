package aws

import (
	"context"
	"watcher4metrics/pkg/common"
)

const providerName = common.AWSCloudProvider

type AWS struct {
	iden string
}

func New() *AWS {
	return new(AWS)
}

func (a *AWS) InitProducer() error {
	return nil
}

func (a *AWS) Run(ctx context.Context, errCh chan error) {

}

func (a *AWS) Reload() error {
	return nil
}
