package aws

import (
	"context"

	"watcher4metrics/config"
)

const providerName = "aws"

type AWS struct {
	iden string
}

func New() *AWS {
	return new(AWS)
}

func (a *AWS) InitProducer() error {
	conf := config.Get().Collector.AWS
	a.iden = conf.Iden
	return nil
}

func (a *AWS) Run(ctx context.Context, errCh chan error) {

}

func (a *AWS) Reload() error {
	return nil
}
