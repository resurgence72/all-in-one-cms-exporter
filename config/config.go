package config

import (
	"errors"
	"io/ioutil"
	"sync"

	"watcher4metrics/pkg/relabel"

	"github.com/prometheus/common/model"

	"gopkg.in/yaml.v3"
)

type Watcher4metricsConfig struct {
	Global *GlobalConfig `yaml:"global"`
	Redis  *RedisConfig  `yaml:"redis"`
	Report *ReportConfig `yaml:"report"`
	Http   *HTTPConfig   `yaml:"http"`
}

type ReportConfig struct {
	RemoteWrites []*RemoteWrite `yaml:"remote_write"`
	Batch        int            `yaml:"batch"`
}

type RemoteWrite struct {
	URL                 string            `yaml:"url"`
	Authorization       map[string]string `yaml:"authorization"`
	WriteRelabelConfigs []*relabel.Config `yaml:"write_relabel_configs"`
	RemoteTimeout       model.Duration    `yaml:"remote_timeout"`
}

type RedisConfig struct {
	RedisServer string `yaml:"redis_server"`
	Password    string `yaml:"password"`
	Prefix      string `yaml:"prefix"`
}

type GlobalConfig struct {
	AutoReload bool `yaml:"auto_reload"`
}

type HTTPConfig struct {
	Listen    string         `yaml:"listen"`
	Timeout   model.Duration `yaml:"timeout"`
	LifeCycle bool           `yaml:"life_cycle"`
}

type Auth struct {
	Key   string `yaml:"key"`
	Value string `yaml:"value"`
}

var (
	config   *Watcher4metricsConfig
	fileName string
	lock     sync.Mutex
)

func Reload() error {
	return InitConfig(fileName)
}

func InitConfig(filePath string) error {
	cfg, err := LoadFile(filePath)
	if err != nil {
		return err
	}
	fileName = filePath
	config = cfg
	return nil
}

func Get() *Watcher4metricsConfig {
	lock.Lock()
	defer lock.Unlock()
	return config
}

func GetFileName() string {
	return fileName
}

func LoadFile(fileName string) (*Watcher4metricsConfig, error) {
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	return Load(bytes)
}

func Load(bytes []byte) (*Watcher4metricsConfig, error) {
	cfg := &Watcher4metricsConfig{}
	err := yaml.Unmarshal(bytes, cfg)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func (h *HTTPConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	hc := &HTTPConfig{}
	type plain HTTPConfig

	if err := unmarshal((*plain)(hc)); err != nil {
		return err
	}

	if hc.Listen == "" {
		hc.Listen = ":8080"
	}

	*h = *hc
	return nil
}

func (g *GlobalConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	gc := &GlobalConfig{}
	type plain GlobalConfig

	if err := unmarshal((*plain)(gc)); err != nil {
		return err
	}

	*g = *gc
	return nil
}

func (r *ReportConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	rc := &ReportConfig{}
	type plain ReportConfig

	if err := unmarshal((*plain)(rc)); err != nil {
		return err
	}

	if rc.Batch <= 0 {
		rc.Batch = 1000
	}

	*r = *rc
	return nil
}

func (r *RemoteWrite) UnmarshalYAML(unmarshal func(interface{}) error) error {
	rw := &RemoteWrite{}
	type plain RemoteWrite

	if err := unmarshal((*plain)(rw)); err != nil {
		return err
	}

	if len(r.URL) == 0 {
		r.URL = "http://localhost:9090/api/v1/write"
	}

	if r.Authorization == nil {
		r.Authorization = make(map[string]string)
	}

	for _, rlcfg := range r.WriteRelabelConfigs {
		if rlcfg == nil {
			return errors.New("empty or null relabeling rule in remote write config")
		}
	}

	*r = *rw
	return nil
}
