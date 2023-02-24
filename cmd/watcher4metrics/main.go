package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"watcher4metrics/config"
	ebus "watcher4metrics/pkg/bus"
	"watcher4metrics/pkg/common"
	_ "watcher4metrics/pkg/metric"
	"watcher4metrics/pkg/notify"
	"watcher4metrics/pkg/provider"
	"watcher4metrics/pkg/remote"
	"watcher4metrics/pkg/version"
	"watcher4metrics/web"
	apiv1 "watcher4metrics/web/api/v1"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	rotatelogs "github.com/lestrrat/go-file-rotatelogs"
	"github.com/oklog/run"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	ginp "github.com/zsais/go-gin-prometheus"
)

var (
	confFile string
	h        bool
	v        bool
)

type reloader struct {
	name     string
	reloader func() error
}

func initLog() {
	var (
		// 日志级别
		logLevel = logrus.DebugLevel
		// 日志格式
		format = &logrus.TextFormatter{TimestampFormat: "2006-01-02 15:04:05"}
		// 日志env标识
		env = common.GetDefaultEnv("WATCHER4METRICS_ENV", "test")

		// 日志文件根目录
		logDir  = "/logs/"
		logName = "watcher4metrics"
		logPath = path.Join(logDir, logName+".log")
	)

	if env == "prod" {
		// prod 环境提高日志等级
		logLevel = logrus.WarnLevel

		// 持久化日志
		if _, err := os.Stat(logDir); err != nil && os.IsNotExist(err) {
			if err := os.Mkdir(logDir, 0666); err != nil {
				logrus.WithFields(logrus.Fields{
					"err": err,
				}).Fatalln("can not mkdir logs")
			}
		}

		// 创建log文件
		file, err := os.OpenFile(
			logPath,
			os.O_CREATE|os.O_WRONLY|os.O_APPEND,
			0666,
		)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"err": err,
			}).Fatalln("can not find logs dir or file")
		}
		writers := []io.Writer{file, os.Stdout}
		multiWriters := io.MultiWriter(writers...)

		// 同时记录gin的日志
		gin.DefaultWriter = multiWriters
		// 同时写文件和屏幕
		logrus.SetOutput(multiWriters)

		// 配置log分割
		logf, err := rotatelogs.New(
			// 切割的日志名称
			path.Join(logDir, logName)+"-%Y%m%d.log",
			// 日志软链
			rotatelogs.WithLinkName(logPath),
			// 日志最大保存时长
			rotatelogs.WithMaxAge(time.Duration(7*24)*time.Hour),
			// 日志切割时长
			rotatelogs.WithRotationTime(time.Duration(24)*time.Hour),
		)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"err": err,
			}).Fatalln("can not new rotatelog")
		}

		// 添加logrus钩子
		hook := lfshook.NewHook(
			lfshook.WriterMap{
				logrus.InfoLevel:  logf,
				logrus.FatalLevel: logf,
				logrus.DebugLevel: logf,
				logrus.WarnLevel:  logf,
				logrus.ErrorLevel: logf,
				logrus.PanicLevel: logf,
			},
			format,
		)
		logrus.AddHook(hook)
	} else {
		logrus.SetOutput(os.Stdout)
	}

	// 设置日志记录级别
	logrus.SetLevel(logLevel)
	// 输出日志中添加文件名和方法信息
	logrus.SetReportCaller(true)
	// 设置日志格式
	logrus.SetFormatter(format)
}

func initArgs() {
	flag.StringVar(&confFile, "config", "./watcher4metrics.yml", "指定config path")
	flag.BoolVar(&v, "v", false, "版本信息")
	flag.BoolVar(&h, "h", false, "帮助信息")
	flag.Parse()

	if v {
		logrus.WithField("version", version.Version).Println("version")
		os.Exit(0)
	}

	if h {
		flag.Usage()
		os.Exit(0)
	}
}

func init() {
	initArgs()
	initLog()
}

func main() {
	// 解析配置
	if err := config.InitConfig(confFile); err != nil {
		logrus.Fatalf("InitConfig err: %v", err)
	}

	// event-bus
	// 初始化provider
	pvdMgr := provider.New(ebus.New(time.Duration(100)*time.Millisecond, 1024))

	reloaders := []reloader{
		{
			name:     "config",
			reloader: config.Reload,
		},
		{
			name:     "rpc",
			reloader: remote.Reload,
		},
	}

	var g run.Group

	ctxAll, cancelAll := context.WithCancel(context.Background())
	{
		// 信号管理
		term := make(chan os.Signal, 1)
		signal.Notify(term, os.Interrupt, syscall.SIGTERM)
		cancel := make(chan struct{})

		g.Add(func() error {
			select {
			case <-term:
				logrus.Warnln("优雅关闭ing...")
				cancelAll()
				return nil
			case <-cancel:
				return nil
			}
		}, func(err error) {
			ebus.Get().Close()
			close(cancel)
		})
	}

	{
		// 启动异步任务
		g.Add(func() error {
			pvdMgr.StartProviders(ctxAll)
			return nil
		}, func(e error) {
			logrus.Warnln("async.Start over")
			cancelAll()
		})
	}

	{
		//n9e client
		g.Add(func() error {
			remote.NewRemoteWritesClient(ctxAll)
			return nil
		}, func(e error) {
			cancelAll()
		})
	}

	{
		// config auto reload
		conf := config.Get()
		// 当仅有 AutoReload 开启的情况下再启用fsnotify
		if conf.Global.AutoReload && !conf.Http.LifeCycle {
			g.Add(func() error {
				return notify.WatchConfigChange(ctxAll)
			}, func(e error) {
				logrus.Warnln("file notify over")
				cancelAll()
			})
		}
	}

	{
		// reload
		g.Add(func() error {
			for {
				select {
				case ch := <-apiv1.GetReloadChan():
					ch <- reloadConfig(reloaders)
				case <-ctxAll.Done():
					return nil
				}
			}
		}, func(e error) {
			logrus.Warnln("reload manager over")
			cancelAll()
		})
	}

	{
		// http
		g.Add(func() error {
			gin.SetMode(gin.ReleaseMode)
			r := gin.Default()

			// 性能分析
			pprof.Register(r)
			// 加载metrics
			p := ginp.NewPrometheus("watcher4metrics")
			// 防止指标高基数
			p.ReqCntURLLabelMappingFn = func(c *gin.Context) string {
				url := c.Request.URL.String()
				if strings.HasPrefix(url, "/api/v1/") {
					return url
				}
				return "/:illegal"
			}
			p.Use(r)

			errCh := make(chan error)
			go func() {
				errCh <- web.StartGin(r)
			}()

			select {
			case err := <-errCh:
				logrus.Errorln("startGin failed ", err)
				return err
			case <-ctxAll.Done():
				return nil
			}
		}, func(e error) {
			logrus.Warnln("http web over")
			web.Close()
			cancelAll()
		})
	}

	g.Run()
}

func reloadConfig(reloaders []reloader) error {
	logrus.Warnln("reloaders start reload")

	failed := false
	for _, reloader := range reloaders {
		if err := reloader.reloader(); err != nil {
			failed = true

			logrus.WithFields(logrus.Fields{
				"status":   "failed",
				"reloader": reloader.name,
			}).Errorln("reloader failed")
		}
	}

	if failed {
		return errors.New("one or more reloader reload failed")
	}
	return nil
}
