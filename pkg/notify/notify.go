package notify

import (
	"context"

	"watcher4metrics/pkg/config"
	"watcher4metrics/pkg/web/api/v1"

	"github.com/sirupsen/logrus"
	"gopkg.in/fsnotify.v1"
)

func WatchConfigChange(ctx context.Context) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		logrus.Error("fsnotify.newWatcher:", err)
		return err
	}
	defer w.Close()

	filePath := config.GetFileName()
	if err = w.Add(filePath); err != nil {
		logrus.Error("v1.add.errors: ", err)
		return err
	}

	logrus.WithFields(logrus.Fields{
		"path": filePath,
	}).Warnln("begin watch config notify")

	var notifyMsg string
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-w.Events:
			if !ok {
				continue
			}
			// 需要适配 VIM 操作
			// VIM 操作会产生 rename chmod remove ,同时inode会变，watch失效
			if event.Op&fsnotify.Create == fsnotify.Create {
				notifyMsg = "notify create file event"
			} else if event.Op&fsnotify.Write == fsnotify.Write {
				notifyMsg = "notify write file event"
			} else if event.Op&fsnotify.Remove == fsnotify.Remove {
				notifyMsg = "notify delete file event"
				// 最后一步删除后，需要重新 watch
				w.Remove(filePath)
				w.Add(filePath)
			}

			logrus.WithFields(logrus.Fields{
				"event": notifyMsg,
			}).Warnln("notify file change")

			// 通知去refresh config
			notifyRefresh()
		case err, ok := <-w.Errors:
			if !ok {
				continue
			}
			logrus.Error("v1.errors: ", err)
		}
	}
}

func notifyRefresh() {
	// 通知管道reload config
	ch := make(chan error)
	v1.GetReloadChan() <- ch

	if err := <-ch; err != nil {
		logrus.WithFields(logrus.Fields{"err": err}).Errorln("failed to reload config by v1")
	} else {
		logrus.Warnln("v1 reload config success")
	}
}
