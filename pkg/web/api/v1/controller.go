package v1

import (
	"net/http"

	ebus "watcher4metrics/pkg/bus"
	"watcher4metrics/pkg/config"
	"watcher4metrics/pkg/provider/ali"
	"watcher4metrics/pkg/provider/google"
	"watcher4metrics/pkg/provider/megaport"
	"watcher4metrics/pkg/provider/tc"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

var reloadCh = make(chan chan error)

func GetReloadChan() chan chan error {
	return reloadCh
}

func reloadWatcher4Metrics(c *gin.Context) {
	if !config.Get().Http.LifeCycle {
		c.JSON(http.StatusOK, gin.H{"msg": "unauthorized", "data": "lifeCycle disable, please open it on watcher4metrics.yml", "code": -1})
		return
	}
	ch := make(chan error)
	reloadCh <- ch

	if err := <-ch; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"msg": "failed", "data": "failed to reload config by http", "code": -1})
	} else {
		c.JSON(http.StatusOK, gin.H{"msg": "success", "data": "config reload success", "code": 0})
	}
}

// ali cron 执行接口
func cronJobAli(c *gin.Context) {
	req := &ali.AliReq{}
	if err := c.ShouldBindJSON(req); err != nil {
		c.JSON(http.StatusOK, gin.H{"msg": "failed", "data": err, "code": -1})
		return
	}

	// bus 通知 ali
	ebus.Get().Publish(ebus.Stream{
		Topic: "ali",
		Data:  req.Decode(),
	})

	logrus.WithFields(logrus.Fields{
		"provider": "ali",
		"iden":     req.Iden,
		"dur":      req.Dur,
	}).Warnln("cronJobAli pub stream")
	c.JSON(http.StatusOK, gin.H{"msg": "success", "data": "ali cron send success", "code": 0})
}

// tc cron 执行接口
func cronJobTC(c *gin.Context) {
	req := &tc.TCReq{}
	if err := c.ShouldBindJSON(req); err != nil {
		c.JSON(http.StatusOK, gin.H{"msg": "failed", "data": err, "code": -1})
		return
	}

	// bus 通知 tc
	ebus.Get().Publish(ebus.Stream{
		Topic: "tc",
		Data:  req.Decode(),
	})

	logrus.WithFields(logrus.Fields{
		"provider": "tc",
		"iden":     req.Iden,
		"dur":      req.Dur,
	}).Warnln("cronJobTC pub stream")

	c.JSON(http.StatusOK, gin.H{"msg": "success", "data": "tc cron send success", "code": 0})
}

// aws cron 执行接口
func cronJobAWS(c *gin.Context) {}

// google cron 执行接口
func cronJobGoogle(c *gin.Context) {
	req := &google.GoogleReq{}
	if err := c.ShouldBindJSON(req); err != nil {
		c.JSON(http.StatusOK, gin.H{"msg": "failed", "data": err, "code": -1})
		return
	}

	ebus.Get().Publish(ebus.Stream{
		Topic: "google",
		Data:  req.Decode(),
	})

	logrus.WithFields(logrus.Fields{
		"provider": "google",
		"iden":     req.Iden,
		"dur":      req.Dur,
	}).Warnln("cronJobGoogle pub stream")

	c.JSON(http.StatusOK, gin.H{"msg": "success", "data": "google cron send success", "code": 0})
}

// megaport cron 执行接口
func cronJobMegaPort(c *gin.Context) {
	req := &megaport.MPReq{}
	if err := c.ShouldBindJSON(req); err != nil {
		c.JSON(http.StatusOK, gin.H{"msg": "failed", "data": err, "code": -1})
		return
	}

	// bus 通知 megaport
	ebus.Get().Publish(ebus.Stream{
		Topic: "megaport",
		Data:  req.Decode(),
	})

	logrus.WithFields(logrus.Fields{
		"provider": "megaport",
		"iden":     req.Iden,
		"dur":      req.Dur,
	}).Warnln("cronJobMegaPort pub stream")

	c.JSON(http.StatusOK, gin.H{"msg": "success", "data": "megaport cron send success", "code": 0})
}
