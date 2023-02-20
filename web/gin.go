package web

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"watcher4metrics/config"
	apiv1 "watcher4metrics/web/api/v1"
)

var server *http.Server

func StartGin(r *gin.Engine) error {
	apiv1.Routers(r)
	conf := config.Get().Http
	timeout := conf.Timeout
	//fmt.Println(conf.Timeout)

	// 使用 http包一下 gin， 获取err
	s := &http.Server{
		Addr:    conf.Listen,
		Handler: r,
		// 注意，这里默认为ms
		// 由于对5m进行了s的转义，所以这里不需要指定time.second
		ReadTimeout:    time.Duration(timeout),
		WriteTimeout:   time.Duration(timeout),
		MaxHeaderBytes: 1 << 20,
	}
	server = s
	return s.ListenAndServe()
}

func Close() {
	server.Shutdown(context.TODO())
}
