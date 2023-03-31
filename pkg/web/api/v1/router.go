package v1

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

const pathPrefix = "/api/v1"

func Routers(r *gin.Engine) {
	admin := r.Group("/-")
	{
		admin.GET("/ping", func(c *gin.Context) {
			c.String(http.StatusOK, "pong+ (｡A｡)")
		})

		admin.GET("/reload", reloadWatcher4Metrics)
	}

	api := r.Group(pathPrefix)
	{
		// ali job
		api.POST("/cron-ali", cronJobAli)
		// tc job
		api.POST("/cron-tc", cronJobTC)
		// aws job
		api.POST("/cron-aws", cronJobAWS)
		// google job
		api.POST("/cron-google", cronJobGoogle)

		// megaport job
		api.POST("/cron-megaport", cronJobMegaPort)
	}
}
