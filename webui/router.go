package webui

import (
	"github.com/gocraft/web"
	"github.com/gojek/work"
)

type RouterOptions struct {
	PathPrefix string
}

func NewRouter(client *work.Client, opts RouterOptions) *web.Router {
	ctx := context{client: client}
	router := web.New(ctx)

	router.Middleware(func(rw web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
		rw.Header().Set("Content-Type", "application/json; charset=utf-8")
		next(rw, r)
	})

	subRouter := router.Subrouter(ctx, opts.PathPrefix)
	subRouter.Get("/ping", ctx.ping)
	subRouter.Get("/queues", ctx.queues)
	subRouter.Get("/worker_pools", ctx.workerPools)
	subRouter.Get("/busy_workers", ctx.busyWorkers)
	subRouter.Get("/retry_jobs", ctx.retryJobs)
	subRouter.Get("/scheduled_jobs", ctx.scheduledJobs)
	subRouter.Get("/dead_jobs", ctx.deadJobs)
	subRouter.Post("/delete_dead_job/:died_at:\\d.*/:job_id", ctx.deleteDeadJob)
	subRouter.Post("/retry_dead_job/:died_at:\\d.*/:job_id", ctx.retryDeadJob)
	subRouter.Post("/delete_all_dead_jobs", ctx.deleteAllDeadJobs)
	subRouter.Post("/retry_all_dead_jobs", ctx.retryAllDeadJobs)

	//
	// Build the HTML page:
	//
	assetRouter := subRouter.Subrouter(ctx, "")
	assetRouter.Get("/", func(c *context, rw web.ResponseWriter, req *web.Request) {
		rw.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = rw.Write(mustAsset("index.html"))
	})
	assetRouter.Get("/work.js", func(c *context, rw web.ResponseWriter, req *web.Request) {
		rw.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		_, _ = rw.Write(mustAsset("work.js"))
	})

	return router
}
