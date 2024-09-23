package webui

import "github.com/gocraft/web"

func setupRoutes(router *web.Router) *web.Router {
	router.Middleware(func(rw web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
		rw.Header().Set("Content-Type", "application/json; charset=utf-8")
		next(rw, r)
	})

	router.Get("/ping", (*context).ping)
	router.Get("/queues", (*context).queues)
	router.Get("/worker_pools", (*context).workerPools)
	router.Get("/busy_workers", (*context).busyWorkers)
	router.Get("/retry_jobs", (*context).retryJobs)
	router.Get("/scheduled_jobs", (*context).scheduledJobs)
	router.Get("/dead_jobs", (*context).deadJobs)
	router.Post("/delete_dead_job/:died_at:\\d.*/:job_id", (*context).deleteDeadJob)
	router.Post("/retry_dead_job/:died_at:\\d.*/:job_id", (*context).retryDeadJob)
	router.Post("/delete_all_dead_jobs", (*context).deleteAllDeadJobs)
	router.Post("/retry_all_dead_jobs", (*context).retryAllDeadJobs)

	//
	// Build the HTML page:
	//
	assetRouter := router.Subrouter(context{}, "")
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
