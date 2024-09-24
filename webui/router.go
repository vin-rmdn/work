package webui

import (
	"html/template"

	"github.com/gocraft/web"
)

func (s *Server) setupRouter() {
	s.router.Middleware(func(c *context, rw web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
		c.Server = s
		next(rw, r)
	})
	s.router.Middleware(func(rw web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
		rw.Header().Set("Content-Type", "application/json; charset=utf-8")
		next(rw, r)
	})

	subRouter := s.router.Subrouter(context{}, s.pathPrefix)

	subRouter.Get("/ping", (*context).ping)
	subRouter.Get("/queues", (*context).queues)
	subRouter.Get("/worker_pools", (*context).workerPools)
	subRouter.Get("/busy_workers", (*context).busyWorkers)
	subRouter.Get("/retry_jobs", (*context).retryJobs)
	subRouter.Get("/scheduled_jobs", (*context).scheduledJobs)
	subRouter.Get("/dead_jobs", (*context).deadJobs)
	subRouter.Post("/delete_dead_job/:died_at:\\d.*/:job_id", (*context).deleteDeadJob)
	subRouter.Post("/retry_dead_job/:died_at:\\d.*/:job_id", (*context).retryDeadJob)
	subRouter.Post("/delete_all_dead_jobs", (*context).deleteAllDeadJobs)
	subRouter.Post("/retry_all_dead_jobs", (*context).retryAllDeadJobs)

	//
	// Build the HTML page:
	//
	assetRouter := subRouter.Subrouter(context{}, "")
	assetRouter.Get("/", func(c *context, rw web.ResponseWriter, req *web.Request) {
		rw.Header().Set("Content-Type", "text/html; charset=utf-8")

		indexTemplate, err := template.New("index.html").
			Delims("{{", "}}").
			Parse(string(mustAsset("index.html")))
		if err != nil {
			renderError(rw, err)

			return
		}

		data := struct {
			PathPrefix string
		}{
			PathPrefix: c.pathPrefix,
		}

		err = indexTemplate.Execute(rw, data)
		if err != nil {
			renderError(rw, err)

			return
		}
	})
	assetRouter.Get("/work.js", func(c *context, rw web.ResponseWriter, req *web.Request) {
		rw.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		_, _ = rw.Write(mustAsset("work.js"))
	})
}
