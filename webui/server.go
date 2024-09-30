package webui

import (
	"net/http"
	"sync"

	"github.com/braintree/manners"
	"github.com/gocraft/web"
	"github.com/gojek/work"
	"github.com/gomodule/redigo/redis"
)

// Server implements an HTTP server which exposes a JSON API to view and manage gojek/work items.
type Server struct {
	namespace  string
	pool       *redis.Pool
	hostPort   string
	server     *manners.GracefulServer
	wg         sync.WaitGroup
	router     *web.Router
	pathPrefix string
}

// NewServer creates and returns a new server. The 'namespace' param is the redis namespace to use. The hostPort param is the address to bind on to expose the API.
func NewServer(namespace string, pool *redis.Pool, hostPort string) *Server {
	client := work.NewClient(namespace, pool)
	router := BuildRouter(client, RouterOptions{})

	return &Server{
		namespace: namespace,
		pool:      pool,
		hostPort:  hostPort,
		router:    router,
		server:    manners.NewWithServer(&http.Server{Addr: hostPort, Handler: router}),
	}
}

// Start starts the server listening for requests on the hostPort specified in NewServer.
func (w *Server) Start() {
	w.wg.Add(1)
	go func(w *Server) {
		_ = w.server.ListenAndServe()

		w.wg.Done()
	}(w)
}

// Stop stops the server and blocks until it has finished.
func (w *Server) Stop() {
	w.server.Close()
	w.wg.Wait()
}
