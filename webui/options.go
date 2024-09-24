package webui

type opts func(*Server) *Server

func WithPrefix(prefix string) opts {
	return func(server *Server) *Server {
		server.pathPrefix = prefix

		return server
	}
}
