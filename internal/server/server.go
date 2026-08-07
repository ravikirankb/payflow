package server

import (
	"context"
	"net/http"
)

type Server struct {
	server *http.Server
}

func New(port string, handler http.Handler) *Server {

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: handler,
	}

	return &Server{
		server: srv,
	}
}

func (s *Server) Start() error {
	return s.server.ListenAndServe()
}

func (s *Server) ShutDown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
