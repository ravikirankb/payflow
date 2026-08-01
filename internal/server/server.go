package server

import (
	"context"
	"net/http"
)

type Server struct {
	server *http.Server
}

func New(port string) *Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
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
