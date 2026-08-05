package server

import (
	"context"
	"net/http"

	"github.com/ravikirankb/payflow/internal/middleware"
)

type Server struct {
	server *http.Server
}

func New(port string) *Server {
	mux := http.NewServeMux()

	handler := middleware.Recovery(
		middleware.RequestID(
			middleware.Logging(http.HandlerFunc(healthHandler)),
		),
	)

	mux.Handle("/health", handler)

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	return &Server{
		server: srv,
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	// w.WriteHeader(http.StatusOK)
	// _, _ = w.Write([]byte("OK"))
	panic("boom")
}

func (s *Server) Start() error {
	return s.server.ListenAndServe()
}

func (s *Server) ShutDown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
