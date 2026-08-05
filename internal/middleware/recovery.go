package middleware

import (
	"log/slog"
	"net/http"
)

func Recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				requestID := w.Header().Get("X-Request-ID")

				slog.Error("panic recovered",
					"request_id", requestID,
					"method", r.Method,
					"path", r.URL.Path,
					"panic", rec,
				)

				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()

		next.ServeHTTP(w, r)
	})
}
