package middleware

import (
	"log/slog"
	"net/http"
	"time"
)

func Logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 1. Record the start time
		start := time.Now()

		// 2. Call next.ServeHTTP(...)
		next.ServeHTTP(w, r)

		// 3. After the handler finishes, calculate duration
		duration := time.Since(start)

		requestID, ok := r.Context().Value(requestIDKey).(string)
		if !ok {
			requestID = "unknown"
		}

		// 4. Log the HTTP method, request path, and duration
		slog.Info("request completed",
			"request_id", requestID,
			"method", r.Method,
			"path", r.URL.Path,
			"duration_ms", duration.Milliseconds(),
		)
	})
}
