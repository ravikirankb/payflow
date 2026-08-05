package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
)

type contextKey string

const requestIDKey contextKey = "request_id"

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := generateRequestID()

		// Add request ID to response header
		w.Header().Set("X-Request-ID", requestID)

		// Store request ID in the request context
		ctx := context.WithValue(r.Context(), requestIDKey, requestID)

		// Create a new request carrying the updated context
		r = r.WithContext(ctx)

		// Continue down the middleware chain
		next.ServeHTTP(w, r)
	})
}

func generateRequestID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b)
}
