// Package httpx has tiny shared helpers so every service's main.go doesn't
// re-implement the same JSON response / health-check boilerplate.
package httpx

import (
	"encoding/json"
	"net/http"
)

func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// HealthHandler returns a standard liveness/readiness endpoint. extra is an
// optional callback to merge additional fields (circuit breaker states,
// outbox backlog size, etc.) into the response body.
func HealthHandler(service string, extra func() map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body := map[string]any{"service": service, "status": "ok"}
		if extra != nil {
			for k, v := range extra() {
				body[k] = v
			}
		}
		JSON(w, http.StatusOK, body)
	}
}
