// vertex-control-plane (formerly NCR Edge UI & Control Plane, cloud side)
// aggregates fleet-wide deployment health reported by every store's
// vertex-agent and exposes a REST API for the frontend dashboard
// (frontend/). It also proxies canary rollout actions to vertex-config so
// the dashboard has one API to talk to.
package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/udaykishore-resu/vertex-sco-platform/internal/config"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/domain"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/httpx"
)

const serviceName = "vertex-control-plane"

type fleetState struct {
	mu     sync.Mutex
	byKey  map[string]domain.DeploymentHealthReport // key = storeID/serviceName
	seenAt map[string]time.Time
}

func newFleetState() *fleetState {
	return &fleetState{byKey: make(map[string]domain.DeploymentHealthReport), seenAt: make(map[string]time.Time)}
}

func (f *fleetState) record(r domain.DeploymentHealthReport) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := r.StoreID + "/" + r.ServiceName
	f.byKey[key] = r
	f.seenAt[key] = time.Now()
}

func (f *fleetState) snapshot() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]map[string]any, 0, len(f.byKey))
	for key, r := range f.byKey {
		out = append(out, map[string]any{
			"key":            key,
			"store_id":       r.StoreID,
			"service_name":   r.ServiceName,
			"version":        r.Version,
			"error_rate_pct": r.ErrorRatePct,
			"healthy":        r.Healthy,
			"last_seen":      f.seenAt[key],
		})
	}
	return out
}

func main() {
	httpAddr := config.Env("VERTEX_HTTP_ADDR", ":8100")
	configAddr := config.Env("VERTEX_CONFIG_ADDR", "http://localhost:8090")

	fs := newFleetState()
	mux := http.NewServeMux()

	mux.HandleFunc("/health", httpx.HealthHandler(serviceName, func() map[string]any {
		return map[string]any{"tracked_reports": len(fs.byKey)}
	}))

	mux.HandleFunc("/fleet/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			httpx.JSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
			return
		}
		var report domain.DeploymentHealthReport
		if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
			httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		fs.record(report)
		httpx.JSON(w, http.StatusAccepted, map[string]string{"status": "recorded"})
	})

	mux.HandleFunc("/fleet", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*") // demo dashboard convenience
		httpx.JSON(w, http.StatusOK, fs.snapshot())
	})

	// Thin proxy so the dashboard only needs to know about the control
	// plane, not vertex-config directly (keeps the store-facing config API
	// off the public/cloud-facing surface).
	mux.HandleFunc("/configs/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		proxy(w, r, configAddr)
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Addr: httpAddr, Handler: mux}
	go func() {
		log.Printf("[%s] listening on %s (proxying config to %s)", serviceName, httpAddr, configAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s] http: %v", serviceName, err)
		}
	}()
	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}

func proxy(w http.ResponseWriter, r *http.Request, targetBase string) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest(r.Method, targetBase+r.URL.Path+"?"+r.URL.RawQuery, r.Body)
	if err != nil {
		httpx.JSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		httpx.JSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
