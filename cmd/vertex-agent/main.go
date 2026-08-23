// vertex-agent (formerly Edge Agent) runs on the store server. It polls
// vertex-config for the active canary version of workloads it manages,
// "deploys" them (in this reference implementation, deployment is
// simulated — a real agent would apply Kubernetes manifests / call the
// container runtime), continuously evaluates health, and reports
// deployment.health_report events. If the observed error rate exceeds a
// threshold while on a canary version, it calls vertex-config's rollback
// endpoint itself — this is the "canary + rollback... automated
// health-check gating" fix from the architecture review, so a bad push's
// blast radius is capped at the canary percentage rather than the whole
// fleet.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/udaykishore-resu/vertex-sco-platform/internal/config"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/domain"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/eventbus"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/httpx"
)

const serviceName = "vertex-agent"

type deployedVersion struct {
	Version   int
	CanaryPct int
}

func main() {
	storeID := config.Env("VERTEX_STORE_ID", "store-demo")
	configAddr := config.Env("VERTEX_CONFIG_ADDR", "http://localhost:8090")
	controlPlaneAddr := config.Env("VERTEX_CONTROL_PLANE_ADDR", "")
	brokerAddr := config.Env("VERTEX_BROKER_ADDR", "")
	httpAddr := config.Env("VERTEX_HTTP_ADDR", ":8095")
	managed := config.Env("VERTEX_MANAGED_SERVICES", "vertex-core,vertex-intervention,vertex-weight")
	errorRateThresholdPct := 8.0 // above this while on a canary version -> auto-rollback

	var bus eventbus.Bus
	if brokerAddr != "" {
		bus = eventbus.NewMQTT(brokerAddr, serviceName+"-"+storeID)
	} else {
		bus = eventbus.NewMemory()
	}
	defer bus.Close()

	deployed := make(map[string]*deployedVersion)
	services := splitCSV(managed)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go reconcileLoop(ctx, configAddr, controlPlaneAddr, storeID, services, deployed, errorRateThresholdPct, bus)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", httpx.HealthHandler(serviceName, func() map[string]any {
		return map[string]any{"store_id": storeID, "deployed": deployed}
	}))
	srv := &http.Server{Addr: httpAddr, Handler: mux}
	go func() {
		log.Printf("[%s] listening on %s (store=%s, managing=%v)", serviceName, httpAddr, storeID, services)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s] http: %v", serviceName, err)
		}
	}()
	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}

func splitCSV(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func reconcileLoop(ctx context.Context, configAddr, controlPlaneAddr, storeID string, services []string, deployed map[string]*deployedVersion, threshold float64, bus eventbus.Bus) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	client := &http.Client{Timeout: 3 * time.Second}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, svc := range services {
				reconcileOne(client, configAddr, controlPlaneAddr, storeID, svc, deployed, threshold, bus)
			}
		}
	}
}

type activeConfigResp struct {
	ServiceName string `json:"service_name"`
	Version     int    `json:"version"`
	CanaryPct   int    `json:"canary_pct"`
	RolledBack  bool   `json:"rolled_back"`
}

func reconcileOne(client *http.Client, configAddr, controlPlaneAddr, storeID, svc string, deployed map[string]*deployedVersion, threshold float64, bus eventbus.Bus) {
	resp, err := client.Get(fmt.Sprintf("%s/configs/%s/active?store_id=%s", configAddr, svc, storeID))
	if err != nil {
		log.Printf("[%s] vertex-config unreachable for %s: %v (staying on last known-good deploy)", serviceName, svc, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}
	var active activeConfigResp
	if err := json.NewDecoder(resp.Body).Decode(&active); err != nil {
		return
	}

	cur, known := deployed[svc]
	if !known || cur.Version != active.Version {
		log.Printf("[%s] deploying %s version %d (canary_pct=%d)", serviceName, svc, active.Version, active.CanaryPct)
		deployed[svc] = &deployedVersion{Version: active.Version, CanaryPct: active.CanaryPct}
		cur = deployed[svc]
	}

	// Simulate a health signal. In production this reads real metrics
	// (error rate from the service's own /health or a Prometheus query);
	// here we synthesize a small baseline error rate, occasionally spiking
	// to demonstrate the rollback path.
	errorRate := simulateErrorRate(svc, active.Version)
	healthy := errorRate < threshold

	report := domain.DeploymentHealthReport{StoreID: storeID, ServiceName: svc, Version: active.Version, ErrorRatePct: errorRate, Healthy: healthy}
	env := domain.Envelope{ID: fmt.Sprintf("health-%s-%s-%d", storeID, svc, time.Now().UnixNano()), Type: domain.EventDeploymentHealthReport, Source: serviceName, StoreID: storeID, OccurredAt: time.Now(), Payload: report}
	_ = bus.Publish("vertex/_/_/"+string(domain.EventDeploymentHealthReport), env)
	if controlPlaneAddr != "" {
		b, _ := json.Marshal(report)
		_, _ = client.Post(controlPlaneAddr+"/fleet/health", "application/json", bytes.NewReader(b))
	}

	if !healthy && cur.CanaryPct < 100 && cur.CanaryPct > 0 {
		log.Printf("[%s] %s v%d error rate %.1f%% exceeds threshold %.1f%% on a canary rollout — triggering auto-rollback", serviceName, svc, active.Version, errorRate, threshold)
		_, _ = client.Post(fmt.Sprintf("%s/configs/%s/rollback?version=%d&reason=error_rate_%.1fpct", configAddr, svc, active.Version, errorRate), "application/json", nil)
	}
}

func simulateErrorRate(svc string, version int) float64 {
	// Deterministic-ish demo signal: version 2+ has a 15% chance per tick of
	// a simulated spike so the auto-rollback path is exercisable in the demo
	// without wiring real metrics.
	if version > 1 && rand.Intn(100) < 15 {
		return 12.5 + rand.Float64()*10
	}
	return rand.Float64() * 3
}
