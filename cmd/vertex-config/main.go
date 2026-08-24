// vertex-config serves versioned configuration to every other Vertex service/store, with
// explicit canary percentage and promote/rollback operations — this is the
// direct fix for "Config/deployment pushes look unversioned... no canary,
// staged rollout, or automatic rollback": every config change is a new
// immutable version, rolled out to a percentage of stores/lanes first, and
// only "promoted" to 100% (or rolled back) by an explicit call, which
// cmd/vertex-agent gates on observed health.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/udaykishore-resu/vertex-sco-platform/internal/config"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/domain"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/eventbus"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/httpx"
)

const serviceName = "vertex-config"

type configVersion struct {
	ServiceName string          `json:"service_name"`
	Version     int             `json:"version"`
	Data        json.RawMessage `json:"data"`
	Checksum    string          `json:"checksum"`
	CanaryPct   int             `json:"canary_pct"` // 0-100; 100 = fully promoted
	CreatedAt   time.Time       `json:"created_at"`
	PromotedAt  time.Time       `json:"promoted_at,omitempty"`
	RolledBack  bool            `json:"rolled_back"`
}

type configStore struct {
	mu       sync.Mutex
	versions map[string][]*configVersion // serviceName -> versions, index 0 = v1
}

func newConfigStore() *configStore {
	return &configStore{versions: make(map[string][]*configVersion)}
}

func (cs *configStore) publish(serviceName string, data json.RawMessage, canaryPct int) *configVersion {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	sum := sha256.Sum256(data)
	v := &configVersion{
		ServiceName: serviceName,
		Version:     len(cs.versions[serviceName]) + 1,
		Data:        data,
		Checksum:    hex.EncodeToString(sum[:]),
		CanaryPct:   canaryPct,
		CreatedAt:   time.Now(),
	}
	cs.versions[serviceName] = append(cs.versions[serviceName], v)
	return v
}

// active returns the version currently in effect for a given store/lane —
// canary versions apply to a deterministic subset of stores (hash-bucketed)
// so the same store consistently lands in or out of a canary wave.
func (cs *configStore) active(serviceName, storeID string) *configVersion {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	versions := cs.versions[serviceName]
	if len(versions) == 0 {
		return nil
	}
	latest := versions[len(versions)-1]
	if !latest.RolledBack && (latest.CanaryPct >= 100 || inCanaryBucket(storeID, latest.CanaryPct)) {
		return latest
	}
	// fall back to the last fully-promoted (non-canary, non-rolled-back) version
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].CanaryPct >= 100 && !versions[i].RolledBack {
			return versions[i]
		}
	}
	if len(versions) > 0 {
		return versions[0]
	}
	return nil
}

func inCanaryBucket(storeID string, pct int) bool {
	if pct <= 0 {
		return false
	}
	sum := sha256.Sum256([]byte(storeID))
	bucket := int(sum[0]) % 100
	return bucket < pct
}

func (cs *configStore) promote(serviceName string, version int) (*configVersion, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	v := cs.find(serviceName, version)
	if v == nil {
		return nil, fmt.Errorf("version not found")
	}
	v.CanaryPct = 100
	v.PromotedAt = time.Now()
	v.RolledBack = false
	return v, nil
}

func (cs *configStore) rollback(serviceName string, version int) (*configVersion, *configVersion, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	v := cs.find(serviceName, version)
	if v == nil {
		return nil, nil, fmt.Errorf("version not found")
	}
	v.RolledBack = true
	// find previous good (promoted, not rolled back) version to fall back to
	versions := cs.versions[serviceName]
	var prev *configVersion
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].Version != version && versions[i].CanaryPct >= 100 && !versions[i].RolledBack {
			prev = versions[i]
			break
		}
	}
	return v, prev, nil
}

func (cs *configStore) find(serviceName string, version int) *configVersion {
	for _, v := range cs.versions[serviceName] {
		if v.Version == version {
			return v
		}
	}
	return nil
}

func (cs *configStore) list(serviceName string) []*configVersion {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return append([]*configVersion(nil), cs.versions[serviceName]...)
}

func main() {
	brokerAddr := config.Env("VERTEX_BROKER_ADDR", "")
	httpAddr := config.Env("VERTEX_HTTP_ADDR", ":8090")

	var bus eventbus.Bus
	if brokerAddr != "" {
		bus = eventbus.NewMQTT(brokerAddr, serviceName)
	} else {
		bus = eventbus.NewMemory()
		log.Printf("[%s] VERTEX_BROKER_ADDR unset, using in-memory bus (dev mode)", serviceName)
	}
	defer bus.Close()

	cs := newConfigStore()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", httpx.HealthHandler(serviceName, nil))

	// POST /configs/{service}?canary_pct=25   body: raw JSON config
	// GET  /configs/{service}/versions
	// GET  /configs/{service}/active?store_id=store-42
	// POST /configs/{service}/promote?version=3
	// POST /configs/{service}/rollback?version=3
	mux.HandleFunc("/configs/", func(w http.ResponseWriter, r *http.Request) {
		routeConfigs(w, r, cs, bus)
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Addr: httpAddr, Handler: mux}
	go func() {
		log.Printf("[%s] listening on %s", serviceName, httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s] http: %v", serviceName, err)
		}
	}()
	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}

func routeConfigs(w http.ResponseWriter, r *http.Request, cs *configStore, bus eventbus.Bus) {
	path := strings.TrimPrefix(r.URL.Path, "/configs/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": "missing service name"})
		return
	}
	svc := parts[0]

	switch {
	case len(parts) == 1 && r.Method == http.MethodPost:
		canaryPct := 100
		if v := r.URL.Query().Get("canary_pct"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				canaryPct = n
			}
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if len(body) == 0 {
			body = []byte(`{}`)
		}
		v := cs.publish(svc, body, canaryPct)
		env := domain.Envelope{
			ID: fmt.Sprintf("cfg-%s-%d-%d", svc, v.Version, time.Now().UnixNano()), Type: domain.EventConfigPublished,
			Source: serviceName, OccurredAt: time.Now(),
			Payload: domain.ConfigPublished{ServiceName: svc, Version: v.Version, Checksum: v.Checksum, CanaryPct: v.CanaryPct},
		}
		_ = bus.Publish("vertex/_/_/"+string(domain.EventConfigPublished), env)
		httpx.JSON(w, http.StatusCreated, v)

	case len(parts) == 2 && parts[1] == "versions" && r.Method == http.MethodGet:
		httpx.JSON(w, http.StatusOK, cs.list(svc))

	case len(parts) == 2 && parts[1] == "active" && r.Method == http.MethodGet:
		storeID := r.URL.Query().Get("store_id")
		v := cs.active(svc, storeID)
		if v == nil {
			httpx.JSON(w, http.StatusNotFound, map[string]string{"error": "no config published"})
			return
		}
		httpx.JSON(w, http.StatusOK, v)

	case len(parts) == 2 && parts[1] == "promote" && r.Method == http.MethodPost:
		version, _ := strconv.Atoi(r.URL.Query().Get("version"))
		v, err := cs.promote(svc, version)
		if err != nil {
			httpx.JSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		env := domain.Envelope{
			ID: fmt.Sprintf("promote-%s-%d-%d", svc, version, time.Now().UnixNano()), Type: domain.EventConfigPromoted,
			Source: serviceName, OccurredAt: time.Now(),
			Payload: domain.ConfigPromoted{ServiceName: svc, Version: version},
		}
		_ = bus.Publish("vertex/_/_/"+string(domain.EventConfigPromoted), env)
		httpx.JSON(w, http.StatusOK, v)

	case len(parts) == 2 && parts[1] == "rollback" && r.Method == http.MethodPost:
		version, _ := strconv.Atoi(r.URL.Query().Get("version"))
		bad, prev, err := cs.rollback(svc, version)
		if err != nil {
			httpx.JSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		toVersion := 0
		if prev != nil {
			toVersion = prev.Version
		}
		env := domain.Envelope{
			ID: fmt.Sprintf("rollback-%s-%d-%d", svc, version, time.Now().UnixNano()), Type: domain.EventConfigRolledBack,
			Source: serviceName, OccurredAt: time.Now(),
			Payload: domain.ConfigRolledBack{ServiceName: svc, FromVersion: version, ToVersion: toVersion, Reason: r.URL.Query().Get("reason")},
		}
		_ = bus.Publish("vertex/_/_/"+string(domain.EventConfigRolledBack), env)
		httpx.JSON(w, http.StatusOK, map[string]any{"rolled_back": bad, "active_now": prev})

	default:
		httpx.JSON(w, http.StatusNotFound, map[string]string{"error": "unknown route"})
	}
}
