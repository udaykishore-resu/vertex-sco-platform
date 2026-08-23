// vertex-intervention (formerly scoxintervention) manages the intervention
// lifecycle from creation to resolution. It is decoupled from vertex-core:
// it subscribes to intervention.requested events on the bus rather than
// being called synchronously, and publishes intervention.resolved when an
// associate/security delegate resolves one via its REST API.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/udaykishore-resu/vertex-sco-platform/internal/config"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/domain"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/eventbus"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/httpx"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/tracing"
)

const serviceName = "vertex-intervention"

type interventionRecord struct {
	domain.InterventionRequested
	LaneID     string    `json:"lane_id"`
	StoreID    string    `json:"store_id"`
	Status     string    `json:"status"` // open | resolved
	CreatedAt  time.Time `json:"created_at"`
	ResolvedAt time.Time `json:"resolved_at,omitempty"`
	ResolvedBy string    `json:"resolved_by,omitempty"`
	Outcome    string    `json:"outcome,omitempty"`
}

type store struct {
	mu   sync.Mutex
	byID map[string]*interventionRecord
}

func newStore() *store { return &store{byID: make(map[string]*interventionRecord)} }

func main() {
	storeID := config.Env("VERTEX_STORE_ID", "store-demo")
	brokerAddr := config.Env("VERTEX_BROKER_ADDR", "")
	httpAddr := config.Env("VERTEX_HTTP_ADDR", ":8082")

	var bus eventbus.Bus
	if brokerAddr != "" {
		bus = eventbus.NewMQTT(brokerAddr, serviceName+"-"+storeID)
	} else {
		bus = eventbus.NewMemory()
		log.Printf("[%s] VERTEX_BROKER_ADDR unset, using in-memory bus (dev mode)", serviceName)
	}
	defer bus.Close()

	st := newStore()

	sub := eventbus.Topic(storeID, "+", domain.EventInterventionRequested)
	if err := bus.Subscribe(sub, func(env domain.Envelope) error {
		return handleRequested(st, env)
	}); err != nil {
		log.Fatalf("[%s] subscribe: %v", serviceName, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", httpx.HealthHandler(serviceName, func() map[string]any {
		st.mu.Lock()
		defer st.mu.Unlock()
		open := 0
		for _, r := range st.byID {
			if r.Status == "open" {
				open++
			}
		}
		return map[string]any{"open_interventions": open, "total": len(st.byID)}
	}))
	mux.HandleFunc("/interventions", func(w http.ResponseWriter, r *http.Request) {
		st.mu.Lock()
		defer st.mu.Unlock()
		list := make([]*interventionRecord, 0, len(st.byID))
		for _, v := range st.byID {
			list = append(list, v)
		}
		httpx.JSON(w, http.StatusOK, list)
	})
	mux.HandleFunc("/interventions/resolve", func(w http.ResponseWriter, r *http.Request) {
		resolveHandler(w, r, st, bus, storeID)
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Addr: httpAddr, Handler: mux}
	go func() {
		log.Printf("[%s] listening on %s (store=%s)", serviceName, httpAddr, storeID)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s] http: %v", serviceName, err)
		}
	}()
	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}

func handleRequested(st *store, env domain.Envelope) error {
	_, span := tracing.StartChild(context.Background(), serviceName, "intervention.create", env.Trace)
	defer span.End(nil)

	b, err := json.Marshal(env.Payload)
	if err != nil {
		return err
	}
	var p domain.InterventionRequested
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}
	rec := &interventionRecord{
		InterventionRequested: p,
		LaneID:                env.LaneID,
		StoreID:               env.StoreID,
		Status:                "open",
		CreatedAt:             time.Now(),
	}
	st.mu.Lock()
	st.byID[p.InterventionID] = rec
	st.mu.Unlock()
	log.Printf("[%s] opened intervention %s on lane %s (%s)", serviceName, p.InterventionID, env.LaneID, p.Reason)
	return nil
}

func resolveHandler(w http.ResponseWriter, r *http.Request, st *store, bus eventbus.Bus, storeID string) {
	if r.Method != http.MethodPost {
		httpx.JSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
		return
	}
	var req struct {
		InterventionID string `json:"intervention_id"`
		ResolvedBy     string `json:"resolved_by"`
		Outcome        string `json:"outcome"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	st.mu.Lock()
	rec, ok := st.byID[req.InterventionID]
	if ok {
		rec.Status = "resolved"
		rec.ResolvedAt = time.Now()
		rec.ResolvedBy = req.ResolvedBy
		rec.Outcome = req.Outcome
	}
	st.mu.Unlock()
	if !ok {
		httpx.JSON(w, http.StatusNotFound, map[string]string{"error": "unknown intervention_id"})
		return
	}

	env := domain.Envelope{
		ID:         fmt.Sprintf("resolved-%s-%d", req.InterventionID, time.Now().UnixNano()),
		Type:       domain.EventInterventionResolved,
		Source:     serviceName,
		LaneID:     rec.LaneID,
		StoreID:    storeID,
		OccurredAt: time.Now(),
		Payload: domain.InterventionResolved{
			InterventionID: req.InterventionID,
			ResolvedBy:     req.ResolvedBy,
			Outcome:        req.Outcome,
		},
	}
	_ = bus.Publish(eventbus.Topic(storeID, rec.LaneID, domain.EventInterventionResolved), env)
	httpx.JSON(w, http.StatusOK, rec)
}
