// vertex-core (formerly scoxcoreservice) is the lane state manager. It
// hosts one statemachine.Machine per active lane and is the hub that
// dependent services react to via published events — it no longer makes
// synchronous inline calls into every dependent service to progress a
// transaction (see internal/statemachine's package doc for why).
//
// Every call to a downstream dependency (used here just for the
// intervention-creation side effect of a WEIGHING -> INTERVENTION move) goes
// through a resilience.Guard: if the dependency's circuit is open or the
// bulkhead is full, the lane transitions to DEGRADED instead of hanging —
// directly fixing "scoxcoreservice is a soft single point of failure".
package main

import (
	"context"
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
	"github.com/udaykishore-resu/vertex-sco-platform/internal/outbox"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/resilience"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/statemachine"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/tracing"
)

const serviceName = "vertex-core"

func main() {
	storeID := config.Env("VERTEX_STORE_ID", "store-demo")
	brokerAddr := config.Env("VERTEX_BROKER_ADDR", "")
	httpAddr := config.Env("VERTEX_HTTP_ADDR", ":8081")
	outboxPath := config.Env("VERTEX_OUTBOX_PATH", "/tmp/vertex-core-outbox.log")

	var bus eventbus.Bus
	if brokerAddr != "" {
		bus = eventbus.NewMQTT(brokerAddr, serviceName+"-"+storeID)
	} else {
		bus = eventbus.NewMemory() // single-binary/dev mode
		log.Printf("[%s] VERTEX_BROKER_ADDR unset, using in-memory bus (dev mode)", serviceName)
	}
	defer bus.Close()

	ob, err := outbox.Open(outboxPath)
	if err != nil {
		log.Fatalf("[%s] opening outbox: %v", serviceName, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go ob.ReplayLoop(ctx, bus, 3*time.Second)

	registry := resilience.NewRegistry()
	interventionGuard := registry.Get("vertex-intervention", resilience.DefaultConfig(), 20)

	lanes := newLaneManager(storeID, bus, ob, interventionGuard)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", httpx.HealthHandler(serviceName, func() map[string]any {
		pending, _ := ob.PendingCount()
		return map[string]any{
			"circuit_breakers": registry.Snapshot(),
			"outbox_pending":   pending,
			"active_lanes":     lanes.count(),
		}
	}))
	mux.HandleFunc("/lanes/", lanes.handleLane) // GET status, POST transition

	srv := &http.Server{Addr: httpAddr, Handler: mux}
	go func() {
		log.Printf("[%s] listening on %s (store=%s)", serviceName, httpAddr, storeID)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s] http server: %v", serviceName, err)
		}
	}()

	<-ctx.Done()
	log.Printf("[%s] shutting down", serviceName)
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}

// laneManager owns one statemachine.Machine per lane ID and wires its
// transitions to: (a) a trace span, (b) an outbox-durable published event,
// (c) for WEIGHING->INTERVENTION specifically, a guarded call that creates
// the intervention record (falling back to DEGRADED on guard failure).
type laneManager struct {
	storeID string
	bus     eventbus.Bus
	ob      *outbox.Outbox
	guard   *resilience.Guard

	mu    sync.Mutex
	lanes map[string]*statemachine.Machine
}

func newLaneManager(storeID string, bus eventbus.Bus, ob *outbox.Outbox, guard *resilience.Guard) *laneManager {
	return &laneManager{storeID: storeID, bus: bus, ob: ob, guard: guard, lanes: make(map[string]*statemachine.Machine)}
}

func (lm *laneManager) count() int {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return len(lm.lanes)
}

func (lm *laneManager) get(laneID string) *statemachine.Machine {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	m, ok := lm.lanes[laneID]
	if !ok {
		m = statemachine.New(laneID)
		m.OnTransition(lm.onTransition)
		lm.lanes[laneID] = m
	}
	return m
}

func (lm *laneManager) onTransition(laneID string, from, to domain.LaneState, reason string) {
	ctx, span := tracing.StartRoot(context.Background(), serviceName, "lane.transition")
	defer span.End(nil)

	env := domain.Envelope{
		ID:         fmt.Sprintf("%s-%d", laneID, time.Now().UnixNano()),
		Type:       domain.EventLaneStateTransitioned,
		Source:     serviceName,
		LaneID:     laneID,
		StoreID:    lm.storeID,
		OccurredAt: time.Now(),
		Trace:      span.Inject(),
		Payload:    domain.LaneStateTransitioned{From: from, To: to, Reason: reason},
	}
	topic := eventbus.Topic(lm.storeID, laneID, domain.EventLaneStateTransitioned)

	// Publish via bus first; if that fails (bus down), durably enqueue so
	// the ReplayLoop delivers it once connectivity returns — this is the
	// offline contract from flaw #9, applied uniformly to every transition.
	if err := lm.bus.Publish(topic, env); err != nil {
		log.Printf("[%s] publish failed, enqueueing to outbox: %v", serviceName, err)
		_ = lm.ob.Enqueue(topic, env)
	}

	if to == domain.StateIntervention {
		lm.raiseIntervention(ctx, laneID, reason)
	}
}

func (lm *laneManager) raiseIntervention(ctx context.Context, laneID, reason string) {
	err := lm.guard.Call(ctx, 3*time.Second, func(ctx context.Context) error {
		env := domain.Envelope{
			ID:         fmt.Sprintf("intervention-%s-%d", laneID, time.Now().UnixNano()),
			Type:       domain.EventInterventionRequested,
			Source:     serviceName,
			LaneID:     laneID,
			StoreID:    lm.storeID,
			OccurredAt: time.Now(),
			Payload: domain.InterventionRequested{
				InterventionID: fmt.Sprintf("iv-%d", time.Now().UnixNano()),
				Kind:           "generic",
				Reason:         reason,
				RequiresRole:   "associate",
			},
		}
		topic := eventbus.Topic(lm.storeID, laneID, domain.EventInterventionRequested)
		return lm.bus.Publish(topic, env)
	})
	if err != nil {
		log.Printf("[%s] vertex-intervention unavailable (%v) — lane %s degrading", serviceName, err, laneID)
		lm.get(laneID).Degrade("intervention_service_unavailable")
	}
}

// handleLane: GET /lanes/{id} -> current state; POST /lanes/{id}?to=STATE&reason=... -> transition.
func (lm *laneManager) handleLane(w http.ResponseWriter, r *http.Request) {
	laneID := r.URL.Path[len("/lanes/"):]
	if laneID == "" {
		httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": "missing lane id"})
		return
	}
	m := lm.get(laneID)
	switch r.Method {
	case http.MethodGet:
		httpx.JSON(w, http.StatusOK, map[string]string{"lane_id": laneID, "state": string(m.State())})
	case http.MethodPost:
		to := domain.LaneState(r.URL.Query().Get("to"))
		reason := r.URL.Query().Get("reason")
		if err := m.Fire(to, reason); err != nil {
			httpx.JSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		httpx.JSON(w, http.StatusOK, map[string]string{"lane_id": laneID, "state": string(m.State())})
	default:
		httpx.JSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}
