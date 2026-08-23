// vertex-weight evaluates bag-scale weight events. On mismatch it publishes both a
// weight.mismatch_detected event (consumed by analytics/weight-learning
// pipelines) and an intervention.requested event directly — it does not
// call vertex-core synchronously; vertex-core only observes the resulting
// state change if/when it decides to move the lane to INTERVENTION.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/udaykishore-resu/vertex-sco-platform/internal/config"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/domain"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/eventbus"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/httpx"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/tracing"
)

const serviceName = "vertex-weight"

func main() {
	storeID := config.Env("VERTEX_STORE_ID", "store-demo")
	brokerAddr := config.Env("VERTEX_BROKER_ADDR", "")
	httpAddr := config.Env("VERTEX_HTTP_ADDR", ":8083")

	var bus eventbus.Bus
	if brokerAddr != "" {
		bus = eventbus.NewMQTT(brokerAddr, serviceName+"-"+storeID)
	} else {
		bus = eventbus.NewMemory()
		log.Printf("[%s] VERTEX_BROKER_ADDR unset, using in-memory bus (dev mode)", serviceName)
	}
	defer bus.Close()

	sub := eventbus.Topic(storeID, "+", domain.EventWeightObserved)
	mismatches := 0
	if err := bus.Subscribe(sub, func(env domain.Envelope) error {
		detected, err := handleObserved(bus, storeID, env)
		if detected {
			mismatches++
		}
		return err
	}); err != nil {
		log.Fatalf("[%s] subscribe: %v", serviceName, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", httpx.HealthHandler(serviceName, func() map[string]any {
		return map[string]any{"mismatches_detected": mismatches}
	}))

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

func handleObserved(bus eventbus.Bus, storeID string, env domain.Envelope) (bool, error) {
	_, span := tracing.StartChild(context.Background(), serviceName, "weight.evaluate", env.Trace)
	defer span.End(nil)

	b, err := json.Marshal(env.Payload)
	if err != nil {
		return false, err
	}
	var p domain.WeightObserved
	if err := json.Unmarshal(b, &p); err != nil {
		return false, err
	}

	delta := math.Abs(p.ObservedGrams - p.ExpectedGrams)
	tolerance := p.ExpectedGrams * (p.TolerancePct / 100.0)
	if delta <= tolerance {
		return false, nil // within tolerance, no action
	}

	log.Printf("[%s] weight mismatch on lane %s: expected=%.1fg observed=%.1fg delta=%.1fg",
		serviceName, env.LaneID, p.ExpectedGrams, p.ObservedGrams, delta)

	mismatchEnv := domain.Envelope{
		ID: fmt.Sprintf("mismatch-%s-%d", env.LaneID, time.Now().UnixNano()), Type: domain.EventWeightMismatchDetected,
		Source: serviceName, LaneID: env.LaneID, StoreID: storeID, OccurredAt: time.Now(),
		Payload: domain.WeightMismatchDetected{DeltaGrams: delta},
	}
	if err := bus.Publish(eventbus.Topic(storeID, env.LaneID, domain.EventWeightMismatchDetected), mismatchEnv); err != nil {
		return true, err
	}

	interventionEnv := domain.Envelope{
		ID: fmt.Sprintf("iv-weight-%s-%d", env.LaneID, time.Now().UnixNano()), Type: domain.EventInterventionRequested,
		Source: serviceName, LaneID: env.LaneID, StoreID: storeID, OccurredAt: time.Now(),
		Payload: domain.InterventionRequested{
			InterventionID: fmt.Sprintf("iv-weight-%d", time.Now().UnixNano()),
			Kind:           "weight_mismatch",
			Reason:         fmt.Sprintf("weight delta %.1fg exceeds tolerance", delta),
			RequiresRole:   "associate",
		},
	}
	return true, bus.Publish(eventbus.Topic(storeID, env.LaneID, domain.EventInterventionRequested), interventionEnv)
}
