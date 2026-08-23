// vertex-trilight (formerly scoxtrilight) — store-tier Vertex service.
//
// Implements business rules for maintaining correct trilight indicator state on the lane, reacting to lane.state.transitioned events.
//
// This is a reference scaffold: it wires up the standard Vertex service
// shape (event bus connection, health endpoint, graceful shutdown) so it
// runs and is deployable today, with its domain-specific logic left as a
// clearly marked extension point. Fill in handleEvents / the HTTP routes
// following the same pattern as cmd/vertex-core, cmd/vertex-intervention,
// or cmd/vertex-weight, which are fully implemented.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/udaykishore-resu/vertex-sco-platform/internal/config"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/eventbus"
	"github.com/udaykishore-resu/vertex-sco-platform/internal/httpx"
)

const serviceName = "vertex-trilight"

func main() {
	storeID := config.Env("VERTEX_STORE_ID", "store-demo")
	brokerAddr := config.Env("VERTEX_BROKER_ADDR", "")
	httpAddr := config.Env("VERTEX_HTTP_ADDR", ":8103")

	var bus eventbus.Bus
	if brokerAddr != "" {
		bus = eventbus.NewMQTT(brokerAddr, serviceName+"-"+storeID)
	} else {
		bus = eventbus.NewMemory()
		log.Printf("[%s] VERTEX_BROKER_ADDR unset, using in-memory bus (dev mode)", serviceName)
	}
	defer bus.Close()

	// TODO: subscribe to the events this service should react to, e.g.:
	// bus.Subscribe(eventbus.Topic(storeID, "+", domain.EventXxx), handleXxx)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", httpx.HealthHandler(serviceName, nil))
	// TODO: add domain-specific REST routes here.

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
