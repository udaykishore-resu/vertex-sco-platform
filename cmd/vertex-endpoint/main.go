// vertex-endpoint (formerly scoxendpoint) — terminal-tier Vertex service, running as a
// DaemonSet on each physical SCO lane.
//
// Provides the lane endpoint UUID via the event bus and REST API; hosts lane-local business logic (EOD, reboot, shutdown).
//
// Reference scaffold — see cmd/vertex-core for the fully-implemented
// pattern this follows (event bus wiring, health endpoint, graceful
// shutdown). Terminal-tier services additionally are expected to load an
// mTLS identity via internal/identity (see deploy/certs) before dialing the
// store-tier broker, since terminals are the most physically exposed part
// of the fleet.
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

const serviceName = "vertex-endpoint"

func main() {
	laneID := config.Env("VERTEX_LANE_ID", "lane-demo")
	brokerAddr := config.Env("VERTEX_BROKER_ADDR", "")
	httpAddr := config.Env("VERTEX_HTTP_ADDR", ":9001")

	var bus eventbus.Bus
	if brokerAddr != "" {
		bus = eventbus.NewMQTT(brokerAddr, serviceName+"-"+laneID)
	} else {
		bus = eventbus.NewMemory()
		log.Printf("[%s] VERTEX_BROKER_ADDR unset, using in-memory bus (dev mode)", serviceName)
	}
	defer bus.Close()

	// TODO: JPOS/XML.POS device I/O and event publishing goes here for
	// vertex-devicegateway; UI-launch/system-function logic for
	// vertex-launchpad; lane registration/REST for vertex-endpoint.

	mux := http.NewServeMux()
	mux.HandleFunc("/health", httpx.HealthHandler(serviceName, nil))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Addr: httpAddr, Handler: mux}
	go func() {
		log.Printf("[%s] listening on %s (lane=%s)", serviceName, httpAddr, laneID)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s] http: %v", serviceName, err)
		}
	}()
	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}
