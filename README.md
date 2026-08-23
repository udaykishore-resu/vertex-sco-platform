# Vertex SCO Platform

A three-tier, event-driven self-checkout (SCO) edge platform — cloud (GCP/GKE-style),
store (Intelligent Edge Server), and terminal (SCO lane) — redesigned from the
Jarvis SCO-X reference architecture after an explicit architecture review.
Every identified flaw has a concrete, working fix in this codebase; see
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full flaw → fix
mapping and the complete service rename table, and
[`docs/RUNBOOK.md`](docs/RUNBOOK.md) for exact commands and observed
results from development.

## Highlights

- **Event-driven core**, not a synchronous call-graph: `vertex-core` (the
  lane state machine) publishes events; dependent services react. Every
  remaining direct call is wrapped in a circuit breaker + bulkhead
  (`internal/resilience`), with an explicit `DEGRADED` lane state instead of
  an indefinite hang.
- **A real, dependency-free MQTT 3.1.1 client** (`internal/eventbus`) with
  QoS1, persistent sessions, and auto-reconnect — verified against a live
  test broker over real TCP (`go test ./internal/eventbus/...`).
- **Versioned, canary-gated configuration** (`vertex-config` +
  `vertex-agent`): every config change is an immutable version with a
  canary percentage, deterministically bucketed by store, promoted or
  rolled back explicitly — or automatically, if `vertex-agent` observes an
  elevated error rate on a canary version.
- **Real mTLS workload identity** (`internal/identity`) with SPIFFE-style
  identity URIs — dev CA + per-service certs generated and verified for all
  24 services.
- **A durable offline queue** (`internal/outbox`): events that fail to
  publish are fsynced to disk and replayed in order once connectivity
  returns, so "what happens if the store loses cloud connectivity mid
  transaction" has an actual, tested answer.
- **End-to-end trace propagation** (`internal/tracing`), W3C-traceparent
  compatible, riding inside every event envelope across the MQTT hop.
- **Zero third-party Go dependencies** — every service is stdlib-only Go,
  built into minimal `distroless/static` images.
- **A live operations dashboard** (`frontend/`, React + TypeScript + Vite)
  showing fleet health and driving canary publish/promote/rollback against
  the real API.

## Repository layout

```
cmd/                    one directory per service (see docs/ARCHITECTURE.md
                         for the full rename table + implementation status)
internal/
  domain/                shared event schema
  eventbus/               MQTT client (stdlib-only) + in-memory bus + tests
  resilience/             circuit breaker + bulkhead
  identity/               mTLS / SPIFFE-style workload identity
  tracing/                W3C-traceparent-compatible span propagation
  outbox/                 durable offline queue-and-replay
  statemachine/           vertex-core's lane state machine
  config/, httpx/         small shared helpers
deploy/
  docker-compose.yml       full local stack (EMQX cluster, split Redis, Mongo, Jaeger)
  Dockerfile.service       multi-stage build for any cmd/<service>
  certs/                   dev CA + cert generation script
  k8s/                     namespace, Argo Rollouts canary, NetworkPolicy
  otel/, haproxy/          supporting infra config
frontend/                React + TS operations dashboard
docs/
  ARCHITECTURE.md          flaw -> fix mapping, service rename table
  RUNBOOK.md               exact commands + observed results
```

## Quick start

```bash
# any single service, no broker needed (falls back to an in-memory bus)
go build -o /tmp/vertex-core ./cmd/vertex-core && VERTEX_HTTP_ADDR=:8081 /tmp/vertex-core

# full test suite
go test ./...

# full stack
cd deploy && docker compose up --build
```

See [`docs/RUNBOOK.md`](docs/RUNBOOK.md) for the full walkthrough, including
the canary-rollout-and-auto-rollback demo and the MQTT wire-protocol
integration test.

## Origin

This project renames and redesigns the Jarvis SCO-X architecture following
an architecture review that identified nine concrete flaws (SPOF coupling,
no broker HA, overloaded Redis, no zero-trust identity, unversioned
deploys, store-server SPOF, dual POS integration complexity, no tracing, no
offline contract). See `docs/ARCHITECTURE.md` §1 for the full mapping from
each flaw to its fix in this codebase.
