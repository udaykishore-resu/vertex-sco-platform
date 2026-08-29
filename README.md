# Vertex SCO Platform

[![CI](https://github.com/udaykishore-resu/vertex-sco-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/udaykishore-resu/vertex-sco-platform/actions/workflows/ci.yml)

A three-tier, event-driven self-checkout (SCO) edge platform — cloud (GCP/GKE-style),
store (Intelligent Edge Server), and terminal (SCO lane) — designed from the
ground up around an explicit architecture review of a prior self-checkout
reference design. Every identified flaw has a concrete, working fix in this
codebase; see
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full flaw → fix
mapping and the complete service catalog, and
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
make run
```

That is the whole thing. `go.mod` has no dependencies, so there is nothing to
download, no Docker, no broker, no certificates. `make run` builds four
services — the config store, the fleet control plane, one store-tier service
and the edge agent — starts them, and walks through what they do:

```
1. the lane state machine ...
   checkout  -> PAYMENT   HTTP 200
   paid      -> COMPLETE  HTTP 200
   illegal   -> PAYMENT   HTTP 409  (COMPLETE may only go back to IDLE)

2. a canary rollout ...
   published v1 at 100%, then v2 to 50% of stores
   store-1=v2 store-2=v2 ... store-8=v1 store-9=v1 store-10=v1

3. the rollback. One call, and every store is back on the old version.

4. the edge agent reconciles ... and reports what it has deployed
   fleet:  [{"store_id":"store-demo","service_name":"vertex-core","version":1,...}]
```

Then:

```bash
make demo        # run that walkthrough again against the running platform
make run-ui      # the React dashboard on :5173 (needs npm)
make test        # full test suite
make compose-up  # all 24 services, EMQX cluster, split Redis, Mongo, Jaeger
```

The four services `make run` starts talk over HTTP. The event-driven pairs —
vertex-core publishing `intervention.requested` to vertex-intervention, for
instance — need the MQTT broker, because with `VERTEX_BROKER_ADDR` unset each
process falls back to an in-process bus that reaches no one else. That is what
`make compose-up` is for.

See [`docs/RUNBOOK.md`](docs/RUNBOOK.md) for the full walkthrough, including
the canary-rollout-and-auto-rollback demo and the MQTT wire-protocol
integration test.

## Origin

This project's design followed an explicit architecture review of a prior
self-checkout reference design that identified nine concrete flaws (SPOF
coupling, no broker HA, overloaded Redis, no zero-trust identity,
unversioned deploys, store-server SPOF, dual POS integration complexity, no
tracing, no offline contract). See `docs/ARCHITECTURE.md` §1 for the full
mapping from
each flaw to its fix in this codebase.

## License

MIT — see [LICENSE](LICENSE).
