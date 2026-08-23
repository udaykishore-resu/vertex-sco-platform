# Vertex SCO Platform — Architecture

Vertex SCO Platform is a rename-and-redesign of the Jarvis SCO-X reference
architecture, produced from an explicit architecture review. Every item
below traces a flaw identified in that review to a concrete fix implemented
in this codebase — not just described, but built, compiled, and in most
cases exercised with a passing automated test or a live smoke test during
development.

## 1. Flaw → Fix map

| # | Flaw (architecture review) | Fix in Vertex | Where |
|---|---|---|---|
| 1 | `scoxcoreservice` is a soft single point of failure — everything coordinates through it synchronously, no fallback/degraded mode | `vertex-core` now *publishes* domain events after each state transition instead of calling dependents inline; every remaining downstream call goes through a `resilience.Guard` (circuit breaker + bulkhead); an explicit `DEGRADED` state exists and is entered automatically when a guarded call fails | `internal/statemachine`, `internal/resilience`, `cmd/vertex-core` |
| 2 | MQTT broker has no HA story — single `mosquitto`, no clustering, no persistent sessions, no QoS/dead-letter handling | Dependency-free MQTT 3.1.1 client with QoS1 + persistent sessions (`CleanSession=false`) + automatic reconnect/backoff + resubscribe-on-reconnect; compose stack runs a 3-node EMQX cluster behind an HAProxy VIP | `internal/eventbus/mqtt.go`, `deploy/docker-compose.yml`, `deploy/haproxy/haproxy.cfg` |
| 3 | Redis overloaded — cache + queue + broker on one instance, no failure-domain isolation | Split into `vertex-cache-redis` (LRU eviction, no persistence) and `vertex-queue-redis` (AOF durability) as separate services/failure domains | `deploy/docker-compose.yml` |
| 4 | No zero-trust/identity layer between tiers — no mTLS, no workload identity | `internal/identity` implements real mTLS via stdlib `crypto/tls`/`x509` with SPIFFE-style identity URIs (`spiffe://vertex.local/<tier>/<service>`); dev CA + per-service leaf certs generated and verified for all 24 services | `internal/identity/mtls.go`, `deploy/certs/generate-dev-ca.sh` |
| 5 | Config/deployment pushes unversioned — no canary, no staged rollout, no auto-rollback | `vertex-config` serves **immutable, versioned** configs with a `canary_pct` and deterministic per-store bucketing; `vertex-agent` polls it, deploys, evaluates health, and calls `rollback` automatically if the error rate exceeds a threshold while on a canary version; a matching Argo Rollouts canary manifest exists for the k8s path | `cmd/vertex-config`, `cmd/vertex-agent`, `deploy/k8s/vertex-core-rollout.yaml` |
| 6 | Store server likely a single node — store-wide SPOF | `deploy/k8s` targets a multi-replica `Rollout` per service rather than a single-node store server; `docs/ARCHITECTURE.md` (this section) calls out that the store-tier is designed to run as a small in-store Kubernetes cluster, not one box | `deploy/k8s/*` |
| 7 | Dual POS integration paths add branching complexity | Kept as two explicitly separate, independently deployable services (`vertex-pos-bridge` for legacy POS, `vertex-posless-adapter` for POS-less) rather than one service branching internally — isolates the complexity instead of hiding it | `cmd/vertex-pos-bridge`, `cmd/vertex-posless-adapter` |
| 8 | No end-to-end tracing — cross-tier bugs (over the MQTT hop especially) are invisible | `internal/tracing` propagates a W3C-traceparent-compatible `TraceContext` inside every event envelope and across HTTP; spans are emitted with a pluggable exporter (stdout JSON by default, OTLP-ready); `deploy/otel` has a working collector config wired to Jaeger | `internal/tracing`, `internal/domain/events.go` (`TraceContext`), `deploy/otel/otel-collector-config.yaml` |
| 9 | Offline/degraded-mode behavior implicit, not designed — no defined contract for what happens if connectivity drops mid-transaction | `internal/outbox` is a durable, fsynced, append-only local queue: every event that fails to publish is durably enqueued and replayed in order once the bus is reachable again, with no data loss across a process crash | `internal/outbox/outbox.go`, wired into `cmd/vertex-core` |

## 2. Service rename table

Every original Jarvis/SCO-X service has a direct, documented Vertex
counterpart. Services marked **(reference impl)** have real business logic
implemented and tested in this repo; services marked **(scaffold)** compile,
run, and expose a health endpoint following the same pattern, with
domain-specific logic left as a clearly marked extension point (see each
`main.go`'s `// TODO` markers).

### Cloud tier

| Original | Vertex | Status |
|---|---|---|
| Centralized Config Mgmt / jarvisconfigservice | `vertex-config` | reference impl (versioned + canary) |
| Edge Control Plane / NCR Edge UI | `vertex-control-plane` | reference impl (fleet API + dashboard backend) |

### Store tier (Intelligent Edge Server)

| Original | Vertex | Status |
|---|---|---|
| scoxcoreservice | `vertex-core` | reference impl |
| scoxintervention | `vertex-intervention` | reference impl |
| scoxweightsecurity | `vertex-weight` | reference impl |
| Edge Agent | `vertex-agent` | reference impl (canary deploy + auto-rollback) |
| scoxcoupon | `vertex-coupon` | scaffold |
| scoxvisualverify | `vertex-visualverify` | scaffold |
| scoxtrilight | `vertex-trilight` | scaffold |
| scoxpicklist | `vertex-picklist` | scaffold |
| scoxcashservice | `vertex-cash` | scaffold |
| scoxdoc | `vertex-doc` | scaffold |
| scoxprinter | `vertex-print` | scaffold |
| scoxweightlearning | `vertex-weightlearning` | scaffold |
| scoxerrorlookup | `vertex-errorlookup` | scaffold |
| scoxauthentication | `vertex-auth` | scaffold |
| scoxresources | `vertex-resources` | scaffold |
| scoxinputsequencer | `vertex-inputsequencer` | scaffold |
| scoxtb7 | `vertex-pos-bridge` | scaffold |
| POSless Adapter | `vertex-posless-adapter` | scaffold |
| mosquitto | EMQX cluster (`emqx1/2/3` + `mqtt-lb`) | infra (replaces app-level service) |
| Redis | `vertex-cache-redis` + `vertex-queue-redis` | infra (split) |
| MongoDB | `vertex-store-db` | infra |

### Terminal tier (SCO Lane)

| Original | Vertex | Status |
|---|---|---|
| scoxendpoint | `vertex-endpoint` | scaffold |
| deviceServerCTM | `vertex-devicegateway` | scaffold |
| launchpad + sendscox-utils | `vertex-launchpad` | scaffold |
| electron app | frontend equivalent not in scope for terminal UI (dashboard in `frontend/` covers the operations/control-plane UI instead) | n/a |

## 3. Design principles carried through every service

- **Zero third-party Go dependencies.** Every `internal/*` package (MQTT
  client, circuit breaker, tracing, mTLS, outbox) is stdlib-only. This was
  originally a build-environment constraint but became a deliberate choice:
  smaller attack surface, no supply-chain risk from transitive
  dependencies, tiny static binaries (see `deploy/Dockerfile.service`,
  which builds on `distroless/static`).
- **Event-driven, not call-graph-driven.** Services communicate by
  publishing/subscribing to versioned events (`internal/domain/events.go`),
  not by calling each other's APIs synchronously in the hot path. The only
  synchronous calls that remain (e.g. `vertex-core` → `vertex-intervention`
  event publish) are wrapped in a `resilience.Guard`.
- **Every cross-service, cross-tier hop is traceable.** `TraceContext` rides
  inside the event envelope; a transaction can be followed from
  `vertex-endpoint` (terminal) through `vertex-core` (store) through
  `vertex-control-plane` (cloud) in one system, once the OTLP exporter is
  wired in.
- **Nothing about a deploy is implicit.** Every config change is a new
  immutable version with an explicit canary percentage; promotion and
  rollback are explicit actions, and rollback can also be triggered
  automatically by observed health.

## 4. What's a full implementation vs. a scaffold, honestly

This is a portfolio/reference implementation, not a live production system
with real hardware behind it — being upfront about that matters more than
overclaiming:

- `vertex-core`, `vertex-intervention`, `vertex-weight`, `vertex-config`,
  `vertex-agent`, `vertex-control-plane` are fully implemented, build, and
  were exercised end-to-end during development (see `docs/RUNBOOK.md` for
  the exact commands and observed output).
- The MQTT client (`internal/eventbus/mqtt.go`) is a real implementation of
  MQTT 3.1.1's CONNECT/PUBLISH/SUBSCRIBE/PUBACK/PING flow, verified against
  a test broker over real TCP in `internal/eventbus/mqtt_integration_test.go`
  — not just a mock.
- The mTLS identity layer generates and loads real X.509 certs; it is not
  wired into every scaffold service's HTTP listener yet (that's a
  mechanical follow-up, not a design gap — the `ServerTLSConfig`/
  `ClientTLSConfig` functions are ready to use).
- The remaining ~17 store/terminal services are scaffolds: they compile,
  run, expose `/health`, and connect to the event bus, with their
  domain-specific business logic left as marked extension points following
  the pattern already proven out in `vertex-core`/`vertex-intervention`/
  `vertex-weight`.
- The Argo Rollouts / NetworkPolicy manifests in `deploy/k8s` describe the
  intended production topology but were not deployed against a live
  cluster as part of this exercise (no k8s cluster available in the build
  environment).
