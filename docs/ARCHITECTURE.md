# Vertex SCO Platform — Architecture

Vertex SCO Platform is a self-checkout edge platform designed from an
explicit architecture review of a prior self-checkout reference design.
Every item below traces a flaw identified in that review to a concrete fix
implemented in this codebase — not just described, but built, compiled,
and in most cases exercised with a passing automated test or a live smoke
test during development.

## 1. Flaw → Fix map

| # | Flaw (architecture review) | Fix in Vertex | Where |
|---|---|---|---|
| 1 | The lane state manager is a soft single point of failure — everything coordinates through it synchronously, no fallback/degraded mode | `vertex-core` now *publishes* domain events after each state transition instead of calling dependents inline; every remaining downstream call goes through a `resilience.Guard` (circuit breaker + bulkhead); an explicit `DEGRADED` state exists and is entered automatically when a guarded call fails | `internal/statemachine`, `internal/resilience`, `cmd/vertex-core` |
| 2 | MQTT broker has no HA story — single `mosquitto`, no clustering, no persistent sessions, no QoS/dead-letter handling | Dependency-free MQTT 3.1.1 client with QoS1 + persistent sessions (`CleanSession=false`) + automatic reconnect/backoff + resubscribe-on-reconnect; compose stack runs a 3-node EMQX cluster behind an HAProxy VIP | `internal/eventbus/mqtt.go`, `deploy/docker-compose.yml`, `deploy/haproxy/haproxy.cfg` |
| 3 | Redis overloaded — cache + queue + broker on one instance, no failure-domain isolation | Split into `vertex-cache-redis` (LRU eviction, no persistence) and `vertex-queue-redis` (AOF durability) as separate services/failure domains | `deploy/docker-compose.yml` |
| 4 | No zero-trust/identity layer between tiers — no mTLS, no workload identity | `internal/identity` implements real mTLS via stdlib `crypto/tls`/`x509` with SPIFFE-style identity URIs (`spiffe://vertex.local/<tier>/<service>`); dev CA + per-service leaf certs generated and verified for all 24 services | `internal/identity/mtls.go`, `deploy/certs/generate-dev-ca.sh` |
| 5 | Config/deployment pushes unversioned — no canary, no staged rollout, no auto-rollback | `vertex-config` serves **immutable, versioned** configs with a `canary_pct` and deterministic per-store bucketing; `vertex-agent` polls it, deploys, evaluates health, and calls `rollback` automatically if the error rate exceeds a threshold while on a canary version; a matching Argo Rollouts canary manifest exists for the k8s path | `cmd/vertex-config`, `cmd/vertex-agent`, `deploy/k8s/vertex-core-rollout.yaml` |
| 6 | Store server likely a single node — store-wide SPOF | `deploy/k8s` targets a multi-replica `Rollout` per service rather than a single-node store server; `docs/ARCHITECTURE.md` (this section) calls out that the store-tier is designed to run as a small in-store Kubernetes cluster, not one box | `deploy/k8s/*` |
| 7 | Dual POS integration paths add branching complexity | Kept as two explicitly separate, independently deployable services (`vertex-pos-bridge` for legacy POS, `vertex-posless-adapter` for POS-less) rather than one service branching internally — isolates the complexity instead of hiding it | `cmd/vertex-pos-bridge`, `cmd/vertex-posless-adapter` |
| 8 | No end-to-end tracing — cross-tier bugs (over the MQTT hop especially) are invisible | `internal/tracing` propagates a W3C-traceparent-compatible `TraceContext` inside every event envelope and across HTTP; spans are emitted with a pluggable exporter (stdout JSON by default, OTLP-ready); `deploy/otel` has a working collector config wired to Jaeger | `internal/tracing`, `internal/domain/events.go` (`TraceContext`), `deploy/otel/otel-collector-config.yaml` |
| 9 | Offline/degraded-mode behavior implicit, not designed — no defined contract for what happens if connectivity drops mid-transaction | `internal/outbox` is a durable, fsynced, append-only local queue: every event that fails to publish is durably enqueued and replayed in order once the bus is reachable again, with no data loss across a process crash | `internal/outbox/outbox.go`, wired into `cmd/vertex-core` |

## 2. Service catalog

Every Vertex service maps to a specific role in the platform. Services
marked **(reference impl)** have real business logic implemented and
tested in this repo; services marked **(scaffold)** compile, run, and
expose a health endpoint following the same pattern, with domain-specific
logic left as a clearly marked extension point (see each `main.go`'s
`// TODO` markers).

### Cloud tier

| Service | Role | Status |
|---|---|---|
| `vertex-config` | Versioned configuration with canary rollout and rollback | reference impl (versioned + canary) |
| `vertex-control-plane` | Fleet-wide health aggregation, API for the operations dashboard | reference impl (fleet API + dashboard backend) |

### Store tier (Intelligent Edge Server)

| Service | Role | Status |
|---|---|---|
| `vertex-core` | Lane state machine — the central component driving a checkout transaction | reference impl |
| `vertex-intervention` | Intervention lifecycle management (create → resolve) | reference impl |
| `vertex-weight` | Bag-scale weight evaluation and mismatch detection | reference impl |
| `vertex-agent` | Store-side deployment agent — canary rollout, health-gated auto-rollback | reference impl (canary deploy + auto-rollback) |
| `vertex-coupon` | Coupon limit enforcement and coupon-sensor control | scaffold |
| `vertex-visualverify` | Visual-verify item handling and associated interventions | scaffold |
| `vertex-trilight` | Lane trilight indicator state | scaffold |
| `vertex-picklist` | Picklist item data serving/editing | scaffold |
| `vertex-cash` | Cash-device payload translation and error events | scaffold |
| `vertex-doc` | Document (receipt/journal) preparation for printing | scaffold |
| `vertex-print` | Printer device connectivity | scaffold |
| `vertex-weightlearning` | Weight-observation capture for the item-weight learning pipeline | scaffold |
| `vertex-errorlookup` | Device error code → description lookup | scaffold |
| `vertex-auth` | Shopper/associate authentication | scaffold |
| `vertex-resources` | Shared localized strings/media asset repository | scaffold |
| `vertex-inputsequencer` | Scan/keyed item input sequencing | scaffold |
| `vertex-pos-bridge` | Legacy POS integration bridge | scaffold |
| `vertex-posless-adapter` | POS-less / thin-client integration | scaffold |
| EMQX cluster (`emqx1/2/3` + `mqtt-lb`) | Clustered MQTT broker | infra |
| `vertex-cache-redis` + `vertex-queue-redis` | Split cache / durable queue | infra |
| `vertex-store-db` | Store-tier persistence (MongoDB) | infra |

### Terminal tier (SCO Lane)

| Service | Role | Status |
|---|---|---|
| `vertex-endpoint` | Lane registration/endpoint identity, lane-local business logic (EOD, reboot, shutdown) | scaffold |
| `vertex-devicegateway` | Hardware abstraction layer for physical devices (scanner, scale, printer, cash, trilight) | scaffold |
| `vertex-launchpad` | Terminal UI launcher, diagnostics, software/terminal lifecycle control | scaffold |
| — | Shopper-facing checkout UI is out of scope for this exercise; the operations/control-plane UI is covered by the dashboard in `frontend/` | n/a |

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
