# Runbook

## Prerequisites

- Go 1.22+ (no external modules required — `go build` works fully offline)
- Docker + Docker Compose, for the full clustered-broker stack
- Node 20+ / npm, for the dashboard (`frontend/`)

## Local (no Docker) — single-binary dev mode

Every service falls back to an in-process event bus when
`VERTEX_BROKER_ADDR` is unset, so you can run and exercise any one service
standalone without a broker:

```bash
go build -o /tmp/vertex-core ./cmd/vertex-core
VERTEX_HTTP_ADDR=:8081 /tmp/vertex-core &

curl -X POST "localhost:8081/lanes/lane-1?to=SCANNING&reason=item_scan"
curl -X POST "localhost:8081/lanes/lane-1?to=WEIGHING&reason=bag_placed"
curl -X POST "localhost:8081/lanes/lane-1?to=INTERVENTION&reason=weight_mismatch"
curl localhost:8081/health
```

This was run during development; observed output included the lane moving
IDLE → SCANNING → WEIGHING → INTERVENTION, an illegal-transition attempt
(`INTERVENTION -> PAYMENT`) correctly rejected with `409`, and the
`/health` response reporting the `vertex-intervention` circuit breaker as
`CLOSED` with `outbox_pending: 0`.

## Canary config + auto-rollback, exercised end-to-end

```bash
go build -o /tmp/vertex-config ./cmd/vertex-config && /tmp/vertex-config &
go build -o /tmp/vertex-control-plane ./cmd/vertex-control-plane && /tmp/vertex-control-plane &
VERTEX_STORE_ID=store-01 VERTEX_CONFIG_ADDR=http://localhost:8090 \
  VERTEX_CONTROL_PLANE_ADDR=http://localhost:8100 \
  go run ./cmd/vertex-agent &

# publish v1, promote to 100%
curl -X POST "localhost:8090/configs/vertex-core?canary_pct=100" -d '{}'
curl -X POST "localhost:8090/configs/vertex-core/promote?version=1"

# publish v2 at 25% canary and watch which stores land on it
curl -X POST "localhost:8090/configs/vertex-core?canary_pct=25" -d '{}'
for i in 1 2 3 4 5 6 7 8 9 10; do
  curl -s "localhost:8090/configs/vertex-core/active?store_id=store-$i" | jq .version
done
# observed during development: 3/10 stores on v2, 7/10 on v1 — matches the
# 25% canary_pct via deterministic hash bucketing (internal to vertex-config)

curl -X POST "localhost:8090/configs/vertex-core/rollback?version=2&reason=error_rate_spike"
curl "localhost:8090/configs/vertex-core/active?store_id=store-1" | jq .version
# observed: falls back to v1 for every store immediately after rollback

curl localhost:8100/fleet  # vertex-agent's health reports, aggregated by the control plane
```

## MQTT wire-protocol verification

```bash
go test ./internal/eventbus/... -run TestMQTTClientAgainstRealBroker -v
```

This spins up a minimal TCP test broker, connects two real `internal/eventbus.MQTT`
clients to it, subscribes on one, publishes on the other, and asserts the
envelope round-trips — proving the hand-rolled MQTT 3.1.1 codec
(`internal/eventbus/mqtt_wire.go`) is correct over real TCP bytes, not just
that it compiles. Observed result: `PASS`.

## Resilience unit tests

```bash
go test ./internal/resilience/... -v
```

Covers: circuit breaker CLOSED → OPEN after `FailureThreshold` consecutive
failures, fast-fail while OPEN, OPEN → HALF_OPEN after `OpenTimeout`,
HALF_OPEN → CLOSED on a successful trial call; and bulkhead rejecting a call
once its concurrency limit is saturated. Observed result: `PASS`.

## mTLS dev certificates

```bash
cd deploy/certs && ./generate-dev-ca.sh
```

Generates a root CA (`ca/ca.crt`) and one leaf certificate per service under
`issued/<service>/{tls.crt,tls.key,ca.crt}`, each with a SPIFFE-style
Subject CN (`spiffe://vertex.local/<tier>/<service>`). Verified during
development for all 24 services with `openssl x509 -noout -subject -issuer`.

## Full stack (Docker Compose)

```bash
cd deploy && docker compose -f docker-compose.yml -f ../frontend/../deploy/docker-compose.yml up --build
```

(Compose file lives at `deploy/docker-compose.yml`; run `docker compose -f
deploy/docker-compose.yml up --build` from the repo root.)

Brings up: 3-node EMQX cluster + HAProxy VIP, split Redis (cache/queue),
MongoDB, Jaeger + OTel collector, `vertex-config`, `vertex-control-plane`,
`vertex-core`, `vertex-intervention`, `vertex-weight`, `vertex-agent`, and
the dashboard.

- Dashboard: http://localhost:5173
- Control plane API: http://localhost:8100/fleet
- Jaeger UI: http://localhost:16686
- HAProxy stats: http://localhost:18404

**Note**: the Compose stack was authored and reviewed but not executed in
the build environment used to produce this repo (no Docker daemon
available there) — see `docs/ARCHITECTURE.md` §4 for what was and wasn't
run end-to-end.

## Frontend dashboard (standalone)

```bash
cd frontend
npm install
npm run dev      # http://localhost:5173, talks to VITE_CONTROL_PLANE_URL (.env)
npm run build    # verified: tsc -b && vite build succeeds with 0 errors
```
