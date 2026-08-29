.PHONY: help run demo run-ui stop build test vet fmt certs frontend-build frontend-dev check-ports compose-up clean

SERVICES := $(shell ls cmd)

# The four services that carry the demo: the config store, the fleet control
# plane, one store-tier service, and the edge agent that ties them together.
# The other nineteen are scaffolds or need the MQTT broker from `make compose-up`.
RUN_SERVICES := vertex-config vertex-control-plane vertex-core vertex-agent

CONFIG_URL        ?= http://localhost:8090
CONTROL_PLANE_URL ?= http://localhost:8100
CORE_URL          ?= http://localhost:8081
AGENT_URL         ?= http://localhost:8095

.DEFAULT_GOAL := help

help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'


build: ## Build every service into ./bin
	@mkdir -p bin
	@for s in $(SERVICES); do \
		echo "building $$s"; \
		go build -o bin/$$s ./cmd/$$s || exit 1; \
	done

# `make run` is the platform in one command. The Go half needs nothing at all —
# go.mod has no dependencies, so there is no module download, no Docker, no
# broker, no certs. Services fall back to an in-process event bus when
# VERTEX_BROKER_ADDR is unset, which is why this runs the four services that
# talk over HTTP and leaves the event-driven pairs to `make compose-up`.

run: ## Clean clone to a running four-service platform, then walk through it
	@command -v go >/dev/null 2>&1 || { echo "go is required: https://go.dev/dl/"; exit 1; }
	@command -v curl >/dev/null 2>&1 || { echo "curl is required"; exit 1; }
	@mkdir -p bin
	@echo "building (no module dependencies — nothing to download)"
	@for s in $(RUN_SERVICES); do go build -o bin/$$s ./cmd/$$s || exit 1; done
	@./bin/vertex-config > bin/vertex-config.log 2>&1 & \
		cfg=$$!; \
		./bin/vertex-control-plane > bin/vertex-control-plane.log 2>&1 & \
		cp=$$!; \
		./bin/vertex-core > bin/vertex-core.log 2>&1 & \
		core=$$!; \
		VERTEX_CONFIG_ADDR=$(CONFIG_URL) VERTEX_CONTROL_PLANE_ADDR=$(CONTROL_PLANE_URL) \
			./bin/vertex-agent > bin/vertex-agent.log 2>&1 & \
		agent=$$!; \
		trap "kill $$cfg $$cp $$core $$agent 2>/dev/null; exit 0" INT TERM; \
		trap "kill $$cfg $$cp $$core $$agent 2>/dev/null" EXIT; \
		for url in $(CONFIG_URL) $(CONTROL_PLANE_URL) $(CORE_URL) $(AGENT_URL); do \
			n=0; ready=""; \
			while [ $$n -lt 60 ]; do \
				if curl -sf -o /dev/null $$url/health 2>/dev/null; then ready=yes; break; fi; \
				n=$$((n+1)); sleep 0.25; \
			done; \
			if [ -z "$$ready" ]; then echo "$$url never became healthy — see bin/*.log"; exit 1; fi; \
		done; \
		$(MAKE) --no-print-directory demo; \
		echo "  all four are still running:"; \
		echo "    $(CONFIG_URL)   vertex-config         versioned config, canary, promote, rollback"; \
		echo "    $(CONTROL_PLANE_URL)   vertex-control-plane  fleet health, dashboard API"; \
		echo "    $(CORE_URL)   vertex-core           the lane state machine"; \
		echo "    $(AGENT_URL)   vertex-agent          reconciles config, reports health, auto-rolls-back"; \
		echo; \
		echo "    make demo     # run the walkthrough above again"; \
		echo "    make run-ui   # the React dashboard on http://localhost:5173 (needs npm)"; \
		echo; \
		echo "  logs are in bin/*.log. The other 19 services and the MQTT-backed event"; \
		echo "  flow between them need the full stack: make compose-up."; \
		echo; \
		echo "  ctrl-c stops everything."; \
		echo; \
		wait $$core

demo: ## Drive a running platform: lane state machine, canary rollout, rollback, fleet
	@command -v curl >/dev/null 2>&1 || { echo "curl is required"; exit 1; }
	@curl -sf -o /dev/null $(CORE_URL)/health 2>/dev/null || { \
		echo "nothing on $(CORE_URL) — run 'make run' first"; exit 1; }
	@echo
	@echo "1. the lane state machine. Every transition is checked against one table"
	@echo "   in internal/statemachine, so an illegal move is a 409, not a bad state."
	@lane=lane-$$$$; \
		for step in "SCANNING item_scan" "WEIGHING bag_placed" "SCANNING weight_ok" "PAYMENT checkout" "COMPLETE paid"; do \
			to=$${step%% *}; reason=$${step#* }; \
			code=$$(curl -s -o /dev/null -w '%{http_code}' -X POST "$(CORE_URL)/lanes/$$lane?to=$$to&reason=$$reason"); \
			printf '   %-9s -> %-9s HTTP %s\n' "$$reason" "$$to" "$$code"; \
		done; \
		code=$$(curl -s -o /dev/null -w '%{http_code}' -X POST "$(CORE_URL)/lanes/$$lane?to=PAYMENT&reason=illegal"); \
		printf '   %-9s -> %-9s HTTP %s  (COMPLETE may only go back to IDLE)\n' "illegal" "PAYMENT" "$$code"
	@echo
	@echo "2. a canary rollout. Publish a config at 100%, then a second version to"
	@echo "   half the fleet, and ask which version ten different stores resolve to."
	@v1=$$(curl -s -X POST "$(CONFIG_URL)/configs/vertex-core?canary_pct=100" -d '{"scan_timeout_ms":800}' \
			| sed 's/.*"version":\([0-9]*\).*/\1/'); \
		v2=$$(curl -s -X POST "$(CONFIG_URL)/configs/vertex-core?canary_pct=50" -d '{"scan_timeout_ms":400}' \
			| sed 's/.*"version":\([0-9]*\).*/\1/'); \
		printf '   published v%s at 100%%, then v%s to 50%% of stores\n' "$$v1" "$$v2"; \
		printf '   '; \
		for i in 1 2 3 4 5 6 7 8 9 10; do \
			v=$$(curl -s "$(CONFIG_URL)/configs/vertex-core/active?store_id=store-$$i" \
				| sed 's/.*"version":\([0-9]*\).*/\1/'); \
			printf 'store-%s=v%s ' "$$i" "$$v"; \
		done; \
		echo; \
		echo "   The bucket is sha256(store_id) — stable, so a store does not flap"; \
		echo "   in and out of the canary between polls."; \
		echo; \
		echo "3. the rollback. One call, and every store is back on the old version."; \
		curl -s -X POST "$(CONFIG_URL)/configs/vertex-core/rollback?version=$$v2&reason=error_rate_spike" > /dev/null; \
		printf '   rolled back v%s: ' "$$v2"; \
		for i in 1 2 3 4 5; do \
			v=$$(curl -s "$(CONFIG_URL)/configs/vertex-core/active?store_id=store-$$i" \
				| sed 's/.*"version":\([0-9]*\).*/\1/'); \
			printf 'store-%s=v%s ' "$$i" "$$v"; \
		done; \
		echo
	@echo
	@echo "4. the edge agent reconciles against vertex-config every 5s and reports"
	@echo "   what it actually has deployed to the control plane. Waiting for a poll."
	@n=0; \
		while [ $$n -lt 40 ]; do \
			if [ "$$(curl -s $(CONTROL_PLANE_URL)/fleet)" != "[]" ]; then break; fi; \
			n=$$((n+1)); sleep 0.5; \
		done
	@printf '   agent:  '; curl -s $(AGENT_URL)/health; echo
	@printf '   fleet:  '; curl -s $(CONTROL_PLANE_URL)/fleet; echo
	@echo

run-ui: ## Run the React dashboard against a platform started by `make run`
	@command -v npm >/dev/null 2>&1 || { echo "npm is required: https://nodejs.org/"; exit 1; }
	@echo "the committed frontend/.env points at the docker-compose ports (1xxxx);"
	@echo "overriding it for the native ports make run uses"
	cd frontend && npm install && VITE_CONTROL_PLANE_URL=$(CONTROL_PLANE_URL) npm run dev

test: ## Run the full Go test suite
	go test ./...

vet: ## go vet everything
	go vet ./...

fmt: ## gofmt everything (writes changes)
	gofmt -w .

certs: ## Generate dev CA + per-service mTLS certs
	cd deploy/certs && ./generate-dev-ca.sh

frontend-build: ## Typecheck + build the dashboard
	cd frontend && npm install && npm run build

frontend-dev: ## Run the dashboard dev server
	cd frontend && npm install && npm run dev

check-ports: ## Verify every Vertex host port is free before starting the stack
	./deploy/check-ports.sh

compose-up: check-ports ## Verify ports, then bring up the full docker-compose stack
	cd deploy && docker compose up --build

clean:
	rm -rf bin frontend/dist frontend/node_modules
