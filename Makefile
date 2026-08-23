.PHONY: build test vet fmt certs frontend-build frontend-dev compose-up clean

SERVICES := $(shell ls cmd)

build: ## Build every service into ./bin
	@mkdir -p bin
	@for s in $(SERVICES); do \
		echo "building $$s"; \
		go build -o bin/$$s ./cmd/$$s || exit 1; \
	done

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

compose-up: ## Bring up the full docker-compose stack
	cd deploy && docker compose up --build

clean:
	rm -rf bin frontend/dist frontend/node_modules
