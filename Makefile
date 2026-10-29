# Windows without make: every target is one plain command, copy it from here.
.PHONY: help build test test-integration cover lint fmt tidy \
        up up-apps down logs ps \
        kind-up kind-load k8s-infra k8s-apps k8s-down

IMAGE ?= saga3:dev

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

# ── Go ───────────────────────────────────────────────────────────────
build: ## build all binaries into ./bin
	go build -o ./bin/ ./cmd/...

test: ## unit tests (no Docker needed)
	go test -count=1 ./...

test-integration: ## integration + full-flow tests (needs Docker; testcontainers)
	go test -tags=integration -count=1 -timeout=10m ./test/integration/...

cover: ## unit test coverage report
	go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | tail -1

lint: ## golangci-lint (install: https://golangci-lint.run)
	golangci-lint run ./...

fmt:
	gofmt -s -w . && go mod tidy

# ── Docker Compose ───────────────────────────────────────────────────
up: ## infra + migrations + topics
	docker compose up -d --build

up-apps: ## infra + all services
	docker compose --profile apps up -d --build

down: ## stop everything and drop volumes
	docker compose --profile apps down -v

logs:
	docker compose --profile apps logs -f --tail=100

ps:
	docker compose --profile apps ps

# ── Kubernetes (kind) ────────────────────────────────────────────────
kind-up: ## create kind cluster + install Strimzi
	kind create cluster --config deploy/k8s/apps/overlays/kind/kind-cluster.yaml
	kubectl create namespace saga
	kubectl create -f 'https://strimzi.io/install/latest?namespace=saga' -n saga
	kubectl -n saga rollout status deployment/strimzi-cluster-operator --timeout=300s

kind-load: ## build image and load it into kind
	docker build -t $(IMAGE) .
	kind load docker-image $(IMAGE) --name saga3

k8s-infra: ## postgres, kafka (Strimzi), otel, jaeger
	kubectl apply -k deploy/k8s/infra
	kubectl -n saga wait kafka/saga-kafka --for=condition=Ready --timeout=600s

k8s-apps: ## jobs + services
	kubectl apply -k deploy/k8s/apps/overlays/kind

k8s-down:
	kind delete cluster --name saga3
