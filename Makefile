# Windows without make: every target is one plain command, copy it from here.
.PHONY: help build test test-integration cover lint fmt tidy \
        up up-apps down logs ps \
        kind-up kind-load k8s-infra k8s-apps k8s-down load up-pgbouncer

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
STRIMZI_VERSION ?= 1.3.0
KEDA_VERSION ?= 2.21.0

kind-up: ## create kind cluster + install Strimzi and KEDA (pinned versions)
	kind create cluster --config deploy/k8s/apps/overlays/kind/kind-cluster.yaml --wait 180s
	kubectl create namespace saga
	curl -sL https://github.com/strimzi/strimzi-kafka-operator/releases/download/$(STRIMZI_VERSION)/strimzi-cluster-operator-$(STRIMZI_VERSION).yaml \
	  | sed 's/namespace: myproject/namespace: saga/g' | kubectl apply --server-side -n saga -f -
	kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v$(KEDA_VERSION)/keda-$(KEDA_VERSION).yaml
	kubectl -n saga rollout status deployment/strimzi-cluster-operator --timeout=300s
	kubectl -n keda rollout status deployment/keda-operator --timeout=300s

kind-load: ## build image and load it into kind
	docker build -t $(IMAGE) .
	kind load docker-image $(IMAGE) --name saga3

k8s-infra: ## postgres, kafka (Strimzi), otel, jaeger
	kubectl apply -k deploy/k8s/infra
	kubectl -n saga wait kafka/saga-kafka --for=condition=Ready --timeout=600s

k8s-apps: ## jobs + services (+ KEDA lag autoscaling)
	kubectl apply -k deploy/k8s/apps/overlays/kind
	kubectl apply -n saga -f deploy/k8s/keda/scaledobjects.yaml

# ── Load test / PgBouncer ────────────────────────────────────────────
RATE ?= 50
DURATION ?= 60s
load: ## k6 against the compose API (RATE orders/s for DURATION)
	docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 -e RATE=$(RATE) -e DURATION=$(DURATION) \
	  -e SEAT_OFFSET=$$(( (RANDOM % 80 + 1) * 1000 )) grafana/k6:2.3.0 run - < test/load/orders.js

up-pgbouncer: ## all services, API and workers through PgBouncer
	docker compose -f docker-compose.yml -f docker-compose.pgbouncer.yml --profile apps up -d --build

k8s-down:
	kind delete cluster --name saga3
