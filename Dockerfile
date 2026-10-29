# syntax=docker/dockerfile:1.7
# One image, every binary: orchestrator-api, *-worker, outbox-relay, sagactl.
# Compose/k8s pick the binary via `command`.

FROM golang:1.27-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags="-s -w -X github.com/Gandalff2k/saga3/internal/platform/app.Version=${VERSION}" \
      -o /out/ ./cmd/...

# distroless/static: no shell, no package manager, runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot:nonroot
EXPOSE 8080 8081
