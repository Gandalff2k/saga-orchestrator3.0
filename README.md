# Saga Orchestrator v3 (Go)

A seat booking saga: reserve a seat, charge the payment, compensate if anything fails. Go rewrite of [saga-orchestrator 2.0](https://github.com/Gandalff2k/saga-orchestrator2.0) with a sharded outbox relay instead of Debezium, batch consumers, a DLQ, a 3-broker Kafka, Kubernetes manifests and telemetry.

## Start

```bash
docker compose up -d --build # Kafka x3, Postgres, Jaeger, Prometheus, Grafana, runs migrations and creates topics

docker compose --profile apps up -d --build # plus all the services (api, 3 workers, 3 outbox relays)

docker compose --profile apps up -d --scale inventory-worker=4 # more consumers, up to 12 do work (12 partitions)

docker compose --profile apps up -d --scale outbox-relay-inventory=3 # more relays, they split 16 shards between themselves

curl -i -X POST localhost:8080/orders -H 'Idempotency-Key: demo-1' -d '{"seat_id": 1, "amount_cents": 10000}' # 202 + Location

curl localhost:8080/orders/<order_id> # order status and saga state

curl -X POST localhost:8080/orders -d '{"seat_id": 2, "amount_cents": 500000}' # above the payment limit, so declined and the seat is released

docker compose stop kafka-2 # lose a broker, everything keeps working

docker compose stop payment-worker # after 30s the sweeper compensates the stuck sagas

docker compose run --rm --no-deps topics sagactl dlq list -topic inventory.commands.dlq # look into a DLQ, replay with "dlq replay"

go test ./... # unit tests, no Docker needed

go test -tags=integration -count=1 ./test/integration/... # 33 scenarios, starts Postgres and Kafka containers itself

docker compose --profile apps down -v # remove the containers and their volumes
```

Jaeger on localhost:16686, Grafana (dashboard "saga3") on localhost:3000, Prometheus with alerts on localhost:9090, Kafka UI on localhost:8085. Running on Kubernetes (kind, Strimzi, KEDA).

## Architecture

PostgreSQL, one database per service (orchestrator, inventory, payment), the source of truth. Each one has its own outbox and inbox tables. migrate and topics are one-shot jobs that apply goose migrations and create the Kafka topics, the services start only after they succeed. Kafka is 3 brokers in KRaft mode, every topic has 12 partitions, replication factor 3 and min.insync.replicas 2. orchestrator-api is the HTTP service that starts sagas. orchestrator-worker reads replies and moves sagas forward, it also runs the timeout sweeper. inventory-worker reserves and releases seats. payment-worker charges and refunds. outbox-relay, one deployment per database, publishes outbox rows into Kafka.

A flow of events: user makes POST /orders, the API writes the order, the saga in RESERVING and a reserve_seat command into the outbox in one transaction and answers 202. The orchestrator relay publishes the command into inventory.commands. inventory-worker reserves the seat and in the same transaction writes the reply into its own outbox, its relay puts it into orchestrator.replies. orchestrator-worker moves the saga to CHARGING and writes a charge command. payment-worker charges and replies, the saga goes COMPLETED and the order CONFIRMED. If the payment is declined the saga goes COMPENSATING, releases the seat and ends ABORTED. If a step hangs longer than SAGA_STEP_TIMEOUT the sweeper refunds and releases. Every service talks to the others only through Kafka, never over HTTP.

```
RESERVING --seat_reserved--> CHARGING --charged--> COMPLETED
    | failed                   | declined / timeout
    v                          v
 ABORTED <--seat_released-- COMPENSATING (REFUNDING first if it was charged)
```

## Structure

cmd one main.go per binary, only wiring: config, build, run. internal/orchestrator the saga state machine (saga.go, pure, no I/O), the service, SQL, HTTP handlers and the sweeper. internal/inventory, internal/payment the participants, service.go with handlers and store.go with SQL. internal/messaging the contract between services: topics, message types, payloads, with golden files. internal/platform infrastructure with no business words in it: outbox, inbox, kafka consumer and DLQ, retry, config, telemetry, logging. migrations goose SQL, platform/ goes into every database. test/integration real Postgres and Kafka. test/load k6. deploy compose configs, k8s manifests, KEDA. docs/adr why the big decisions were made.

http calls the service, the service opens the transaction and calls store functions. The state machine returns what to do and never does it. platform never imports a business package, so it could serve another saga as is.

## Edge case handling

Two orders for the same seat: a partial unique index allows only one HELD reservation per seat, so the second one gets a conflict, replies seat_reservation_failed and that saga ends ABORTED. The database decides, not the code.

Duplicate requests from the user: Idempotency-Key with a unique constraint and INSERT ON CONFLICT DO NOTHING. If the key already exists the existing order is returned as 200. Same key with a different body is 422, not the old order.

Several relays on one outbox: the outbox is split into 16 shards by hash of the saga id. Each relay holds leases on its fair share of the shards and only the owner publishes a shard, in seq order. When a relay is added or dies the leases move, a crashed one loses its shards after the 15s lease TTL.

State change and its message: the outbox row is written in the same transaction as the business change, so either both exist or neither. Services never produce to Kafka directly.

Publishing from the database to Kafka: a row is marked published only after acks=all from Kafka. A crash in between means the row is published again, so delivery is at-least-once, not exactly-once. If only part of a batch was acknowledged only that prefix is marked. A failed shard backs off and retries forever, it never skips a row because the next one would get ahead of it.

Message consuming: the Kafka key is the saga id, so one saga's messages are in one partition and in order. A worker takes a batch per partition and handles it in one database transaction together with inbox rows (consumer, message_id), so a redelivered message is a no-op. Offsets are committed only after that transaction commits, auto commit is off. If the batch fails it is replayed one record at a time to find the bad one. A bad payload or unknown type goes straight into topic.dlq with the error in headers. A transient error retries with exponential backoff and jitter, after the retries run out it goes to the DLQ only if the database is healthy. If the database is down the worker stops and doesn't commit, so nothing is dead-lettered because of an outage. On a rebalance a partly handled partition is rewound, never skipped.

Seat taken or payment declined are not errors, they are reply messages and the saga compensates.

A worker hangs or is down: the sweeper finds sagas whose updated_at is older than SAGA_STEP_TIMEOUT with FOR UPDATE SKIP LOCKED (so several workers split them) and compensates. A charge that arrives after its refund hits the CANCELLED tombstone and is refused, a reservation after its release hits the RELEASED one. Every compensation is idempotent and always replies.

Database rollback: everything is one transaction, so on failure the order, the saga, the outbox row and the inbox row are all gone together and the message will be handled again.

Graceful shutdown: stop polling, finish the batch in flight, commit, release the leases, close the clients, all within SHUTDOWN_TIMEOUT, which is shorter than the Kubernetes grace period.

## Architecture changes

Own relay instead of Debezium: Debezium runs one task per database and needs Kafka Connect and a logical replication slot. The relay is a plain Go binary that scales up to 16 replicas per database.

Shards with leases instead of FOR UPDATE SKIP LOCKED in the relay: with SKIP LOCKED two relays can hold messages of one saga in different batches and publish them in the wrong order. Shards keep order per saga and still spread the load.

Polling instead of LISTEN/NOTIFY: NOTIFY was built and measured. A transaction with NOTIFY takes a database-wide lock on commit, so all order transactions committed one by one, HTTP p95 got 5 times worse and throughput fell from 150 to 94 orders/s. Rolled back, the relay polls every 200ms.

No retry topics: a message parked in a retry topic lets the next message of the same saga go ahead of it. Retries happen in place, the partition waits.

Inbox in the same transaction instead of only idempotent handlers: the handler and the dedup row commit together, so a crash between them can't happen.

Sweeper owns the timeout: the first version stored a deadline, the first one written by the API and the next ones by the worker, each with its own config. On Kubernetes the two configs drifted apart and 541 of 600 sagas were compensated for nothing. Now only the sweeper decides, from updated_at.

Kafka key = saga id and partitions fixed at 12: adding partitions moves keys to other partitions, so it needs in-flight sagas drained first.

UUIDv7 for ids: they are time ordered, so primary key inserts don't scatter over the index.

Workers scale by consumer lag with KEDA, not CPU: a worker waiting on the database has low CPU and a big lag. The API scales by CPU with HPA.

PgBouncer in transaction mode for the API and workers: every transaction lives on one server connection and the janitor lock is transaction scoped, so it is safe. Relays connect directly to keep their lease timing predictable. An optional compose override.

Every decision with the rejected options is in DECISIONS.md and docs/adr.

## Tests and tools

pgx for Postgres, franz-go for Kafka (cooperative sticky rebalancing, idempotent producer), goose for migrations, OpenTelemetry for traces and metrics, slog for JSON logs with trace_id, testcontainers for integration tests, k6 for load. Stdlib testing only, fakes are written by hand.

Unit tests cover every saga transition and timeout, retry and backoff, the consumer algorithm (batch ok, poison in the middle, DLQ down so no commit, database outage) with fake processors, relay lease math, message contracts with golden files, HTTP and metrics. 33 integration scenarios run every component in process against real Postgres and Kafka: outbox atomicity, relay order and crash takeover, duplicates, poison, a real database outage, rebalance in the middle of a retry, the full saga happy and failing paths, timeouts with refunds and tombstones, concurrent sweepers, the DLQ list, fix and replay cycle. Coverage is 81.9% from unit and integration together.

Critical tests are mutation checked: the logic under test is broken on purpose and the test has to go red. Running for real found bugs that tests didn't: a rebalance that skipped a half handled partition, a 5s metadata cache hiding in a 200ms poll, alerts that never fired because the counter was born with its first event, the deadline drift above. Each got a test that failed first, then the fix.

Checked by hand on compose and on kind: killed and stopped workers and relays, a stopped broker, a drained node, KEDA scaling 2 to 12 and back. After each run the counts were reconciled: confirmed orders = held seats = charged payments, 0 double charges. Load: 150 orders/s sustained on one laptop, HTTP p95 86ms, saga end to end p95 about 2s.

CI: vet, unit tests with -race and golangci-lint, then integration tests with coverage, then the Docker image, compose config and rendered k8s manifests.

## What is missing for prod grade code, cause it is presonal isolated project

No authentication on the API. The payment is a fake with a limit, a real provider needs its own idempotency keys. Postgres is a single instance with no replicas or backups. Alerts are defined but there is no Alertmanager routing, nobody gets paged. DSNs are plain env values, no secret manager. The relay lease is a time based fence, a paused relay can publish duplicates after losing its lease (the inbox absorbs them). Shard and partition counts are fixed, changing them needs a drain. CI does not run the compose smoke test or k6. No Helm or Argo CD, kustomize only.
