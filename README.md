# Saga Orchestrator v3 (Go)

A seat booking saga: check the order for fraud, reserve a seat, charge the payment, compensate if anything fails. Go rewrite of [saga-orchestrator 2.0](https://github.com/Gandalff2k/saga-orchestrator2.0) with a sharded outbox relay instead of Debezium, batch consumers, a DLQ, a 3-broker Kafka, Kubernetes manifests and telemetry. On top of that a risk service scores every order with explainable rules, sends the grey zone to manual review, and an LLM agent prepares each review case for the analyst without deciding anything itself.

## Start

```bash
docker compose up -d --build # Kafka x3, Postgres, Jaeger, Prometheus, Grafana, runs migrations and creates topics

docker compose --profile apps up -d --build # plus all the services (api, 4 workers, 4 outbox relays)

docker compose --profile apps up -d --scale inventory-worker=4 # more consumers, up to 12 do work (12 partitions)

docker compose --profile apps up -d --scale outbox-relay-inventory=3 # more relays, they split 16 shards between themselves

curl -i -X POST localhost:8080/orders -H 'Idempotency-Key: demo-1' -d '{"seat_id": 1, "amount_cents": 5000, "customer_id": "cus_1", "card_number": "4242424242424242", "ip_address": "203.0.113.7", "country": "UA"}' # 202 + Location

curl localhost:8080/orders/<order_id> # order status and saga state

curl -X POST localhost:8080/orders -d '{"seat_id": 2, "amount_cents": 500000, "customer_id": "cus_2", "card_number": "4000056655665556", "ip_address": "203.0.113.8", "country": "UA"}' # above the 100.00 review limit, so the saga waits in IN_REVIEW

docker compose run --rm --no-deps migrate sagactl review list # the review queue with the rules that fired and the agent's advice

docker compose run --rm --no-deps migrate sagactl review approve <saga_id> -by alice # approved, but 5000.00 is above the payment limit, so declined and the seat is released

docker compose --profile apps --profile agent up -d # plus the LLM review agent, needs LLM_API_KEY in .env (any OpenAI compatible provider, Groq by default)

go run ./cmd/fraudgen run -seed 1 && RISK_DSN=postgres://saga:saga@localhost:5432/risk?sslmode=disable go run ./cmd/fraudgen report # labelled fraud traffic, then precision and recall of the rules

go run ./cmd/risk-eval # the agent on 21 labelled cases against a live model (LLM_* env), -record saves the answers for CI

docker compose stop kafka-2 # lose a broker, everything keeps working

docker compose stop risk-worker # after 30s new orders abort, nothing passes unchecked

docker compose stop payment-worker # after 30s the sweeper compensates the stuck sagas

docker compose run --rm --no-deps topics sagactl dlq list -topic inventory.commands.dlq # look into a DLQ, replay with "dlq replay"

go test ./... # unit tests and the recorded agent eval, no Docker or network needed

go test -tags=integration -count=1 ./test/integration/... # 43 scenarios, starts Postgres and Kafka containers itself

docker compose --profile apps --profile agent down -v # remove the containers and their volumes
```

Jaeger on localhost:16686, Grafana (dashboard "saga3", risk panels at the bottom) on localhost:3000, Prometheus with alerts on localhost:9090, Kafka UI on localhost:8085. Running on Kubernetes (kind, Strimzi, KEDA).

## Architecture

PostgreSQL, one database per service (orchestrator, risk, inventory, payment), the source of truth. Each one has its own outbox and inbox tables. migrate and topics are one-shot jobs that apply goose migrations and create the Kafka topics, the services start only after they succeed. Kafka is 3 brokers in KRaft mode, every topic has 12 partitions, replication factor 3 and min.insync.replicas 2. orchestrator-api is the HTTP service that starts sagas. orchestrator-worker reads replies and moves sagas forward, it also runs the timeout sweeper. risk-worker scores orders and closes review cases. inventory-worker reserves and releases seats. payment-worker charges and refunds. outbox-relay, one deployment per database, publishes outbox rows into Kafka. risk-agent is optional, it reads the review queue and writes advice for the analyst.

A flow of events: user makes POST /orders, the API turns the card number into an HMAC, writes the order, the saga in SCORING and a score command into the outbox in one transaction and answers 202. The orchestrator relay publishes the command into risk.commands. risk-worker counts this order against the card's, customer's and IP's history, runs the rules and in the same transaction writes the decision and the reply into its own outbox, its relay puts it into orchestrator.replies. Approved: the saga goes RESERVING and writes a reserve_seat command. inventory-worker reserves the seat and replies, orchestrator-worker moves the saga to CHARGING and writes a charge command. payment-worker charges and replies, the saga goes COMPLETED and the order CONFIRMED. Blocked: the saga ends ABORTED with the rules in failure_reason and nothing was reserved or charged. Review: the saga waits in IN_REVIEW until a person approves or rejects the case with sagactl, the risk service then sends approved or rejected and the saga goes on. If the payment is declined the saga goes COMPENSATING, releases the seat and ends ABORTED. If a step hangs longer than SAGA_STEP_TIMEOUT (a review longer than SAGA_REVIEW_TIMEOUT) the sweeper aborts, refunds and releases. Every service talks to the others only through Kafka, never over HTTP.

```
SCORING --approved------------------------> RESERVING --seat_reserved--> CHARGING --charged--> COMPLETED
   | review_required                  ^        | failed                     | declined / timeout
   v                                  |        v                            v
IN_REVIEW --approved (by a person)----+     ABORTED <--seat_released-- COMPENSATING (REFUNDING first if it was charged)
   | rejected / timeout
   v
ABORTED  (SCORING blocked / timeout goes here too, with cancel_review sent to the risk service)
```

The risk rules are pure functions over counts taken from the risk database inside the scoring transaction: card_testing (4+ payments of at most 5.00 from one card in 10 minutes, 70 points), many_cards (3+ cards for one customer in 24 hours, 50), new_country (a known customer from a country not seen in 90 days, 30), amount_spike (more than 5 times the customer's average with 3+ earlier orders, 30), shared_ip (5+ customers from one IP in an hour, 40). 70 points and more is BLOCK, 30 and more is REVIEW, and any amount above RISK_REVIEW_LIMIT_CENTS (100.00) is at least REVIEW, so a big order is never approved automatically. Every decision stores its score, the rules that fired with a human readable detail and the rules version.

The agent takes the oldest pending case with a lease, looks at the history of the case's customer, card and IP through three read-only tools, and has to finish with a submit_recommendation call: approve or reject, confidence, a short summary and the order ids or rules it relies on. An id that no tool returned is sent back to the model as invalid. The recommendation lands on the case, the analyst sees it in sagactl review list and decides. The model is behind one interface with an OpenAI compatible client, so Groq, OpenRouter, Ollama or Gemini is a matter of LLM_BASE_URL and LLM_MODEL.

## Structure

cmd one main.go per binary, only wiring: config, build, run. internal/orchestrator the saga state machine (saga.go, pure, no I/O), the service, SQL, HTTP handlers, card hashing and the sweeper. internal/risk the risk participant: rules.go (pure scoring), store.go with the counting SQL and locks, service.go with handlers, review.go with the review case operations, evidence.go with history and leases for the agent, queue.go with the review queue gauges. internal/agent the investigator loop, its prompt and tools, the runner that works through the queue, and eval/ with the labelled dataset and the recorded answers. internal/llm the provider neutral chat client, the OpenAI compatible implementation, recording and replay. internal/fraudgen the labelled traffic plan and the precision and recall report. internal/inventory, internal/payment the participants, service.go with handlers and store.go with SQL. internal/messaging the contract between services: topics, message types, payloads, with golden files. internal/platform infrastructure with no business words in it: outbox, inbox, kafka consumer and DLQ, retry, config, telemetry, logging. migrations goose SQL, platform/ goes into every database. test/integration real Postgres and Kafka. test/load k6. deploy compose configs, k8s manifests, KEDA. docs/adr why the big decisions were made.

http calls the service, the service opens the transaction and calls store functions. The state machine and the rules return what to do and never do it. platform never imports a business package, so it could serve another saga as is.

## Edge case handling

Two orders for the same seat: a partial unique index allows only one HELD reservation per seat, so the second one gets a conflict, replies seat_reservation_failed and that saga ends ABORTED. The database decides, not the code.

Duplicate requests from the user: Idempotency-Key with a unique constraint and INSERT ON CONFLICT DO NOTHING. If the key already exists the existing order is returned as 200. Same key with a different body (any field, the card and IP included) is 422, not the old order.

The card number: hashed with HMAC-SHA256 and a secret key (CARD_HASH_KEY) at the HTTP edge. The number itself is never stored, never logged and never put into Kafka. A plain SHA-256 would not be enough, card numbers are few enough to brute force.

Card testing in parallel: six small payments from one card arrive at once and are scored by several risk workers. Each scoring transaction takes advisory locks on the card, the customer and the IP (sorted, so two transactions lock in the same order), so the counts are exact and exactly the 4th, 5th and 6th are blocked, whatever order they arrive in. Two batches that lock the same keys in a different order are a deadlock Postgres detects in a second, the losing batch is retried.

The same score command twice: the inbox catches a redelivery, but a re-sent command has a new message id. Scoring it again would count the order against itself and could turn ALLOW into BLOCK, so the stored decision is replied again instead.

Risk service down: the saga waits SAGA_STEP_TIMEOUT in SCORING and aborts. Fail closed, an order never goes through without a decision. Nothing was reserved or charged yet, so there is nothing to compensate.

Nobody reviews in time: after SAGA_REVIEW_TIMEOUT (30 minutes) the sweeper aborts the order and sends cancel_review, the risk service marks the case EXPIRED. A reviewer who comes later gets "already closed", so nobody approves an order the customer was told failed. A late approval that still reaches the orchestrator is ignored, the saga is terminal.

A seat taken while the order waits for review: the seat is reserved only after the approval, so a fraudster never holds a seat. If somebody else took it meanwhile the reservation fails and the saga ends ABORTED the usual way.

The agent: two replicas never take the same case (a lease taken with FOR UPDATE SKIP LOCKED). A model that is down, rate limited or keeps citing invented evidence leaves the case to the analyst, after 3 attempts the agent stops retrying it. The agent has no tool that changes anything. Everything that comes from the order (customer id, history) is wrapped in data blocks and the prompt says it is untrusted, an eval case has an instruction in the customer id.

Publishing from the database to Kafka: a row is marked published only after acks=all from Kafka. A crash in between means the row is published again, so delivery is at-least-once, not exactly-once. If only part of a batch was acknowledged only that prefix is marked. A failed shard backs off and retries forever, it never skips a row because the next one would get ahead of it.

Several relays on one outbox: the outbox is split into 16 shards by hash of the saga id. Each relay holds leases on its fair share of the shards and only the owner publishes a shard, in seq order. When a relay is added or dies the leases move, a crashed one loses its shards after the 15s lease TTL.

State change and its message: the outbox row is written in the same transaction as the business change, so either both exist or neither. Services never produce to Kafka directly. A review decision from sagactl is a transaction in the risk database with its reply in the outbox, the same way.

Message consuming: the Kafka key is the saga id, so one saga's messages are in one partition and in order (that is also why cancel_review always arrives after its score command). A worker takes a batch per partition and handles it in one database transaction together with inbox rows (consumer, message_id), so a redelivered message is a no-op. Offsets are committed only after that transaction commits, auto commit is off. If the batch fails it is replayed one record at a time to find the bad one. A bad payload or unknown type goes straight into topic.dlq with the error in headers. A transient error retries with exponential backoff and jitter, after the retries run out it goes to the DLQ only if the database is healthy. If the database is down the worker stops and doesn't commit, so nothing is dead-lettered because of an outage. On a rebalance a partly handled partition is rewound, never skipped.

Seat taken, payment declined or an order blocked are not errors, they are reply messages and the saga compensates or ends.

A worker hangs or is down: the sweeper finds sagas whose updated_at is older than SAGA_STEP_TIMEOUT with FOR UPDATE SKIP LOCKED (so several workers split them) and compensates. A charge that arrives after its refund hits the CANCELLED tombstone and is refused, a reservation after its release hits the RELEASED one, a review decision after the timeout hits EXPIRED. Every compensation is idempotent.

Database rollback: everything is one transaction, so on failure the order, the saga, the outbox row and the inbox row are all gone together and the message will be handled again.

Graceful shutdown: stop polling, finish the batch in flight, commit, release the leases, close the clients, all within SHUTDOWN_TIMEOUT, which is shorter than the Kubernetes grace period.

## Architecture changes

Own relay instead of Debezium: Debezium runs one task per database and needs Kafka Connect and a logical replication slot. The relay is a plain Go binary that scales up to 16 replicas per database.

Shards with leases instead of FOR UPDATE SKIP LOCKED in the relay: with SKIP LOCKED two relays can hold messages of one saga in different batches and publish them in the wrong order. Shards keep order per saga and still spread the load.

Polling instead of LISTEN/NOTIFY: NOTIFY was built and measured. A transaction with NOTIFY takes a database-wide lock on commit, so all order transactions committed one by one, HTTP p95 got 5 times worse and throughput fell from 150 to 94 orders/s. Rolled back, the relay polls every 200ms.

No retry topics: a message parked in a retry topic lets the next message of the same saga go ahead of it. Retries happen in place, the partition waits.

Inbox in the same transaction instead of only idempotent handlers: the handler and the dedup row commit together, so a crash between them can't happen.

Sweeper owns the timeout: the first version stored a deadline, the first one written by the API and the next ones by the worker, each with its own config. On Kubernetes the two configs drifted apart and 541 of 600 sagas were compensated for nothing. Now only the sweeper decides, from updated_at. The review timeout went the same way, a second duration in the same sweeper, not a deadline column.

Risk as one more saga participant instead of a synchronous call in the API: real payment systems often score inside the request with a ~100ms budget, which is faster for the customer but makes the API depend on the risk service being up. Here it goes through the outbox like every other step, survives a risk outage without losing orders and keeps the API free of cross-service calls. The price is latency, the order is answered 202 and checked a moment later.

Scoring before reserving, not after: a blocked order never holds a seat and needs no compensation, scoring changes nothing outside the risk database.

Fail closed: with the risk service down every order aborts. Losing some sales is cheaper than chargebacks, and the amount limit makes sure even a "small" order can't sneak through unchecked.

Counters in Postgres instead of Redis: COUNT over (card_hash, created_at) and similar indexes is enough at this size and keeps the counts in the same transaction as the decision, so locks make them exact. Redis or pre-aggregated buckets would be the next step under real load.

Rules instead of ML: there are no labelled data, every decision has to be explained to the analyst and the customer, and rules are what a first antifraud layer usually is.

The agent only advises: a wrong LLM answer then costs an analyst a minute, not money. Its tools are bound to the case (no arguments), so it can't read other customers' data, and its final answer is a tool call with a JSON schema, because Groq can't combine structured outputs with tools.

One OpenAI compatible client instead of a provider SDK: Groq, OpenRouter, Ollama and Gemini all speak /chat/completions, so switching is configuration and there is no new dependency. CI runs the agent on recorded answers, so it needs no key and no network.

cancel_review doesn't reply: every other compensation replies, but the saga is already ABORTED when it sends cancel_review and nobody waits for an answer.

Kafka key = saga id and partitions fixed at 12: adding partitions moves keys to other partitions, so it needs in-flight sagas drained first.

UUIDv7 for ids: they are time ordered, so primary key inserts don't scatter over the index.

Workers scale by consumer lag with KEDA, not CPU: a worker waiting on the database has low CPU and a big lag. The API scales by CPU with HPA.

PgBouncer in transaction mode for the API and workers: every transaction lives on one server connection and the janitor lock is transaction scoped, so it is safe. Relays connect directly to keep their lease timing predictable. An optional compose override.

## Tests and tools

pgx for Postgres, franz-go for Kafka (cooperative sticky rebalancing, idempotent producer), goose for migrations, OpenTelemetry for traces and metrics, slog for JSON logs with trace_id, testcontainers for integration tests, k6 for load. Stdlib testing only, fakes are written by hand. The LLM client is plain net/http, no SDK.

Unit tests cover every saga transition and timeout, every risk rule and threshold (the over limit rule included), retry and backoff, the consumer algorithm (batch ok, poison in the middle, DLQ down so no commit, database outage) with fake processors, relay lease math, message contracts with golden files, HTTP and card hashing, the LLM client wire format and rate limit retries, and the agent loop with a scripted model (cited evidence, invented evidence sent back, step and token limits, untrusted text kept out of the system prompt). 43 integration scenarios run every component in process against real Postgres and Kafka: outbox atomicity, relay order and crash takeover, duplicates, poison, a real database outage, rebalance in the middle of a retry, the full saga happy and failing paths, timeouts with refunds and tombstones, concurrent sweepers, the DLQ list, fix and replay cycle, card testing scored in parallel, review approved, rejected and timed out, risk down, a re-sent score command, and the agent recommending without deciding, splitting the queue between replicas and leaving failed cases to people. Coverage is 81.7% from unit and integration together.

Critical tests are mutation checked: the logic under test is broken on purpose and the test has to go red. For the risk part 7 mutations were all caught: no advisory lock (4 confirmed instead of 3), no decision replay, cancel that doesn't expire, no review timeout, no agent lease, invented evidence accepted, over limit not forcing review. Running for real found bugs that tests didn't: a rebalance that skipped a half handled partition, a 5s metadata cache hiding in a 200ms poll, alerts that never fired because the counter was born with its first event, the deadline drift above.

The rules against labelled traffic (fraudgen, seed 1, 457 orders on the compose stack): precision 85.1%, recall 55.3%, 2.8% of legit orders sent to review and none blocked. Recall is low on purpose of the design, not by accident: a velocity rule fires only after its threshold, so the first 3 card tests, the first 2 stolen cards and the first 4 farm accounts pass. The data is synthetic and the rules were written knowing the scenarios, so the numbers show the mechanics work, not how good the rules are on real fraud.

The agent on the eval set (openai/gpt-oss-120b on Groq, 21 cases): 18/21 on the recorded run, 0 failures, about 4.4k tokens and 3.4 steps per case. The first run was 19/20 but approved the case with an instruction in the customer id. A control case with the same data and a plain id was approved too, so it was the reasoning ("the earlier cards passed, so it's normal"), not the injection. The prompt now says an earlier ALLOW isn't proof, both cases went 12 of 12 after that, and a full run then missed 3 others. Run to run noise of this model is about that size, so single runs of 21 cases can't rank prompts.

Checked by hand on compose and on kind: killed and stopped workers and relays, a stopped broker, a drained node, KEDA scaling 2 to 12 and back. After each run the counts were reconciled: confirmed orders = held seats = charged payments, 0 double charges. Load: 150 orders/s sustained on one laptop, HTTP p95 86ms, saga end to end p95 about 2s (measured before the risk step).

CI: vet, unit tests with -race and golangci-lint, then integration tests with coverage, then the Docker image, compose config and rendered k8s manifests.

## What is missing for prod grade code, cause it is presonal isolated project

No authentication on the API, and no auth for reviewers: anybody with the risk DSN can approve. The payment is a fake with a limit, a real provider needs its own idempotency keys. Postgres is a single instance with no replicas or backups. Alerts are defined but there is no Alertmanager routing, nobody gets paged. DSNs and the card hash key are plain env values, no secret manager and no key rotation. The relay lease is a time based fence, a paused relay can publish duplicates after losing its lease (the inbox absorbs them). Shard and partition counts are fixed, changing them needs a drain. CI does not run the compose smoke test or k6, and the load numbers above are from before the risk step.

For the risk part: the country comes from the request, there is no geo IP lookup. risk_events grow forever, there is no retention janitor yet. There is no feedback loop from chargebacks into labels, so the rules are never measured on real outcomes. The agent eval is 21 synthetic cases on one model with no repeated runs, and the agent sends order data to a third party LLM, which is fine for synthetic data and not for real customers. The agent wasn't run on kind, only its manifest was rendered.
