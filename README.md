# EventBridge Hub

Portfolio reference implementation of a 2021 project, built to show reliable event-driven integration between an order service and downstream inventory, with no dual writes and no double-applied events.

**Stack:** Java 17, Spring Boot 2.7, Spring Kafka, Debezium 1.6 (Postgres CDC + outbox event router), PostgreSQL 13, Flyway, Micrometer/Prometheus.

## The problem
"Save the order, then publish to Kafka" fails in two ways:
- If the process dies between the two steps, the event is lost.
- If the publish succeeds but the transaction rolls back, a phantom event goes out.

On the consumer side, Kafka delivers at least once, so a naive handler reserves stock twice after a rebalance.

## The design
```
OrderService ──tx──▶ orders + outbox row          (one Postgres transaction)
                          │ WAL
                    Debezium (pgoutput) + EventRouter SMT
                          ▼
                 Kafka topic  order.events   key = order id (per-order ordering)
                          ▼
InventoryHandler ──tx──▶ inbox claim + inventory update + outbox(InventoryReserved | InventoryShortage)
```
- **Transactional outbox** (`outbox/Outbox.java`):
  - The event row is written in the same transaction as the business change.
  - `Propagation.MANDATORY` makes calling it outside a transaction an error, not a silent bug.
  - Debezium tails the WAL, so there's no polling and the outbox adds no latency.
- **Debezium EventRouter** (`connect/order-outbox-connector.json`):
  - Routes `aggregate_type` to the `<type>.events` topic.
  - The Kafka key is `aggregate_id`, so ordering is per order.
  - The outbox row id and event type go into headers.
- **Idempotent consumer / inbox** (`inbox/Inbox.java`):
  - `(consumer, message_id)` is claimed *inside* the handler's transaction.
  - If the handler fails, the claim rolls back and the retry runs again. If it commits, every redelivery after that is a no-op.
- **All-or-nothing reservations:**
  - Rows are updated in SKU order to avoid deadlocks.
  - Any shortage rolls back the partial reservations, then records an `InventoryShortage` event in a fresh transaction.
  - Cancels release stock. A double cancel emits no second event.
- **Errors** (`config/KafkaErrorHandling.java`):
  - Transient failures (for example a DB failover) get blocking exponential-backoff retries for about 5 minutes, which preserves partition ordering.
  - After that the record goes to `<topic>.DLT` with the exception headers.
  - Malformed or undeserializable records skip retries entirely.
- **Delivery settings:**
  - Producer: `acks=all`, idempotence on.
  - Consumer: `read_committed`, manual per-record acks.
  - Metrics for processed, duplicate and shortage events.

## Run
```bash
docker compose up -d
curl -X POST localhost:8083/connectors -H 'Content-Type: application/json' -d @connect/order-outbox-connector.json
mvn spring-boot:run
curl -X POST localhost:8080/api/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"c-1","totalCents":4200,"lines":[{"sku":"TEE-M","qty":2}]}'
```

## Test
```bash
mvn test
```
The test replays outbox rows as the records Debezium would emit, including duplicate deliveries. It checks:
- One event per change, and none on rollback.
- Exactly-once effects under redelivery.
- Shortage rollback.
- Idempotent cancel.
- Poison-message rejection.
