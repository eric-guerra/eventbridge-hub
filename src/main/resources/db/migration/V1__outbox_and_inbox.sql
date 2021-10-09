-- Producer side: business rows and the outbox row are written in the same transaction.
-- Debezium tails the WAL for the outbox table and routes each row to Kafka; nothing polls.
CREATE TABLE orders (
    id          UUID PRIMARY KEY,
    customer_id VARCHAR(40)   NOT NULL,
    status      VARCHAR(20)   NOT NULL,
    total_cents BIGINT        NOT NULL,
    created_at  TIMESTAMP     NOT NULL,
    version     INT           NOT NULL DEFAULT 0
);

CREATE TABLE order_lines (
    order_id UUID        NOT NULL REFERENCES orders (id),
    sku      VARCHAR(40) NOT NULL,
    qty      INT         NOT NULL,
    PRIMARY KEY (order_id, sku)
);

CREATE TABLE outbox (
    id             UUID         PRIMARY KEY,
    aggregate_type VARCHAR(40)  NOT NULL,  -- routes to topic  "<aggregate_type>.events"
    aggregate_id   VARCHAR(64)  NOT NULL,  -- becomes the Kafka key → per-aggregate ordering
    event_type     VARCHAR(80)  NOT NULL,
    payload        TEXT         NOT NULL,
    created_at     TIMESTAMP    NOT NULL
);

-- Consumer side: the inbox makes handlers idempotent. A message id is recorded in the same
-- transaction as the handler's side effects, so a redelivery is detected and skipped.
CREATE TABLE inbox (
    consumer     VARCHAR(60) NOT NULL,
    message_id   VARCHAR(64) NOT NULL,
    processed_at TIMESTAMP   NOT NULL,
    PRIMARY KEY (consumer, message_id)
);

CREATE TABLE inventory (
    sku       VARCHAR(40) PRIMARY KEY,
    on_hand   INT NOT NULL,
    reserved  INT NOT NULL DEFAULT 0,
    CONSTRAINT ck_reserved CHECK (reserved >= 0 AND reserved <= on_hand)
);

CREATE TABLE reservations (
    order_id UUID        NOT NULL,
    sku      VARCHAR(40) NOT NULL,
    qty      INT         NOT NULL,
    PRIMARY KEY (order_id, sku)
);
