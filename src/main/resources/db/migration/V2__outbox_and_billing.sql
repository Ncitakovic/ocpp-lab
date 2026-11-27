-- Transactional outbox: a row is written in the same transaction as the session change,
-- and OutboxRelay later publishes it to Kafka.
CREATE TABLE outbox_event (
    id            UUID         PRIMARY KEY,           -- event id; the consumer deduplicates on it
    -- Insertion order for the relay. Note: a smaller seq can commit later than a larger one
    -- (parallel transactions), so seq is not a global commit order. See README "Limitations".
    seq           BIGSERIAL    NOT NULL UNIQUE,
    aggregate_id  VARCHAR(64)  NOT NULL,              -- transactionId; also the Kafka message key
    type          VARCHAR(64)  NOT NULL,              -- SessionOpened / SessionClosed
    payload       JSONB        NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL,
    published_at  TIMESTAMPTZ                         -- NULL until the broker acknowledged the send
);

-- The relay only ever looks for unpublished rows, so the index covers just those.
CREATE INDEX outbox_event_unpublished_idx ON outbox_event (seq) WHERE published_at IS NULL;

-- Billing consumer side. No foreign keys to charging_session on purpose: billing only knows
-- what it received from Kafka and could live in a separate service with its own database.

-- Every event the billing consumer has handled (idempotent consumer).
CREATE TABLE processed_event (
    event_id      UUID         PRIMARY KEY,
    processed_at  TIMESTAMPTZ  NOT NULL
);

CREATE TABLE billing_record (
    id               BIGSERIAL      PRIMARY KEY,
    transaction_id   INT            NOT NULL UNIQUE,  -- second safety net: one bill per session
    event_id         UUID           NOT NULL,         -- the SessionClosed event that produced it
    charge_point_id  VARCHAR(255)   NOT NULL,
    id_tag           VARCHAR(20)    NOT NULL,
    energy_wh        BIGINT         NOT NULL,         -- meterStop - meterStart
    price_per_kwh    NUMERIC(12, 4) NOT NULL,
    amount           NUMERIC(12, 4) NOT NULL,         -- energy_wh / 1000 * price_per_kwh, HALF_UP to 4 decimals
    billed_at        TIMESTAMPTZ    NOT NULL
);
