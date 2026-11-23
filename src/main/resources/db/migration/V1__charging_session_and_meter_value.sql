CREATE TABLE charging_session
(
    -- OCPP 1.6 transactionId is a 32-bit integer, so SERIAL (int4) rather than BIGSERIAL.
    transaction_id  SERIAL PRIMARY KEY,
    charge_point_id VARCHAR(255) NOT NULL,
    connector_id    INTEGER      NOT NULL,
    id_tag          VARCHAR(20)  NOT NULL, -- IdToken is CiString20Type
    meter_start     INTEGER      NOT NULL, -- Wh
    meter_stop      INTEGER,               -- Wh, set when the session is closed
    started_at      TIMESTAMPTZ  NOT NULL,
    stopped_at      TIMESTAMPTZ,
    stop_reason     VARCHAR(32),
    status          VARCHAR(6)   NOT NULL,
    CONSTRAINT charging_session_status_chk CHECK (status IN ('OPEN', 'CLOSED'))
);

CREATE TABLE meter_value
(
    id             BIGSERIAL PRIMARY KEY,
    transaction_id INTEGER     NOT NULL REFERENCES charging_session (transaction_id),
    timestamp      TIMESTAMPTZ NOT NULL,
    value_wh       NUMERIC     NOT NULL
);

CREATE INDEX meter_value_transaction_id_idx ON meter_value (transaction_id);
