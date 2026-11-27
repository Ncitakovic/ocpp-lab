# ocpp-lab

A small OCPP 1.6J Central System (CSMS) built for learning. Charge points
connect over WebSocket and can run a full charging session:
Boot → Authorize → StartTransaction → MeterValues → StopTransaction.
Sessions and meter readings are stored in Postgres. Session events go to Kafka
through a transactional outbox, and a billing consumer turns each closed
session into exactly one bill.

Stack: Kotlin, Spring Boot 3, Java 21, Gradle (Kotlin DSL), Jackson,
Spring Data JDBC, Flyway, Postgres 16, Kafka (spring-kafka), JUnit 5,
Testcontainers and Awaitility.
The charge point emulator is written in Python 3 with `websockets`.

## How it works

```
charge point ──ws://host:8080/ocpp/{chargePointId}──▶ OcppWebSocketHandler
   (subprotocol "ocpp1.6")                                │ raw text frame
                                                          ▼
                                               MessageDispatcher
                                                 │ parse(raw)           ← protocol/Parser.kt
                                                 │ BootNotification, Heartbeat, Authorize,
                                                 │ StatusNotification   → ConnectorStatusRegistry (memory)
                                                 │ StartTransaction, MeterValues,
                                                 │ StopTransaction      → SessionService → Postgres
                                                 ▼
                                      CallResult / CallError ──toJson──▶ back to the charge point
```

- **Subprotocol**: only `ocpp1.6` is accepted. If the client does not offer it,
  the handshake completes without a `Sec-WebSocket-Protocol` header and the
  server immediately closes the connection with 1002 (OCPP-J 1.6, section 3.2).
- **Authorize** always answers `Accepted` (good enough for the lab).
- **StopTransaction** always gets a normal `CallResult`, even for a duplicate or an
  unknown `transactionId`. Otherwise the charge point keeps resending it.
- **Time** comes from an injected `java.time.Clock` bean, so tests use `Clock.fixed(...)`.

### Persistence

Spring Data JDBC rather than JPA: no persistence context, lazy loading or dirty
checking, so what you `save` is exactly what is written. A conditional
`@Modifying @Query("UPDATE ... WHERE status = 'OPEN'")` returns the number of
updated rows directly. Flyway migrations are in `src/main/resources/db/migration`.

| Table | Columns |
|---|---|
| `charging_session` | `transaction_id` (SERIAL PK), `charge_point_id`, `connector_id`, `id_tag`, `meter_start`, `meter_stop`, `started_at`, `stopped_at`, `stop_reason`, `status` (`OPEN` / `CLOSED`) |
| `meter_value` | `id`, `transaction_id` (FK), `timestamp`, `value_wh` |
| `outbox_event` | `id` (UUID, the event id), `seq` (BIGSERIAL), `aggregate_id` (transactionId), `type`, `payload` (JSONB), `created_at`, `published_at` |
| `processed_event` | `event_id` (PK), `processed_at` |
| `billing_record` | `id`, `transaction_id` (UNIQUE), `event_id`, `charge_point_id`, `id_tag`, `energy_wh`, `price_per_kwh`, `amount` (NUMERIC(12,4)), `billed_at` |

### Events: transactional outbox → Kafka → billing

```
charge point ──StopTransaction──▶ CSMS (MessageDispatcher → SessionService.stopSession)
                                    │  ONE database transaction:
                                    │    UPDATE charging_session ... WHERE status = 'OPEN'   (closeIfOpen)
                                    │    INSERT INTO outbox_event (SessionClosed)            only if 1 row updated
                                    ▼
                                 outbox_event (published_at IS NULL)
                                    │
                         OutboxRelay (@Scheduled, every 500 ms)
                                    │  oldest first (ORDER BY seq)
                                    │  send(key = transactionId) → wait for ack (acks=all, 10 s timeout)
                                    │  then UPDATE published_at
                                    ▼
                     Kafka topic charging-session-events (3 partitions)
                                    │
                         BillingListener (group "billing", manual ack)
                                    │  BillingService.bill(), ONE transaction:
                                    │    processed_event (event_id)  ─ already there? skip
                                    │    billing_record  (amount = energy kWh × price)
                                    │  commit, THEN ack the offset
                                    ▼
                                 billing_record
```

`StartTransaction` writes a `SessionOpened` event the same way. Billing ignores it.

**Why an outbox instead of sending to Kafka directly.** A direct send from
`stopSession` has two failure modes, and both are bad:

- Send first, then commit: the commit can fail, and Kafka has an event for a
  session that never closed.
- Commit first, then send: the process can die in between, and the event is
  lost forever. The session is closed but never billed.

With the outbox, the session change and the event are one database commit.
Either both exist or neither does. Sending is a separate, retryable step: a row
stays unpublished until the broker acknowledged it.

**Guarantee: at-least-once delivery + idempotent consumer.**

- The relay sets `published_at` only after the ack. If it crashes between the ack
  and the UPDATE, the row is sent again. Duplicates are possible, loss is not.
- Every copy carries the same `eventId` (the outbox row id). The consumer
  records it in `processed_event` in the same transaction as the `billing_record`,
  and skips an event id it has already seen.
- The offset is committed only after that transaction commits. A crash before the
  commit means redelivery, and a crash after it means the redelivery is skipped.
- `UNIQUE (transaction_id)` on `billing_record` is a second safety net. If two
  different event ids ever arrive for the same session, the second one hits the
  constraint (`INSERT ... ON CONFLICT (transaction_id) DO NOTHING`). `BillingService`
  treats that as "already billed": it logs a warning, keeps the event id in
  `processed_event` and returns `AlreadyProcessed`, and the listener acknowledges.
  It is not retried forever, so it cannot block the partition.
- Deduplication is a single `INSERT ... ON CONFLICT DO NOTHING` on `processed_event`,
  not `exists()` followed by an insert. Two copies of the same event processed at the
  same moment therefore cannot both bill.

The result is effectively-once billing, without Kafka transactions.

Producer settings: `acks=all`, `enable.idempotence=true` (no duplicates from the
producer's own retries). Message key = `transactionId`, so all events of one
session land in the same partition, in order.

**Limitations (lab scope).**

- The relay is meant to run on **one instance**. Two relays would both read the
  same unpublished rows and send them twice. That is still correct, because the
  consumer deduplicates, but it is wasteful. Running several instances would need
  `SELECT ... FOR UPDATE SKIP LOCKED` or leader election.
- `seq` is the **insertion** order, not the commit order. With parallel
  transactions, a row with a smaller `seq` can commit after one with a larger `seq`,
  so the relay may publish them in a different order than `seq` suggests. That is
  fine here: ordering only matters per key, and `SessionOpened` and `SessionClosed`
  of one session are always written in two consecutive transactions
  (StartTransaction, then later StopTransaction).
- A message that keeps failing for another reason (e.g. the database is down) is
  retried every second, forever. That blocks its partition until the cause is fixed,
  which is the price of not losing a bill.
- No Kafka volume in `docker-compose.yml`: `docker compose down` drops topics and
  consumer offsets.

## Layout

```
src/main/kotlin/dev/ocpplab/
  OcppLabApplication.kt        Spring Boot entry point
  OcppConfig.kt                Clock and MessageParser beans
  protocol/                    OCPP-J framing: messages, error codes, parser, serializer
  dispatch/                    MessageDispatcher, request payloads, connector status registry
  session/                     charging session entities, repositories, SessionService
  events/                      SessionOpened / SessionClosed and the Kafka envelope
  outbox/                      outbox table access, OutboxRelay, topic and scheduler config
  billing/                     BillingListener (Kafka consumer), BillingService, repositories
  ws/                          WebSocket endpoint, handshake and subprotocol handling
src/main/resources/db/migration/   Flyway SQL migrations
src/test/kotlin/dev/ocpplab/   unit tests and Testcontainers integration tests
emulator/                      Python charge point emulator
docker-compose.yml             local Postgres 16 and a single KRaft Kafka broker
```

## Running

### 1. Database and Kafka

```bash
docker compose up -d       # Postgres 16 on localhost:5433 (db/user/password: ocpplab), Kafka on localhost:9092
docker compose down        # stop (data is kept in the ocpplab-data volume)
docker compose down -v     # stop and delete the data
```

The database uses host port **5433**, not the default 5432, so it does not clash
with another local Postgres. Inside the container it still listens on 5432.

### 2. Server

```bash
./gradlew bootRun          # Windows: gradlew.bat bootRun
```

Flyway creates the tables on startup. The server listens on
`ws://localhost:8080/ocpp/{chargePointId}`.

### 3. A charging session from the emulator

```bash
cd emulator
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python charge_point.py session                  # one full session
python charge_point.py session --duplicate-stop # StopTransaction sent twice
python charge_point.py                          # just Boot + Heartbeat
```

To see the result in the database:

```bash
docker exec -it ocpplab-postgres psql -U ocpplab -d ocpplab \
  -c "select * from charging_session" -c "select * from meter_value" \
  -c "select seq, type, aggregate_id, published_at from outbox_event" \
  -c "select * from billing_record"
```

To watch the events on the topic:

```bash
docker exec -it ocpplab-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic charging-session-events --from-beginning --property print.key=true
```

More options are in [emulator/README.md](emulator/README.md).

## Tests

Docker must be running: the integration tests start Postgres and Kafka with Testcontainers.

```bash
./gradlew test --rerun-tasks   # all tests
./gradlew test --tests "dev.ocpplab.session.*"
./gradlew build            # compile + test + jar
```

What is covered:

- **Parser**: valid CALL / CALLRESULT / CALLERROR, broken JSON, wrong element counts, wrong field types, id length.
- **Dispatcher**: every supported action, with a fixed `Clock` and a fake `SessionService`.
- **Sessions** (real Postgres): open, meter values, close, duplicate and unknown StopTransaction,
  and two identical StopTransactions at the same moment closing the session exactly once.
  Closing is a single conditional `UPDATE ... WHERE status = 'OPEN'`, so only one caller can win.
- **WebSocket**: subprotocol negotiation on a real port.
- **Outbox → Kafka** (real Postgres + Kafka, relay called directly, Awaitility instead of sleeps):
  closing a session gives exactly one event with key = transactionId; two concurrent identical
  StopTransactions give one outbox row and one event; duplicate or unknown stops write nothing;
  a relay "crash" after the send but before `published_at` resends the event, and billing still
  has one record.
- **Billing**: the same message delivered twice gives one `billing_record`; two different event ids
  for one session give one record, and the consumer keeps going; amount rounding (`HALF_UP`, 4 decimals).

To build without running tests, use `./gradlew build -x test`.

## Security note

The `ocpplab` username and password in `docker-compose.yml` and `application.properties`
are for the local development database only. Do not reuse them anywhere else.

## License

[MIT](LICENSE)
