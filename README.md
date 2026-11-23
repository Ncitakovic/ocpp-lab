# ocpp-lab

A small OCPP 1.6J Central System (CSMS) built for learning. Charge points
connect over WebSocket and can run a full charging session:
Boot → Authorize → StartTransaction → MeterValues → StopTransaction.
Sessions and meter readings are stored in Postgres.

Stack: Kotlin, Spring Boot 3, Java 21, Gradle (Kotlin DSL), Jackson,
Spring Data JDBC, Flyway, Postgres 16, JUnit 5 and Testcontainers.
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

## Layout

```
src/main/kotlin/dev/ocpplab/
  OcppLabApplication.kt        Spring Boot entry point
  OcppConfig.kt                Clock and MessageParser beans
  protocol/                    OCPP-J framing: messages, error codes, parser, serializer
  dispatch/                    MessageDispatcher, request payloads, connector status registry
  session/                     charging session entities, repositories, SessionService
  ws/                          WebSocket endpoint, handshake and subprotocol handling
src/main/resources/db/migration/   Flyway SQL migrations
src/test/kotlin/dev/ocpplab/   unit tests and Testcontainers integration tests
emulator/                      Python charge point emulator
docker-compose.yml             local Postgres 16
```

## Running

### 1. Database

```bash
docker compose up -d       # Postgres 16 on localhost:5433, db/user/password: ocpplab
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
  -c "select * from charging_session" -c "select * from meter_value"
```

More options are in [emulator/README.md](emulator/README.md).

## Tests

Docker must be running: the integration tests start Postgres with Testcontainers.

```bash
./gradlew test             # all tests
./gradlew test --tests "dev.ocpplab.session.*"
./gradlew build            # compile + test + jar
```

> **Work in progress:** `SessionServiceTest$StopSession` fails until these are implemented:
> - `JdbcSessionService.stopSession` (`src/main/kotlin/dev/ocpplab/session/JdbcSessionService.kt`)
> - `ChargingSessionRepository.closeIfOpen` (`src/main/kotlin/dev/ocpplab/session/Repositories.kt`), as a single conditional UPDATE
>
> Until then the server also fails on `StopTransaction`. Everything else must pass.
> To build without running tests, use `./gradlew build -x test`.
