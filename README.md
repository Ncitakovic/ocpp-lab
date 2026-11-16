# ocpp-lab

A small OCPP 1.6J Central System (CSMS) built for learning. Charge points
connect over WebSocket, and the server answers `BootNotification` and
`Heartbeat`. Any other action gets a `NotImplemented` error.

Stack: Kotlin, Spring Boot 3, Java 21, Gradle (Kotlin DSL), Jackson, JUnit 5.
The charge point emulator is written in Python 3 with `websockets`.

## How it works

```
charge point ──ws://host:8080/ocpp/{chargePointId}──▶ OcppWebSocketHandler
   (subprotocol "ocpp1.6")                                │ raw text frame
                                                          ▼
                                               MessageDispatcher
                                                 │ parse(raw)  ← protocol/Parser.kt
                                                 │ BootNotification / Heartbeat / ...
                                                 ▼
                                      CallResult / CallError ──toJson──▶ back to the charge point
```

- **Subprotocol**: only `ocpp1.6` is accepted. If the client does not offer it,
  the handshake completes without a `Sec-WebSocket-Protocol` header and the
  server immediately closes the connection with 1002 (OCPP-J 1.6, section 3.2).
- **Charge point id** is the last path segment and is stored in the session attributes.
- **Time** comes from an injected `java.time.Clock` bean, so tests use `Clock.fixed(...)`.

## Layout

```
src/main/kotlin/dev/ocpplab/
  OcppLabApplication.kt        Spring Boot entry point
  OcppConfig.kt                Clock and MessageParser beans
  protocol/                    OCPP-J framing: messages, error codes, parser, serializer
  dispatch/MessageDispatcher   handles CALLs and builds responses
  ws/                          WebSocket endpoint, handshake and subprotocol handling
src/test/kotlin/dev/ocpplab/   unit and integration tests
emulator/                      Python charge point emulator
```

## Running the server

```bash
./gradlew bootRun          # Windows: gradlew.bat bootRun
```

The server listens on `ws://localhost:8080/ocpp/{chargePointId}`.

## Tests

```bash
./gradlew test             # all tests
./gradlew test --tests "dev.ocpplab.dispatch.*"
./gradlew build            # compile + test + jar
```

> **`ParserTest` fails on purpose** until `parse` in
> `src/main/kotlin/dev/ocpplab/protocol/Parser.kt` is implemented. It is
> currently `TODO("Nikola implementira")`, so `./gradlew build` reports
> failures for `ParserTest` only. Everything else must pass. To build
> without running tests, use `./gradlew build -x test`.

## Emulator

See [emulator/README.md](emulator/README.md). In short:

```bash
cd emulator
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python charge_point.py                          # ws://localhost:8080/ocpp/CP001
```
