# Charge point emulator

A minimal OCPP 1.6J charge point written in Python. It connects to the
CSMS with the `ocpp1.6` subprotocol, sends `BootNotification`, and then
sends `Heartbeat` at the interval the server returns. Every sent (`>>>`)
and received (`<<<`) frame is printed.

## Setup

Requires Python 3.10+.

```bash
cd emulator
python -m venv .venv
# Windows:      .venv\Scripts\activate
# Linux/macOS:  source .venv/bin/activate
pip install -r requirements.txt
```

## Run

Start the server first (`./gradlew bootRun` in the repo root), then:

```bash
python charge_point.py                                  # ws://localhost:8080/ocpp/CP001
python charge_point.py ws://localhost:8080/ocpp/CP042   # different charge point id
```

Stop with `Ctrl+C`.

> Until `parse` is implemented on the server, the server fails while handling
> the first message and closes the connection. The handshake still succeeds,
> so the emulator prints `Connected, negotiated subprotocol: ocpp1.6`.
