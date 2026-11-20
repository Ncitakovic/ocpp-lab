# Charge point emulator

A minimal OCPP 1.6J charge point written in Python. It connects to the
CSMS with the `ocpp1.6` subprotocol. Every sent (`>>>`) and received
(`<<<`) frame is printed.

## Setup

Requires Python 3.10+.

```bash
cd emulator
python -m venv .venv
# Windows:      .venv\Scripts\activate
# Linux/macOS:  source .venv/bin/activate
pip install -r requirements.txt
```

## Commands

Start the database and the server first (see the root README), then:

### `heartbeat` (default)

`BootNotification`, then `Heartbeat` at the interval the server returns. Stop with `Ctrl+C`.

```bash
python charge_point.py
python charge_point.py heartbeat --url ws://localhost:8080/ocpp/CP042
```

### `session`

One full charging session, then the emulator exits:

```
BootNotification
StatusNotification  Preparing
Authorize
StartTransaction    meterStart = 1000 Wh   -> server returns transactionId
MeterValues x3      2500, 4000, 5500 Wh    (Energy.Active.Import.Register)
StopTransaction     meterStop = 5500 Wh, reason EVDisconnected
StatusNotification  Available
```

```bash
python charge_point.py session
python charge_point.py session --duplicate-stop      # send the same StopTransaction twice
python charge_point.py session --id-tag MYTAG --connector 2 --meter-interval 0.2
```

`--duplicate-stop` resends the identical StopTransaction payload with a new
message id, the way a charge point retries when it thinks the first one was
lost. The server must answer both with a normal `CallResult` and close the
session only once.

| Option | Default | Meaning |
|---|---|---|
| `--url` | `ws://localhost:8080/ocpp/CP001` | CSMS endpoint; the last path segment is the charge point id |
| `--id-tag` | `TAG-0001` | idTag for Authorize / Start / Stop |
| `--connector` | `1` | connector id |
| `--meter-interval` | `1` | seconds between MeterValues |
| `--duplicate-stop` | off | send StopTransaction twice |

> Until `SessionService.stopSession` is implemented, the server fails on
> `StopTransaction` and closes the connection, so the session command stops there.
