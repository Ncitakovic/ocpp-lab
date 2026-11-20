"""Minimal OCPP 1.6J charge point emulator.

Connects to the CSMS, sends BootNotification and then Heartbeat at the
interval returned by the server. Every sent and received frame is printed.

Usage:
    python charge_point.py [ws://localhost:8080/ocpp/CP001]
"""

import asyncio
import json
import sys
import uuid
from datetime import datetime, timezone

import websockets

DEFAULT_URL = "ws://localhost:8080/ocpp/CP001"
SUBPROTOCOL = "ocpp1.6"

CALL, CALL_RESULT, CALL_ERROR = 2, 3, 4


def log(direction: str, frame: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"{ts} {direction} {frame}", flush=True)


class OcppError(Exception):
    pass


async def call(ws, action: str, payload: dict) -> dict:
    """Send a CALL and wait for the matching CALLRESULT / CALLERROR."""
    message_id = str(uuid.uuid4())
    frame = json.dumps([CALL, message_id, action, payload])
    log(">>>", frame)
    await ws.send(frame)

    while True:
        raw = await ws.recv()
        log("<<<", raw)
        msg = json.loads(raw)

        if msg[0] == CALL:
            # Server-initiated calls are not supported yet; answer politely.
            reply = json.dumps([CALL_ERROR, msg[1], "NotImplemented", "Emulator does not handle CALLs yet", {}])
            log(">>>", reply)
            await ws.send(reply)
            continue

        if msg[1] != message_id:
            print(f"    (ignoring response for unknown id {msg[1]})")
            continue

        if msg[0] == CALL_RESULT:
            return msg[2]
        if msg[0] == CALL_ERROR:
            raise OcppError(f"{action} failed: {msg[2]} - {msg[3]}")


async def run(url: str) -> None:
    print(f"Connecting to {url} with subprotocol {SUBPROTOCOL}")
    async with websockets.connect(url, subprotocols=[SUBPROTOCOL]) as ws:
        if ws.subprotocol != SUBPROTOCOL:
            print(f"Server did not accept {SUBPROTOCOL} (got {ws.subprotocol!r}), giving up")
            return
        print(f"Connected, negotiated subprotocol: {ws.subprotocol}")

        while True:
            boot = await call(ws, "BootNotification", {
                "chargePointVendor": "OcppLab",
                "chargePointModel": "Emulator-1",
            })
            interval = int(boot.get("interval", 60)) or 60
            if boot.get("status") == "Accepted":
                break
            print(f"BootNotification status {boot.get('status')}, retrying in {interval}s")
            await asyncio.sleep(interval)

        print(f"Boot accepted, sending Heartbeat every {interval}s (Ctrl+C to stop)")
        while True:
            await asyncio.sleep(interval)
            await call(ws, "Heartbeat", {})


def main() -> None:
    url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL
    try:
        asyncio.run(run(url))
    except KeyboardInterrupt:
        print("\nStopped")
    except websockets.ConnectionClosed as e:
        print(f"Connection closed: code={e.rcvd.code if e.rcvd else None} reason={e.rcvd.reason if e.rcvd else ''}")
    except OSError as e:
        print(f"Could not connect to {url}: {e}")


if __name__ == "__main__":
    main()
