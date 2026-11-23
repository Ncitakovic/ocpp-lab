"""Minimal OCPP 1.6J charge point emulator.

Commands:
  heartbeat (default)  BootNotification, then Heartbeat at the interval returned by the server.
  session              One full charging session:
                       Boot -> StatusNotification Preparing -> Authorize -> StartTransaction
                       -> 3x MeterValues -> StopTransaction -> StatusNotification Available

Every sent (>>>) and received (<<<) frame is printed.

Usage:
    python charge_point.py [heartbeat|session] [--url URL] [--duplicate-stop]
"""

import argparse
import asyncio
import json
import uuid
from datetime import datetime, timezone

import websockets

DEFAULT_URL = "ws://localhost:8080/ocpp/CP001"
SUBPROTOCOL = "ocpp1.6"

CALL, CALL_RESULT, CALL_ERROR = 2, 3, 4


def log(direction: str, frame: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"{ts} {direction} {frame}", flush=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


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


async def boot(ws) -> int:
    """Send BootNotification until accepted; returns the heartbeat interval."""
    while True:
        result = await call(ws, "BootNotification", {
            "chargePointVendor": "OcppLab",
            "chargePointModel": "Emulator-1",
        })
        interval = int(result.get("interval", 60)) or 60
        if result.get("status") == "Accepted":
            return interval
        print(f"BootNotification status {result.get('status')}, retrying in {interval}s")
        await asyncio.sleep(interval)


async def status_notification(ws, connector_id: int, status: str) -> None:
    await call(ws, "StatusNotification", {
        "connectorId": connector_id,
        "errorCode": "NoError",
        "status": status,
        "timestamp": now_iso(),
    })


async def run_heartbeat(ws, args) -> None:
    interval = await boot(ws)
    print(f"Boot accepted, sending Heartbeat every {interval}s (Ctrl+C to stop)")
    while True:
        await asyncio.sleep(interval)
        await call(ws, "Heartbeat", {})


async def run_session(ws, args) -> None:
    connector = args.connector
    meter_start = 1000  # Wh

    await boot(ws)
    await status_notification(ws, connector, "Preparing")

    auth = await call(ws, "Authorize", {"idTag": args.id_tag})
    if auth.get("idTagInfo", {}).get("status") != "Accepted":
        print(f"idTag {args.id_tag} not accepted, stopping")
        return

    start = await call(ws, "StartTransaction", {
        "connectorId": connector,
        "idTag": args.id_tag,
        "meterStart": meter_start,
        "timestamp": now_iso(),
    })
    transaction_id = start["transactionId"]
    print(f"Transaction {transaction_id} started")

    energy = meter_start
    for _ in range(3):
        await asyncio.sleep(args.meter_interval)
        energy += 1500
        await call(ws, "MeterValues", {
            "connectorId": connector,
            "transactionId": transaction_id,
            "meterValue": [{
                "timestamp": now_iso(),
                "sampledValue": [{
                    "value": str(energy),
                    "context": "Sample.Periodic",
                    "measurand": "Energy.Active.Import.Register",
                    "unit": "Wh",
                }],
            }],
        })

    stop_payload = {
        "transactionId": transaction_id,
        "idTag": args.id_tag,
        "meterStop": energy,
        "timestamp": now_iso(),
        "reason": "EVDisconnected",
    }
    await call(ws, "StopTransaction", stop_payload)
    if args.duplicate_stop:
        print("Sending the same StopTransaction again (--duplicate-stop)")
        await call(ws, "StopTransaction", stop_payload)

    await status_notification(ws, connector, "Available")
    print(f"Session done: transaction {transaction_id}, {energy - meter_start} Wh charged")


async def run(args) -> None:
    print(f"Connecting to {args.url} with subprotocol {SUBPROTOCOL}")
    async with websockets.connect(args.url, subprotocols=[SUBPROTOCOL]) as ws:
        if ws.subprotocol != SUBPROTOCOL:
            print(f"Server did not accept {SUBPROTOCOL} (got {ws.subprotocol!r}), giving up")
            return
        print(f"Connected, negotiated subprotocol: {ws.subprotocol}")

        if args.command == "session":
            await run_session(ws, args)
        else:
            await run_heartbeat(ws, args)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OCPP 1.6J charge point emulator")
    parser.add_argument("command", nargs="?", choices=["heartbeat", "session"], default="heartbeat")
    parser.add_argument("--url", default=DEFAULT_URL, help=f"CSMS endpoint (default {DEFAULT_URL})")
    parser.add_argument("--id-tag", default="TAG-0001", help="idTag used for Authorize/Start/Stop")
    parser.add_argument("--connector", type=int, default=1, help="connector id (default 1)")
    parser.add_argument("--meter-interval", type=float, default=1.0,
                        help="seconds between MeterValues in the session command (default 1)")
    parser.add_argument("--duplicate-stop", action="store_true",
                        help="session: send the same StopTransaction twice")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\nStopped")
    except OcppError as e:
        print(f"Error: {e}")
    except websockets.ConnectionClosed as e:
        print(f"Connection closed: code={e.rcvd.code if e.rcvd else None} reason={e.rcvd.reason if e.rcvd else ''}")
    except OSError as e:
        print(f"Could not connect to {args.url}: {e}")


if __name__ == "__main__":
    main()
