#!/usr/bin/env python3
"""Test-only journal gates. No sockets, subprocesses or Broker operations.

Broker/process/readiness proofs are external observations, NOT self-attesting
facts established by this validator. A separately reviewed lifecycle verifier
and watchdog are required before any live acceptance window.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
import time
from typing import Callable


class GateError(ValueError):
    pass


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise GateError(reason)


def nonce(value: object) -> str:
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{32}", value) is not None,
            "expected a 128-bit lowercase hex nonce")
    return value


def endpoint(value: object) -> str:
    require(isinstance(value, str), "endpoint must be text")
    match = re.fullmatch(r"127\.0\.0\.1:([1-9][0-9]{0,4})", value)
    require(match is not None and int(match[1]) <= 65535, "nonloopback endpoint")
    return value


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def identity(value: object) -> tuple[int, int, int]:
    require(isinstance(value, list) and len(value) == 3 and
            all(type(x) is int and x > 0 for x in value), "pid/start_ticks/pgid required")
    return tuple(value)


class ControllerGate:
    """Pure controller state; read_raw reads only named synthetic frame evidence."""

    def __init__(self, run_id: str, read_raw: Callable[[str], bytes],
                 challenge: Callable[[], str] = lambda: secrets.token_hex(16)):
        self.run_id = nonce(run_id)
        self.read_raw = read_raw
        self.challenge = challenge
        self.records: list[bytes] = []
        self.connections: dict[int, int] = {}
        self.live: set[int] = set()
        self.startup = False
        self.frontend: str | None = None
        self.backend: str | None = None
        self.wait_entered = False
        self.target: tuple[str, str] | None = None
        self.original: bytes | None = None
        self.original_connection: int | None = None
        self.replay_connection: int | None = None
        self.held_files: set[str] = set()
        self.replayed = False
        self.release_authorized = False
        self.forwarded = False
        self.future_result: bool | None = None
        self.received = self.acks = 0
        self.commands: list[dict] = []
        self.acked = 0
        self.proofs: dict[str, dict] = {}
        self.complete = False

    def _frame(self, name: str) -> bytes:
        require(name in {"original-send.bin", "replayed-send.bin", "new-receipt.bin"} or
                re.fullmatch(r"old-receipt-[1-8]\.bin", name) is not None, "unapproved raw path")
        raw = self.read_raw(name)
        require(type(raw) is bytes and 8 <= len(raw) <= 65540 and
                int.from_bytes(raw[:4], "big") == len(raw) - 4, "invalid bounded frame evidence")
        return raw

    def _state(self, data: dict, *, pending: bool = False) -> dict:
        state = data.get("state")
        require(isinstance(state, dict), "missing attempt state")
        require(type(state.get("attempts")) is int and type(state.get("waits")) is int and
                state.get("attempts") == 1 and state.get("waits") == 1,
                "application admission or waiter was replaced/repeated")
        require(type(state.get("terminal_reconnect_errors")) is int and
                state.get("terminal_reconnect_errors") == 0, "terminal reconnect error")
        if pending:
            require(state.get("completed") is False, "original waiter already settled")
        return state

    @staticmethod
    def _key(state: dict) -> tuple[str, str]:
        key = (state.get("target_producer"), state.get("target_sequence"))
        for number in key:
            require(isinstance(number, str) and re.fullmatch(r"0|[1-9][0-9]*", number) is not None
                    and int(number) <= 2**64 - 1, "invalid target identity")
        return key

    def consume(self, record: dict) -> None:
        require(record.get("run_id") == self.run_id, "wrong run")
        index = record.get("seq")
        require(type(index) is int and 0 <= index < 512, "invalid event index")
        encoded = canonical(record)
        require(len(encoded) <= 2048, "event size bound")
        if index < len(self.records):
            require(self.records[index] == encoded, "conflicting duplicate event")
            return
        require(index == len(self.records), "missing or late event index")
        event, data = record.get("event"), record.get("data")
        require(isinstance(data, dict) and not self.complete, "invalid or post-complete event")
        self._consume(event, data)
        self.records.append(encoded)

    def _consume(self, event: str, data: dict) -> None:
        if event == "startup":
            require(not self.startup and not self.records, "repeated startup")
            self.frontend = endpoint(data.get("frontend"))
            self.backend = endpoint(data.get("backend"))
            require(data.get("topic") == "persistent://public/default/pending-replay-" + self.run_id
                    and data.get("producer_name") == "pending-producer-" + self.run_id,
                    "nonunique resource names")
            require(data.get("send_timeout_ms") == 180000 and
                    data.get("whole_client_timeout_ms") == 240000, "deadline changed")
            self.startup = True
            return
        require(self.startup, "event before startup")
        if event == "connection_open":
            number, epoch = data.get("connection"), data.get("epoch")
            require(type(number) is int and number not in self.connections and
                    len(self.connections) < 128 and len(self.live) < 8, "connection bounds/reuse")
            require((epoch == 1 and self.acked < 2) or
                    (epoch == 2 and any(c["command"] == "grant_replacement" for c in self.commands)),
                    "connection epoch not granted")
            require(endpoint(data.get("backend")) == self.backend, "backend changed from startup")
            self.connections[number] = epoch
            self.live.add(number)
        elif event == "connection_retired":
            number = data.get("connection")
            require(number in self.live and self.connections[number] == data.get("epoch"),
                    "duplicate/unknown connection retirement")
            self.live.remove(number)
        elif event == "original_send":
            require(self.original is None and data.get("epoch") == 1 and
                    self.connections.get(data.get("connection")) == 1 and
                    data.get("file") == "original-send.bin", "repeated/noninitial original SEND")
            self.original = self._frame(data["file"])
            self.target = self._key(data["state"])
            self.original_connection = data["connection"]
        elif event == "wait_entered":
            require(not self.wait_entered, "second waiter")
            self._state(data, pending=True)
            self.wait_entered = True
        elif event == "receipt_held":
            require(self.original is not None and data.get("epoch") == 1 and
                    data.get("connection") == self.original_connection and
                    self._key(data["state"]) == self.target, "wrong receipt generation/identity")
            require(type(data["state"].get("held_persisted")) is int and
                    data["state"]["held_persisted"] > 0, "receipt evidence not persisted")
            name = data.get("file")
            require(isinstance(name, str) and name not in self.held_files and
                    re.fullmatch(r"old-receipt-[1-8]\.bin", name) is not None, "receipt evidence reused")
            self._frame(name)
            self.held_files.add(name)
        elif event == "command_ack":
            require(self.acked < len(self.commands), "unexpected command ACK")
            expected = self.commands[self.acked]
            require(type(data.get("index")) is int and
                    all(data.get(k) == expected[k] for k in ("index", "command", "challenge")),
                    "late/conflicting command ACK")
            state = self._state(data, pending=expected["command"] != "finish")
            require(self._key(state) == self.target, "target changed across challenge")
            if expected["command"] == "old_gone":
                require(state.get("old_live") == 0 and not any(
                    self.connections[i] == 1 for i in self.live), "old connections remain")
            self.acked += 1
        elif event == "replay_send":
            require(not self.replayed and any(c["command"] == "grant_replacement" for c in self.commands)
                    and data.get("epoch") == 2 and self.connections.get(data.get("connection")) == 2
                    and self._key(data["state"]) == self.target and
                    data.get("file") == "replayed-send.bin", "ungranted/repeated replay")
            require(self._frame(data["file"]) == self.original, "replayed SEND bytes changed")
            self.replayed = True
            self.replay_connection = data["connection"]
        elif event in {"receipt_release_authorized", "new_receipt_forwarded"}:
            require(self.replayed and data.get("epoch") == 2 and
                    data.get("connection") == self.replay_connection and
                    self._key(data["state"]) == self.target, "receipt without matching new replay")
            if event == "receipt_release_authorized":
                require(not self.release_authorized, "second release authorization")
                self.release_authorized = True
            else:
                require(self.release_authorized and not self.forwarded and
                        data.get("file") == "new-receipt.bin", "old or repeated forwarded receipt")
                self._frame(data["file"])
                self.forwarded = True
        elif event == "future_result":
            state = self._state(data)
            require(self.release_authorized and self.future_result is None and
                    state.get("completed") is True and state.get("success") is True,
                    "early/repeated/failed original future")
            self.future_result = True
        elif event in {"received", "ack_confirmed"}:
            state = data.get("state", {})
            if event == "ack_confirmed":
                self._state(data)
                require(state.get("producer_reconnects", 0) > 0 and
                        state.get("consumer_reconnects", 0) > 0, "ACK without recovered objects")
            received, acks = state.get("received"), state.get("acks")
            require(type(received) is int and type(acks) is int and
                    0 <= acks <= received <= 16 and received >= self.received and acks >= self.acks,
                    "consumer observation counters regressed")
            self.received, self.acks = received, acks
        elif event == "complete":
            state = self._state(data)
            require(self.acked == 4 and self.forwarded and self.future_result is True and self.acks > 0,
                    "incomplete final record")
            require(state.get("completed") is True and state.get("success") is True and
                    self._key(state) == self.target and state.get("acks", 0) >= self.acks,
                    "final state changed the original attempt")
            self.complete = True
        elif event == "lookup_rewritten":
            require(endpoint(data.get("frontend")) == self.frontend, "LOOKUP frontend changed")
            require(data.get("connection") in self.connections, "unknown LOOKUP connection")
        elif event == "old_partial_discarded":
            size = data.get("bytes")
            require(self.connections.get(data.get("connection")) == 1 and data.get("epoch") == 1
                    and bool(self.commands) and self.commands[0]["command"] == "arm_fault"
                    and type(size) is int and 0 < size < 65540, "uncontrolled truncated frame")
        elif event not in {"frame_dropped", "connection_rejected"}:
            raise GateError("unknown event")

    def proof(self, kind: str, proof: dict) -> None:
        require(proof.get("run_id") == self.run_id, "wrong proof run")
        nonce(proof.get("nonce"))
        if kind in self.proofs:
            require(canonical(self.proofs[kind]) == canonical(proof), "conflicting proof retry")
            return
        require(kind in {"initial_identity", "old_broker_gone", "replacement_ready"}, "unknown proof")
        client = identity(proof.get("client_identity"))
        broker = identity(proof.get("broker_identity"))
        if kind == "initial_identity":
            require(not self.commands, "late initial identity")
        else:
            require("initial_identity" in self.proofs and
                    client == identity(self.proofs["initial_identity"]["client_identity"]),
                    "client process changed")
            require(proof["nonce"] not in {p["nonce"] for p in self.proofs.values()}, "stale proof nonce")
            if kind == "old_broker_gone":
                require(self.acked >= 1 and broker == identity(self.proofs["initial_identity"]["broker_identity"])
                        and proof.get("group_absent") is True and proof.get("listeners_absent") is True,
                        "old Broker exit observation missing")
            else:
                require(self.acked >= 2 and "old_broker_gone" in self.proofs and
                        broker != identity(self.proofs["initial_identity"]["broker_identity"])
                        and proof.get("version") == "4.2.4" and proof.get("admin_ready") is True
                        and proof.get("connected_ready") is True and proof.get("lookup_echo") == proof["nonce"]
                        and proof.get("socket_owner_pid") == broker[0], "fresh replacement readiness missing")
        self.proofs[kind] = proof

    def next_command(self) -> dict | None:
        if len(self.commands) != self.acked or self.complete:
            return None
        stage = len(self.commands)
        eligible = [
            self.wait_entered and bool(self.held_files) and "initial_identity" in self.proofs,
            "old_broker_gone" in self.proofs and not any(self.connections[i] == 1 for i in self.live),
            "replacement_ready" in self.proofs,
            self.forwarded and self.future_result is True and self.received > 0 and self.acks > 0,
        ]
        if stage >= 4 or not eligible[stage]:
            return None
        command = {"index": stage + 1, "command": ["arm_fault", "old_gone", "grant_replacement", "finish"][stage],
                   "challenge": nonce(self.challenge())}
        require(command["challenge"] not in {c["challenge"] for c in self.commands}, "reused command challenge")
        self.commands.append(command)
        return dict(command)


def read_regular(directory: Path, name: str, limit: int) -> bytes:
    require("/" not in name and ".." not in name, "invalid evidence basename")
    fd = os.open(directory / name, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_size <= limit, "unsafe evidence file")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            value = stream.read(limit + 1)
        require(len(value) <= limit, "evidence grew beyond bound")
        return value
    finally:
        os.close(fd)


def follow(run_id: str) -> None:
    """File-only coordinator; requires external, separately verified Broker proofs."""
    run_id = nonce(run_id)
    directory = Path("/tmp/moonpulsar-pending-" + run_id)
    gate = ControllerGate(run_id, lambda name: read_regular(directory, name, 65540))
    deadline = time.monotonic() + 720
    while time.monotonic() < deadline:
        if not directory.exists():
            time.sleep(0.05)
            continue
        require(stat.S_ISDIR(directory.lstat().st_mode), "run directory is not a real directory")
        index = len(gate.records)
        path = directory / f"event-{index}.json"
        if path.exists():
            record = json.loads(read_regular(directory, path.name, 2048))
            # The pure gate permits an identical event retry. In this adapter
            # each published filename is one immutable ordinal, so an old
            # record copied into the next slot must not stall/replay the trace.
            require(isinstance(record, dict) and type(record.get("seq")) is int
                    and record["seq"] == index, "event filename/index mismatch")
            gate.consume(record)
        for kind in ("initial_identity", "old_broker_gone", "replacement_ready"):
            name = "proof-" + kind + ".json"
            if (directory / name).exists():
                gate.proof(kind, json.loads(read_regular(directory, name, 4096)))
        command = gate.next_command()
        if command:
            name = f"command-{command['index']}.txt"
            text = f"{command['command']}\n{run_id}\n{command['index']}\n{command['challenge']}\n".encode()
            temp = directory / (name + ".tmp")
            fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(text); stream.flush(); os.fsync(stream.fileno())
            # link() refuses to replace an existing command, unlike rename().
            os.link(temp, directory / name, follow_symlinks=False)
            temp.unlink()
            print(json.dumps({"issued": command}), flush=True)
        if gate.complete:
            print(json.dumps({"status": "TRACE_COMPLETE", "run_id": run_id,
                              "original_send_sha256": hashlib.sha256(gate.original).hexdigest(),
                              "scope": "one pending SEND; external lifecycle proofs require independent verification"}))
            return
        time.sleep(0.05)
    raise GateError("720-second coordinator deadline; outer watchdog must clean owned processes")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--follow", metavar="RUN_ID", required=True)
    follow(parser.parse_args().follow)
