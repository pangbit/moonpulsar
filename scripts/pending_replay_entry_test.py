#!/usr/bin/env python3
"""Opt-in native entry regressions for precreated relay roots.

Only a freshly built private fixture is launched. No Linux sampler, executor,
watchdog, Broker or legacy readiness proof is imported. A successful positive
case ends at persisted receipt withholding, then requires bounded child TERM;
it does not claim completion of the retained future or full SEND replay.
"""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import stat
import struct
import subprocess
import threading
import time
import uuid


CASES = ("missing-token", "wrong-token", "root-symlink", "token-symlink",
         "invalid-run-id", "valid-owner")


def sha(data):
    return hashlib.sha256(data).hexdigest()


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def birth(path):
    s = path.lstat()
    return (s.st_dev, s.st_ino, stat.S_IFMT(s.st_mode))


def tree(path):
    """Flat, bounded, no-follow snapshot of this harness's synthetic files."""
    identity = birth(path)
    if path.is_symlink():
        return {"identity": identity, "link": os.readlink(path)}
    files = {}
    for p in sorted(path.iterdir()):
        s = p.lstat()
        require(len(files) < 520, "owned snapshot entry limit")
        if stat.S_ISLNK(s.st_mode):
            files[p.name] = {"link": os.readlink(p), "identity": birth(p)}
        else:
            require(stat.S_ISREG(s.st_mode) and s.st_size <= 131072,
                    "unexpected owned snapshot entry")
            files[p.name] = {"sha256": sha(p.read_bytes()), "size": s.st_size,
                             "mtime_ns": s.st_mtime_ns, "identity": birth(p)}
    return {"identity": identity, "files": files}


def varint(value):
    require(0 <= value < 1 << 64, "unsigned test varint range")
    out = bytearray()
    while value >= 128:
        out.append((value & 127) | 128)
        value >>= 7
    return bytes(out) + bytes([value])


def uint(field, value):
    return varint(field << 3) + varint(value)


def blob(field, value):
    return varint((field << 3) | 2) + varint(len(value)) + value


def fields(data):
    result, at = {}, 0

    def number():
        nonlocal at
        value = 0
        for offset in range(10):
            require(at < len(data), "truncated test protobuf")
            byte = data[at]
            at += 1
            require(offset != 9 or byte <= 1, "overflow test protobuf")
            value |= (byte & 127) << (offset * 7)
            if byte < 128:
                return value
        raise AssertionError("unterminated test protobuf")

    while at < len(data):
        key = number()
        field, wire = key >> 3, key & 7
        require(field > 0 and field not in result, "duplicate test control field")
        if wire == 0:
            value = number()
        elif wire == 2:
            length = number()
            require(length <= len(data) - at, "truncated test byte field")
            value = data[at:at + length]
            at += length
        else:
            raise AssertionError("unsupported test control wire type")
        result[field] = value
    return result


def frame(kind, body):
    command = uint(1, kind) + blob(kind, body)
    return struct.pack(">II", len(command) + 4, len(command)) + command


def read_exact(peer, count, deadline):
    data = bytearray()
    while len(data) < count:
        remaining = deadline - time.monotonic()
        require(remaining > 0, "mock absolute read deadline")
        peer.settimeout(min(0.1, remaining))
        try:
            chunk = peer.recv(count - len(data))
        except socket.timeout:
            continue
        if not chunk:
            raise EOFError("owned fixture closed its upstream")
        data.extend(chunk)
    return bytes(data)


class Mock:
    """One old-generation mock, using the existing I/O fixture's wire replies."""
    def __init__(self, server, nonce, deadline):
        self.server, self.nonce, self.deadline = server, nonce, deadline
        self.expected_shutdown = False
        self.peer = None
        self.error = None
        self.accepted = 0
        self.lookups = []
        self.receipt = None
        self.commands = []
        self.worker = threading.Thread(target=self.run, daemon=True)

    def run(self):
        try:
            self.server.settimeout(max(0.001, self.deadline - time.monotonic()))
            self.peer, _ = self.server.accept()
            self.accepted += 1
            for _ in range(64):
                total = struct.unpack(">I", read_exact(self.peer, 4, self.deadline))[0]
                require(4 <= total <= 65536, "mock frame bound")
                raw = read_exact(self.peer, total, self.deadline)
                size = struct.unpack(">I", raw[:4])[0]
                require(0 < size <= total - 4, "mock command size")
                command = fields(raw[4:4 + size])
                kind = command[1]
                request = fields(command[kind])
                self.commands.append(kind)
                if kind == 2:
                    response = frame(3, blob(1, b"owned-local-io-mock") + uint(2, 21))
                elif kind == 21:
                    response = frame(22, uint(1, 0) + uint(2, request[2]))
                elif kind == 23:
                    topic = ("persistent://public/default/pending-replay-" + self.nonce).encode()
                    require(request[1] == topic, "unique entry topic")
                    self.lookups.append(request[2])
                    endpoint = "pulsar://127.0.0.1:" + str(self.server.getsockname()[1])
                    response = frame(24, blob(1, endpoint.encode()) + uint(3, 1)
                                     + uint(4, request[2]) + uint(8, 0))
                elif kind == 4:
                    response = frame(13, uint(1, request[5]))
                elif kind == 5:
                    require(request[4] == ("pending-producer-" + self.nonce).encode(),
                            "entry producer identity")
                    response = frame(17, uint(1, request[3]) + blob(2, request[4])
                                     + uint(3, (1 << 64) - 1))
                elif kind == 6:
                    require(self.receipt is None, "one original SEND only")
                    self.receipt = frame(7, uint(1, request[1]) + uint(2, request[2])
                                         + blob(3, uint(1, 9) + uint(2, 1)))
                    response = self.receipt
                elif kind == 18:
                    response = frame(19, b"")
                elif kind == 11:
                    continue
                else:
                    raise AssertionError("unexpected entry mock command " + str(kind))
                # The actual TCP path includes fragmented control replies.
                for part in (response[:1], response[1:3], response[3:]):
                    self.peer.sendall(part)
            raise AssertionError("mock command limit")
        except (EOFError, OSError) as error:
            if not self.expected_shutdown:
                self.error = repr(error)
        except BaseException as error:
            self.error = repr(error)
        finally:
            if self.peer is not None:
                self.peer.close()


def journal(root, nonce):
    paths = sorted((p for p in root.iterdir()
                    if re.fullmatch(r"event-[0-9]+\.json", p.name)),
                   key=lambda p: int(p.stem[6:]))
    require(len(paths) <= 512, "entry journal bound")
    records = []
    for ordinal, p in enumerate(paths):
        require(p.name == "event-" + str(ordinal) + ".json", "entry journal ordinal")
        require(stat.S_ISREG(p.lstat().st_mode) and p.stat().st_size <= 2048,
                "entry journal regular bounded record")
        record = json.loads(p.read_bytes())
        require(record["run_id"] == nonce and record["seq"] == ordinal,
                "entry journal nonce/sequence")
        records.append(record)
    return records


def port_closed(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.25)
        return probe.connect_ex(("127.0.0.1", port)) == errno.ECONNREFUSED


def case(plan, name, nonce, artifact, artifact_hash, work, evidence, global_deadline):
    deadline = min(global_deadline - 1, time.monotonic() + 10)
    active_deadline = deadline - 3
    root = Path("/tmp/moonpulsar-pending-" + nonce)
    require(not os.path.lexists(root), "canonical root already exists")
    canary = work / (name + "-outside")
    canary.mkdir(mode=0o700)
    (canary / "sentinel").write_bytes(b"synthetic owned canary; do not mutate")
    before_canary = tree(canary)
    if name == "root-symlink":
        root.symlink_to(canary, target_is_directory=True)
    else:
        root.mkdir(mode=0o700)
        if name == "token-symlink":
            (root / "owner-token").symlink_to(canary / "sentinel")
        elif name != "missing-token":
            (root / "owner-token").write_bytes(
                ("0" * 32 if name == "wrong-token" else nonce).encode())
    pinned_root = birth(root)
    before_root = tree(root)
    out = evidence / name
    out.mkdir(mode=0o700)
    (out / "owned-root.json").write_text(json.dumps({
        "path": str(root), "nonce": nonce, "identity": pinned_root,
        "outside_canary": str(canary), "symlink_case": name == "root-symlink",
    }, indent=2) + "\n")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(8)
    ports = [server.getsockname()[1]]
    worker = None
    child = None
    result = {"case": name, "nonce": nonce, "success": False,
              "timeout": False, "cleanup_kill": False, "expected_checkpoint_term": False}
    try:
        require(artifact.is_file() and not artifact.is_symlink()
                and sha(artifact.read_bytes()) == artifact_hash, "new artifact changed")
        env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "MP_PENDING_ENABLE": "1",
               "MP_PENDING_RUN_ID": nonce + "/../outside" if name == "invalid-run-id" else nonce,
               "MP_PENDING_BACKEND": "127.0.0.1:" + str(ports[0]),
               "MP_PENDING_PRECREATED_ROOT": "1"}
        if name == "valid-owner":
            worker = Mock(server, nonce, active_deadline)
            worker.worker.start()
        with (out / "stdout.log").open("xb") as stdout, (out / "stderr.log").open("xb") as stderr:
            child = subprocess.Popen([str(artifact)], cwd=work, env=env,
                                     stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr)
            # Inherit the outer runner's dedicated group: its hard timeout must
            # also contain this exact native child, rather than orphaning a new
            # session. The native fixture never launches further processes.
            result["pid"] = child.pid
            (out / "started.json").write_text(json.dumps({
                "pid": child.pid, "artifact_sha256": artifact_hash,
                "argv": [str(artifact)], "root_identity": pinned_root,
                "inherits_outer_owned_group": True,
            }, indent=2) + "\n")
            if name == "valid-owner":
                wake = threading.Event()
                while True:
                    require(time.monotonic() < active_deadline, "entry checkpoint deadline")
                    require(child.poll() is None, "entry exited before positive checkpoint")
                    require(worker.error is None, "mock failed: " + str(worker.error))
                    records = journal(root, nonce)
                    events = {r["event"] for r in records}
                    if {"startup", "connection_open", "lookup_rewritten", "original_send",
                        "wait_entered", "receipt_held"} <= events:
                        break
                    wake.wait(min(0.01, max(0, active_deadline - time.monotonic())))
                startup = next(r["data"] for r in records if r["event"] == "startup")
                frontend = startup["frontend"]
                require(re.fullmatch(r"127\.0\.0\.1:[1-9][0-9]{0,4}", frontend),
                        "entry relay must stay literal loopback")
                ports.append(int(frontend.split(":")[1]))
                held = next(r["data"]["state"] for r in records if r["event"] == "receipt_held")
                waited = next(r["data"]["state"] for r in records if r["event"] == "wait_entered")
                require(waited["attempts"] == waited["waits"] == 1 and not waited["completed"],
                        "one original admission and waiter required")
                require(held["attempts"] == held["held_persisted"] == 1
                        and not held["completed"] and not held["receipt_authorized"],
                        "entry must retain original pending waiter and persisted receipt")
                require(worker.lookups and worker.accepted == 1 and worker.receipt is not None,
                        "real CONNECTED/LOOKUP/SEND path not witnessed")
                require((root / "old-receipt-1.bin").read_bytes() == worker.receipt,
                        "persisted receipt bytes differ from mock")
                require("future_result" not in events and "complete" not in events,
                        "positive checkpoint is pending, not replay completion")
                result["checkpoint"] = {"events": sorted(events), "pending_state": held,
                                        "lookup_request_ids": worker.lookups[:],
                                        "accepted": worker.accepted, "mock_commands": worker.commands[:]}
                (out / "journal.json").write_text(json.dumps(records, indent=2) + "\n")
                worker.expected_shutdown = True
                child.terminate()
                result["expected_checkpoint_term"] = True
                child.wait(timeout=max(0.001, deadline - time.monotonic()))
                require(child.returncode == -signal.SIGTERM, "checkpoint TERM outcome")
            else:
                child.wait(timeout=max(0.001, active_deadline - time.monotonic()))
                require(child.returncode != 0, "unsafe owner entry accepted")
                error = "\n".join((out / stream).read_text(errors="replace")
                                  for stream in ("stdout.log", "stderr.log"))
                # The native main runner prints the qualified suberror
                # constructor, omitting Rejected's String payload. Require
                # that exact category, plus the independent path/no-connect
                # invariants below, instead of an unobservable message.
                expected = "pangbit/moonpulsar_examples/pending_replay.FixtureError.Rejected"
                require(error.strip() == expected or
                        (name == "missing-token" and "owner-token" in error),
                        "negative exit lacks the expected owner/nonce refusal")
                after_root = tree(root)
                require(after_root == before_root, "negative case mutated canonical root")
                result["refusal"] = {
                    "natural_nonzero_exit": True, "expected_native_error": expected,
                    "canonical_root_unchanged": True,
                    "before_snapshot_sha256": sha(json.dumps(before_root, sort_keys=True).encode()),
                    "after_snapshot_sha256": sha(json.dumps(after_root, sort_keys=True).encode()),
                }
        result["exit"] = child.returncode
        server.setblocking(False)
        try:
            bypass, _ = server.accept()
        except BlockingIOError:
            pass
        else:
            bypass.close()
            raise AssertionError("unexpected backend connect or relay bypass")
        result["backend_pending_connections"] = 0
        require(tree(canary) == before_canary, "outside canary was touched")
        result["success"] = True
    except BaseException as error:
        result["error"] = repr(error)
        result["timeout"] = isinstance(error, (TimeoutError, subprocess.TimeoutExpired))
    finally:
        if worker is not None:
            worker.expected_shutdown = True
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=max(0.001, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=1)
                result["cleanup_kill"] = True
                result["success"] = False
        if worker is not None:
            worker.worker.join(timeout=max(0.001, deadline - time.monotonic()))
            if worker.worker.is_alive() or worker.error is not None:
                result["success"] = False
                result["mock_cleanup_error"] = worker.error or "worker still alive"
        server.close()
        try:
            require(child is None or child.poll() is not None, "child not reaped")
            require(birth(root) == pinned_root, "canonical root identity changed")
            require(tree(canary) == before_canary, "outside canary changed before cleanup")
            if root.is_symlink():
                root.unlink()
            else:
                allowed = re.compile(r"owner-token|original-send\.bin|old-receipt-1\.bin|event-[0-9]+\.json(?:\.tmp)?")
                require(all(allowed.fullmatch(p.name) for p in root.iterdir()),
                        "unknown entry prevents canonical-root deletion")
                shutil.rmtree(root)
            require(not os.path.lexists(root), "canonical root remains")
            require(all(port_closed(p) for p in ports), "owned port still open or probe did not refuse")
            result["cleanup"] = {"child_reaped": child is None or child.poll() is not None,
                                 "shares_outer_owned_group": True, "ports_checked": len(ports),
                                 "canonical_root_absent": True, "outside_canary_unchanged": True}
        except BaseException as error:
            result["success"] = False
            result["cleanup_error"] = repr(error)
        result["artifact_sha256"] = artifact_hash
        result["exit"] = child.returncode if child is not None else None
        result["log_sha256"] = {p.name: sha(p.read_bytes()) for p in
                                (out / "stdout.log", out / "stderr.log") if p.is_file()}
        (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def make_plan(artifact, evidence_root):
    """Create a fresh six-case plan without launching a process or writing files."""
    artifact = Path(artifact).absolute()
    evidence_root = Path(evidence_root).absolute()
    require(".." not in artifact.parts and len(artifact.parents) >= 7, "absolute native artifact path")
    build_target = artifact.parents[6]
    expected = build_target / "native/debug/build/pangbit/moonpulsar_examples/pending_replay/pending_replay.exe"
    require(artifact == expected, "expected pending_replay debug artifact layout")
    require(artifact.is_file() and not artifact.is_symlink(), "native artifact missing")
    require(evidence_root.is_dir() and not evidence_root.is_symlink(), "existing evidence directory required")
    repo = Path(__file__).resolve().parents[1]
    resolved = evidence_root.resolve()
    require(resolved != repo and repo not in resolved.parents, "evidence must stay outside the checkout")
    def nonce():
        value = uuid.uuid4().hex
        while value[0] == "0":
            value = uuid.uuid4().hex
        return value
    batch = nonce()
    return {"batch_nonce": batch, "artifact": str(artifact),
            "build_target": str(build_target), "evidence_root": str(evidence_root),
            "case_evidence": str(evidence_root / ("entry-" + batch)),
            "work_root": "/tmp/moonpulsar-entry-owner-work-" + batch,
            "cases": [{"name": name, "nonce": nonce()} for name in CASES]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--plan", help="existing six-case JSON plan")
    source.add_argument("--artifact", help="absolute path to the freshly built debug fixture")
    parser.add_argument("--evidence-root", help="existing directory outside the checkout")
    parser.add_argument("--print-plan", action="store_true", help="print a plan without executing it")
    args = parser.parse_args()
    if args.artifact:
        if not args.evidence_root:
            parser.error("--artifact requires --evidence-root")
        plan = make_plan(args.artifact, args.evidence_root)
    else:
        if args.evidence_root:
            parser.error("--evidence-root is only used with --artifact")
        plan = json.loads(Path(args.plan).read_text())
    if args.print_plan:
        print(json.dumps(plan, indent=2))
        return 0
    require(tuple(x["name"] for x in plan["cases"]) == CASES, "explicit six-case selection")
    batch = plan["batch_nonce"]
    require(re.fullmatch("[0-9a-f]{32}", batch), "batch nonce")
    work = Path("/tmp/moonpulsar-entry-owner-work-" + batch)
    require(str(work) == plan["work_root"], "fixed owned work root")
    artifact = Path(plan["artifact"])
    require(artifact == Path(plan["build_target"]) / "native/debug/build/pangbit/moonpulsar_examples/pending_replay/pending_replay.exe",
            "only the newly built private fixture path is accepted")
    require(artifact.is_file() and not artifact.is_symlink(), "fresh native artifact missing")
    artifact_hash = sha(artifact.read_bytes())
    evidence = Path(plan["case_evidence"])
    require(evidence.parent == Path(plan["evidence_root"]), "fixed case evidence path")
    evidence.mkdir(mode=0o700)
    work.mkdir(mode=0o700)
    (work / "owner-token").write_text(batch)
    pinned = birth(work)
    (evidence / "owned-work-root.json").write_text(json.dumps({
        "path": str(work), "identity": pinned, "batch_nonce": batch,
    }, indent=2) + "\n")
    deadline = time.monotonic() + 52  # Outer hard 60s retains a cleanup margin.
    results = []
    cleanup = False
    try:
        for item in plan["cases"]:
            nonce = item["nonce"]
            require(re.fullmatch("[1-9a-f][0-9a-f]{31}", nonce), "case nonce")
            results.append(case(plan, item["name"], nonce, artifact, artifact_hash,
                                work, evidence, deadline))
            if not results[-1]["success"]:
                break  # Preserve first error; never hide a partial selection as six PASS.
    finally:
        require(birth(work) == pinned and (work / "owner-token").read_text() == batch,
                "owned work marker changed")
        canonical_gone = all(not os.path.lexists("/tmp/moonpulsar-pending-" + x["nonce"])
                             for x in plan["cases"])
        if canonical_gone:
            shutil.rmtree(work)
            cleanup = not os.path.lexists(work)
        summary = {"selected_cases": 6, "executed_cases": len(results),
                   "passed_cases": sum(x["success"] for x in results), "skips": 0,
                   "work_root_absent": cleanup, "artifact_sha256": artifact_hash,
                   "full_replay_claimed": False,
                   "success": len(results) == 6 and all(x["success"] for x in results) and cleanup}
        (evidence / "result.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary), flush=True)
    return 0 if summary["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
