"""File-follow candidates: owned files/child Python processes; no sockets.

Each worker calls
the real follow(), replacing only time and command-fsync with causal test
checkpoints. Protocol records and process proofs below are synthetic inputs,
never observations about a real Broker, PID or network service.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import time
import unittest
import uuid

import pending_replay_controller as controller


RAW = b"\x00\x00\x00\x04send"
OLD_RECEIPT = b"\x00\x00\x00\x04old!"
NEW_RECEIPT = b"\x00\x00\x00\x04new!"
CLIENT = [101, 1001, 101]
OLD = [201, 2001, 201]
NEW = [202, 2002, 202]
COMMANDS = ("arm_fault", "old_gone", "grant_replacement", "finish")


def checkpoint(kind: str) -> float:
    print(json.dumps({"checkpoint": kind}), flush=True)
    line = sys.stdin.readline(256)
    if not line:
        raise controller.GateError("test control pipe closed")
    message = json.loads(line)
    elapsed = message.get("advance")
    controller.require(type(elapsed) in (int, float) and 0 <= elapsed <= 721,
                       "invalid test clock step")
    return elapsed


def worker(run_id: str) -> None:
    class SteppedTime:
        now = 0.0

        def monotonic(self):
            return self.now

        def sleep(self, _delay):
            self.now += checkpoint("poll")

    class ObservedOs:
        def __getattr__(self, name):
            return getattr(os, name)

        def fsync(self, fd):
            checkpoint("before_fsync")
            os.fsync(fd)
            # The real fsync has returned; follow() has not called link yet.
            checkpoint("after_fsync")

    controller.time = SteppedTime()
    controller.os = ObservedOs()
    controller.follow(run_id)


class FollowProcess:
    """One owned worker and its exact production-path synthetic run directory."""

    def __init__(self, case: unittest.TestCase, *, create_directory=True):
        self.case = case
        self.run_id = uuid.uuid4().hex
        self.directory = Path("/tmp/moonpulsar-pending-" + self.run_id)
        self.created = False
        self.process = None
        self.expected_exit = None
        self.selector = selectors.DefaultSelector()
        self.buffer = bytearray()
        self.stdout = bytearray()
        self.stderr = bytearray()
        self.transcript = []
        self.last_checkpoint = None
        self.deadline = time.monotonic() + 10
        self.seq = 0
        self.state = dict(attempts=1, waits=1, completed=False, success=False,
                          received=0, acks=0, target_producer="7", target_sequence="23",
                          held=1, held_persisted=1, old_live=1, replay_seen=False,
                          receipt_authorized=False, producer_reconnects=0,
                          consumer_reconnects=0, terminal_reconnect_errors=0)
        case.addCleanup(self.cleanup)
        # Printed and flushed before mkdir/Popen: an outer hard-limit wrapper
        # can recover only these planned paths if unittest itself is killed.
        print(json.dumps({"follow_scope_planned": case.id(), "run_id": self.run_id}), flush=True)
        if create_directory:
            self.create_directory()
        # No environment value is printed or inspected. The worker's entry
        # calls only this file-only coordinator; it imports no Moon/Broker code.
        self.process = subprocess.Popen(
            [sys.executable, "-E", "-S", "-B", "-u", str(Path(__file__).resolve()), "--worker", self.run_id],
            cwd=Path(__file__).resolve().parent,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        # Workers stay in the runner's group so its outer hard limit covers
        # all of them. Tests signal only their own direct child's PID.
        self.case.assertEqual(os.getpgid(self.process.pid), os.getpgrp())
        for stream, name in ((self.process.stdout, "stdout"), (self.process.stderr, "stderr")):
            os.set_blocking(stream.fileno(), False)
            self.selector.register(stream, selectors.EVENT_READ, name)
        self.case.assertEqual(self.read_checkpoint(), "poll")

    def create_directory(self):
        self.directory.mkdir(mode=0o700)
        self.created = True
        self.write("owner-token", self.run_id.encode())

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        self.case.assertGreater(remaining, 0, "10-second owned-worker bound")
        return min(remaining, 3)

    def write(self, name, value):
        self.case.assertNotIn("/", name)
        self.case.assertNotIn("..", name)
        with (self.directory / name).open("xb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())

    def publish(self, name, value):
        temporary = name + ".tmp"
        self.write(temporary, value)
        os.link(self.directory / temporary, self.directory / name, follow_symlinks=False)
        (self.directory / temporary).unlink()

    def event(self, kind, **data):
        record = dict(run_id=self.run_id, seq=self.seq, event=kind, data=data)
        self.seq += 1
        return record

    def publish_event(self, record):
        encoded = json.dumps(record, separators=(",", ":")).encode() + b"\n"
        self.publish(f"event-{record['seq']}.json", encoded)

    def proof(self, kind, **changes):
        proof = dict(run_id=self.run_id, nonce="a" * 32, client_identity=CLIENT,
                     broker_identity=OLD)
        if kind == "old_broker_gone":
            proof.update(nonce="b" * 32, group_absent=True, listeners_absent=True)
        elif kind == "replacement_ready":
            proof.update(nonce="c" * 32, broker_identity=NEW, version="4.2.4",
                         admin_ready=True, connected_ready=True, lookup_echo="c" * 32,
                         socket_owner_pid=NEW[0])
        proof.update(changes)
        return proof

    def publish_proof(self, kind, **changes):
        self.publish("proof-" + kind + ".json", json.dumps(self.proof(kind, **changes)).encode())

    def snap(self, **changes):
        return dict(copy.deepcopy(self.state), **changes)

    def read_message(self):
        deadline = time.monotonic() + self.remaining()
        while True:
            if b"\n" in self.buffer:
                line, _, rest = self.buffer.partition(b"\n")
                self.buffer[:] = rest
                self.case.assertLessEqual(len(line), 4096)
                message = json.loads(line)
                self.transcript.append(message)
                return message
            if not self.selector.get_map():
                self.case.assertFalse(self.buffer, "partial worker stdout line")
                return None
            remaining = deadline - time.monotonic()
            self.case.assertGreater(remaining, 0, "worker checkpoint/exit deadline")
            ready = self.selector.select(remaining)
            self.case.assertTrue(ready, "worker did not reach a checkpoint")
            for key, _ in ready:
                chunk = os.read(key.fileobj.fileno(), 4096)
                if not chunk:
                    self.selector.unregister(key.fileobj)
                elif key.data == "stderr":
                    self.stderr.extend(chunk)
                    self.case.assertLessEqual(len(self.stderr), 65536)
                else:
                    self.stdout.extend(chunk)
                    self.buffer.extend(chunk)
                    self.case.assertLessEqual(len(self.stdout), 65536)
                    self.case.assertLessEqual(len(self.buffer), 65536)

    def read_checkpoint(self):
        while True:
            message = self.read_message()
            self.case.assertIsNotNone(message, self.stderr.decode(errors="replace"))
            if "checkpoint" in message:
                self.last_checkpoint = message["checkpoint"]
                return self.last_checkpoint

    def resume(self, elapsed=0.05):
        self.case.assertIsNotNone(self.last_checkpoint)
        self.process.stdin.write(json.dumps({"advance": elapsed}).encode() + b"\n")
        self.process.stdin.flush()
        self.last_checkpoint = None

    def poll(self, count=1):
        for _ in range(count):
            self.case.assertEqual(self.last_checkpoint, "poll")
            self.resume()
            while True:
                point = self.read_checkpoint()
                if point == "poll":
                    break
                self.case.assertIn(point, ("before_fsync", "after_fsync"))
                self.resume(0)

    def expect_exit(self, code, *, resume=True, elapsed=0.05, error=None):
        if resume:
            self.resume(elapsed)
        while True:
            message = self.read_message()
            if message is None:
                break
            self.case.assertNotIn("checkpoint", message, "unexpected additional follow iteration")
        self.case.assertEqual(self.process.wait(timeout=self.remaining()), code,
                              self.stderr.decode(errors="replace"))
        if code:
            self.case.assertFalse(any(m.get("status") == "TRACE_COMPLETE" for m in self.transcript))
        if error:
            self.case.assertIn(error, self.stderr.decode(errors="replace"))
        self.expected_exit = code

    def command(self, index):
        raw = (self.directory / f"command-{index}.txt").read_bytes()
        parts = raw.decode("ascii").split("\n")
        self.case.assertEqual(len(parts), 5)
        self.case.assertEqual(parts[:3], [COMMANDS[index - 1], self.run_id, str(index)])
        self.case.assertEqual(parts[4], "")
        self.case.assertRegex(parts[3], r"^[0-9a-f]{32}$")
        self.case.assertEqual(stat.S_IMODE((self.directory / f"command-{index}.txt").stat().st_mode), 0o600)
        self.case.assertFalse((self.directory / f"command-{index}.txt.tmp").exists())
        return dict(index=index, command=parts[0], challenge=parts[3])

    def assert_no_command(self, index=1):
        self.case.assertFalse((self.directory / f"command-{index}.txt").exists())

    def initial(self, *, held=True, raw=True, proof=True):
        self.write("original-send.bin", RAW)
        if raw:
            self.write("old-receipt-1.bin", OLD_RECEIPT)
        records = [
            self.event("startup", frontend="127.0.0.1:19001", backend="127.0.0.1:6650",
                       topic="persistent://public/default/pending-replay-" + self.run_id,
                       producer_name="pending-producer-" + self.run_id,
                       send_timeout_ms=180000, whole_client_timeout_ms=240000),
            self.event("connection_open", connection=0, epoch=1, backend="127.0.0.1:6650"),
            self.event("original_send", connection=0, epoch=1, file="original-send.bin",
                       state=self.snap(waits=0)),
            self.event("wait_entered", state=self.snap()),
        ]
        if held:
            records.append(self.event("receipt_held", connection=0, epoch=1,
                                      file="old-receipt-1.bin", state=self.snap()))
        for record in records:
            self.publish_event(record)
        if proof:
            self.publish_proof("initial_identity")
        return records

    def arm(self):
        self.initial()
        self.poll(5)
        command = self.command(1)
        self.publish_event(self.event("command_ack", **command, state=self.snap()))
        self.poll()

    def old_gone(self):
        self.arm()
        self.state["old_live"] = 0
        self.publish_event(self.event("connection_retired", connection=0, epoch=1, state=self.snap()))
        self.publish_proof("old_broker_gone")
        self.poll()
        command = self.command(2)
        self.publish_event(self.event("command_ack", **command, state=self.snap()))
        self.poll()

    def replacement(self):
        self.old_gone()
        self.publish_proof("replacement_ready")
        self.poll()
        command = self.command(3)
        self.publish_event(self.event("command_ack", **command, state=self.snap()))
        self.poll()
        self.publish_event(self.event("connection_open", connection=1, epoch=2, backend="127.0.0.1:6650"))
        self.poll()

    def cleanup(self):
        # Only this newly started child can be signalled. Never use any
        # of the synthetic PID values found in the supplied protocol proofs.
        process = self.process
        forced = False
        if process is not None and process.poll() is None:
            forced = True
            try:
                process.terminate()
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                process.wait(timeout=1)
        if process is not None:
            # The worker has no children. After wait(), drain its remaining
            # bounded bytes before closing pipes, including on an assertion
            # failure while a checkpoint was in flight.
            for stream, output in ((process.stdout, self.stdout), (process.stderr, self.stderr)):
                while len(output) <= 65536:
                    chunk = os.read(stream.fileno(), min(4096, 65537 - len(output)))
                    if not chunk:
                        break
                    output.extend(chunk)
        self.selector.close()
        if process is not None:
            for stream in (process.stdin, process.stdout, process.stderr):
                stream.close()
            try:
                os.kill(process.pid, 0)
                process_absent = False
            except ProcessLookupError:
                process_absent = True
            self.case.assertTrue(process_absent, "owned worker PID remains after wait")
        else:
            process_absent = True
        # Dump the bounded, already-read transcript on failure via unittest's
        # captured stdout. No host files, process table or ports are inspected.
        if self.created:
            self.case.assertTrue(stat.S_ISDIR(self.directory.lstat().st_mode))
            marker = self.directory / "owner-token"
            self.case.assertTrue(stat.S_ISREG(marker.lstat().st_mode))
            self.case.assertEqual(marker.read_bytes(), self.run_id.encode())
            shutil.rmtree(self.directory)
        self.case.assertFalse(self.directory.exists())
        exit_matched = process is not None and self.expected_exit is not None and process.returncode == self.expected_exit
        print(json.dumps({"follow_cleanup": self.case.id(), "run_id": self.run_id,
                          "pid": process.pid if process else None,
                          "exit": process.returncode if process else None,
                          "expected_exit": self.expected_exit, "worker_exit_matched": exit_matched,
                          "forced_owned_cleanup": forced, "process_absent": process_absent,
                          "directory_absent": True, "transcript": self.transcript,
                          "stdout": self.stdout.decode(errors="replace"),
                          "stderr": self.stderr.decode(errors="replace")}), flush=True)
        self.case.assertLessEqual(len(self.stdout), 65536)
        self.case.assertLessEqual(len(self.stderr), 65536)
        self.case.assertFalse(forced, "worker did not reach its expected exit before cleanup")
        self.case.assertTrue(exit_matched, "owned worker exit was not independently asserted")


class FileFollowTests(unittest.TestCase):
    def test_complete_file_trace_issues_four_unique_commands(self):
        run = FollowProcess(self)
        run.replacement()
        run.write("replayed-send.bin", RAW)
        run.write("new-receipt.bin", NEW_RECEIPT)
        run.state.update(replay_seen=True, producer_reconnects=1, consumer_reconnects=1)
        run.publish_event(run.event("replay_send", connection=1, epoch=2,
                                    file="replayed-send.bin", state=run.snap()))
        run.poll()
        run.state["receipt_authorized"] = True
        run.publish_event(run.event("receipt_release_authorized", connection=1, epoch=2, state=run.snap()))
        run.poll()
        run.publish_event(run.event("new_receipt_forwarded", connection=1, epoch=2,
                                    file="new-receipt.bin", state=run.snap()))
        run.poll()
        run.state.update(completed=True, success=True)
        run.publish_event(run.event("future_result", state=run.snap()))
        run.poll()
        run.state["received"] = 1
        run.publish_event(run.event("received", state=run.snap()))
        run.poll()
        run.assert_no_command(4)
        run.state["acks"] = 1
        run.publish_event(run.event("ack_confirmed", state=run.snap()))
        run.poll()
        command = run.command(4)
        challenges = [run.command(i)["challenge"] for i in range(1, 5)]
        self.assertEqual(len(set(challenges)), 4)
        run.publish_event(run.event("command_ack", **command, state=run.snap()))
        run.poll()
        run.publish_event(run.event("complete", state=run.snap()))
        run.expect_exit(0)
        complete = [m for m in run.transcript if m.get("status") == "TRACE_COMPLETE"]
        self.assertEqual(len(complete), 1)
        self.assertEqual(complete[0]["run_id"], run.run_id)
        self.assertEqual(complete[0]["original_send_sha256"], hashlib.sha256(RAW).hexdigest())
        self.assertEqual([m["issued"]["command"] for m in run.transcript if "issued" in m], list(COMMANDS))

    def test_partial_append_and_command_fsync_have_causal_barriers(self):
        run = FollowProcess(self)
        run.initial(held=False)
        run.poll(4)
        held = run.event("receipt_held", connection=0, epoch=1,
                         file="old-receipt-1.bin", state=run.snap())
        raw = json.dumps(held).encode() + b"\n"
        path = run.directory / f"event-{held['seq']}.json.tmp"
        run.write(path.name, raw[:len(raw) // 2])
        run.poll()
        run.assert_no_command()
        with path.open("ab") as stream:
            stream.write(raw[len(raw) // 2:])
            stream.flush()
            os.fsync(stream.fileno())
        run.poll()
        run.assert_no_command()
        os.link(path, run.directory / f"event-{held['seq']}.json", follow_symlinks=False)
        path.unlink()
        run.resume()
        self.assertEqual(run.read_checkpoint(), "before_fsync")
        run.assert_no_command()
        staged = run.directory / "command-1.txt.tmp"
        self.assertTrue(staged.is_file())
        first_bytes = staged.read_bytes()
        self.assertEqual(len(first_bytes.split(b"\n")), 5)
        run.resume(0)
        self.assertEqual(run.read_checkpoint(), "after_fsync")
        run.assert_no_command()
        self.assertEqual(staged.read_bytes(), first_bytes)
        run.resume(0)
        self.assertEqual(run.read_checkpoint(), "poll")
        self.assertEqual((run.directory / "command-1.txt").read_bytes(), first_bytes)
        run.command(1)
        run.expect_exit(1, elapsed=721, error="720-second coordinator deadline")

    def test_missing_raw_receipt_fails_without_fault_command(self):
        run = FollowProcess(self)
        run.initial(raw=False)
        run.poll(4)
        run.expect_exit(1, error="FileNotFoundError")
        run.assert_no_command()

    def test_partial_proof_append_is_ignored_until_publication(self):
        run = FollowProcess(self)
        run.initial(proof=False)
        run.poll(5)
        run.assert_no_command()
        raw = json.dumps(run.proof("initial_identity")).encode() + b"\n"
        path = run.directory / "proof-initial_identity.json.tmp"
        run.write(path.name, raw[:len(raw) // 2])
        run.poll()
        run.assert_no_command()
        with path.open("ab") as stream:
            stream.write(raw[len(raw) // 2:])
            stream.flush()
            os.fsync(stream.fileno())
        run.poll()
        run.assert_no_command()
        os.link(path, run.directory / "proof-initial_identity.json", follow_symlinks=False)
        path.unlink()
        run.poll()
        run.command(1)
        run.expect_exit(1, elapsed=721, error="720-second coordinator deadline")

    def test_receipt_not_persisted_fails_without_fault_command(self):
        run = FollowProcess(self)
        run.initial(held=False)
        run.poll(4)
        run.publish_event(run.event("receipt_held", connection=0, epoch=1,
                                    file="old-receipt-1.bin", state=run.snap(held_persisted=0)))
        run.expect_exit(1, error="receipt evidence not persisted")
        run.assert_no_command()

    def test_wrong_run_event_fails_without_fault_command(self):
        run = FollowProcess(self)
        records = run.initial(held=False, proof=False)
        foreign_run = ("0" if run.run_id[0] != "0" else "1") + run.run_id[1:]
        record = dict(records[0], run_id=foreign_run)
        (run.directory / "event-0.json").unlink()
        run.publish_event(record)
        run.expect_exit(1, error="wrong run")
        run.assert_no_command()

    def test_old_event_in_next_file_slot_fails_immediately(self):
        run = FollowProcess(self)
        records = run.initial(held=False)
        run.poll(4)
        run.publish("event-4.json", json.dumps(records[0]).encode())
        run.expect_exit(1, error="event filename/index mismatch")
        run.assert_no_command()

    def test_wrong_run_proof_cannot_arm_fault(self):
        run = FollowProcess(self)
        run.initial(proof=False)
        foreign_run = ("0" if run.run_id[0] != "0" else "1") + run.run_id[1:]
        run.publish_proof("initial_identity", run_id=foreign_run)
        run.expect_exit(1, error="wrong proof run")
        run.assert_no_command()

    def test_same_published_proof_retry_never_reissues_command(self):
        run = FollowProcess(self)
        run.initial()
        run.poll(5)
        command = (run.directory / "command-1.txt").read_bytes()
        run.poll(3)
        self.assertEqual((run.directory / "command-1.txt").read_bytes(), command)
        self.assertEqual(len([m for m in run.transcript if "issued" in m]), 1)
        run.assert_no_command(2)
        run.expect_exit(1, elapsed=721, error="720-second coordinator deadline")

    def test_conflicting_proof_retry_fails(self):
        run = FollowProcess(self)
        run.initial(held=False)
        run.poll(4)
        (run.directory / "proof-initial_identity.json").unlink()
        run.publish_proof("initial_identity", nonce="d" * 32)
        run.expect_exit(1, error="conflicting proof retry")
        run.assert_no_command()

    def test_stale_replacement_nonce_cannot_grant_generation(self):
        run = FollowProcess(self)
        run.old_gone()
        run.publish_proof("replacement_ready", nonce="b" * 32, lookup_echo="b" * 32)
        run.expect_exit(1, error="stale proof nonce")
        run.assert_no_command(3)

    def test_changed_client_identity_cannot_grant_generation(self):
        run = FollowProcess(self)
        run.old_gone()
        run.publish_proof("replacement_ready", client_identity=[999, 999, 999])
        run.expect_exit(1, error="client process changed")
        run.assert_no_command(3)

    def test_old_epoch_cannot_claim_replay(self):
        run = FollowProcess(self)
        run.replacement()
        run.write("replayed-send.bin", RAW)
        run.publish_event(run.event("replay_send", connection=0, epoch=1,
                                    file="replayed-send.bin", state=run.snap()))
        run.expect_exit(1, error="ungranted/repeated replay")
        run.assert_no_command(4)

    def test_changed_replay_bytes_fail_without_finish(self):
        run = FollowProcess(self)
        run.replacement()
        run.write("replayed-send.bin", b"\x00\x00\x00\x04diff")
        run.publish_event(run.event("replay_send", connection=1, epoch=2,
                                    file="replayed-send.bin", state=run.snap()))
        run.expect_exit(1, error="replayed SEND bytes changed")
        run.assert_no_command(4)

    def test_published_partial_json_is_a_protocol_error(self):
        run = FollowProcess(self)
        run.write("event-0.json", b'{"run_id":')
        run.expect_exit(1, error="JSONDecodeError")
        run.assert_no_command()

    def test_symlink_raw_evidence_is_not_read(self):
        run = FollowProcess(self)
        run.initial(raw=False)
        run.write("synthetic-receipt.bin", OLD_RECEIPT)
        (run.directory / "old-receipt-1.bin").symlink_to("synthetic-receipt.bin")
        run.poll(4)
        run.expect_exit(1, error="OSError")
        run.assert_no_command()

    def test_missing_ordinal_waits_without_consuming_later_file(self):
        run = FollowProcess(self)
        records = run.initial()
        missing = (run.directory / "event-0.json").read_bytes()
        (run.directory / "event-0.json").unlink()
        run.poll(2)
        run.assert_no_command()
        run.publish("event-0.json", missing)
        run.poll(len(records))
        run.command(1)
        run.expect_exit(1, elapsed=721, error="720-second coordinator deadline")

    def test_missing_directory_and_deadline_exit_are_bounded(self):
        run = FollowProcess(self, create_directory=False)
        self.assertFalse(run.directory.exists())
        run.poll()
        run.create_directory()
        run.poll()
        run.expect_exit(1, elapsed=721, error="720-second coordinator deadline")
        run.assert_no_command()

    def test_control_pipe_eof_is_an_abnormal_exit(self):
        run = FollowProcess(self)
        run.process.stdin.close()
        run.expect_exit(1, resume=False, error="test control pipe closed")
        run.assert_no_command()

    def test_owned_worker_signal_is_not_trace_complete(self):
        run = FollowProcess(self)
        run.process.send_signal(signal.SIGTERM)
        run.expect_exit(-signal.SIGTERM, resume=False)
        run.assert_no_command()


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--worker":
        worker(sys.argv[2])
    else:
        unittest.main()
