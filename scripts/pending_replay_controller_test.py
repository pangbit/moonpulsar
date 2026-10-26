"""Offline controller regressions: in-memory records/bytes, no sockets or files."""
import copy
import unittest

from pending_replay_controller import ControllerGate, GateError, endpoint


RUN = "a" * 32
CLIENT = [101, 1001, 101]
OLD = [201, 2001, 201]
NEW = [202, 2002, 202]
RAW = b"\x00\x00\x00\x04send"


class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.files = {"original-send.bin": RAW, "replayed-send.bin": RAW,
                      "old-receipt-1.bin": b"\x00\x00\x00\x04old!",
                      "old-receipt-2.bin": b"\x00\x00\x00\x04late",
                      "new-receipt.bin": b"\x00\x00\x00\x04new!"}
        challenges = iter(str(i) * 32 for i in range(1, 5))
        self.gate = ControllerGate(RUN, self.files.__getitem__, lambda: next(challenges))
        self.state = dict(attempts=1, waits=1, completed=False, success=False,
                          received=0, acks=0, target_producer="7", target_sequence="23",
                          held=1, held_persisted=1, old_live=1, replay_seen=False, receipt_authorized=False,
                          producer_reconnects=0, consumer_reconnects=0, terminal_reconnect_errors=0)

    def event(self, event, **data):
        record = dict(run_id=RUN, seq=len(self.gate.records), event=event, data=data)
        self.gate.consume(record)
        return record

    def snap(self, **extra):
        return dict(copy.deepcopy(self.state), **extra)

    def startup(self):
        self.event("startup", frontend="127.0.0.1:19001", backend="127.0.0.1:6650",
                   topic="persistent://public/default/pending-replay-" + RUN,
                   producer_name="pending-producer-" + RUN,
                   send_timeout_ms=180000, whole_client_timeout_ms=240000)
        self.event("connection_open", connection=0, epoch=1, backend="127.0.0.1:6650")

    def original(self, held=True):
        self.startup()
        self.event("original_send", connection=0, epoch=1, file="original-send.bin", state=self.snap(waits=0))
        self.event("wait_entered", state=self.snap())
        if held:
            self.event("receipt_held", connection=0, epoch=1, file="old-receipt-1.bin", state=self.snap())
        self.gate.proof("initial_identity", dict(run_id=RUN, nonce="a" * 32,
                                               client_identity=CLIENT, broker_identity=OLD))

    def ack(self, command):
        self.event("command_ack", **command, state=self.snap())

    def arm(self):
        self.original()
        command = self.gate.next_command()
        self.assertEqual(command["command"], "arm_fault")
        self.ack(command)

    def old_gone(self):
        self.arm()
        self.event("connection_retired", connection=0, epoch=1, state=self.snap(old_live=0))
        self.state["old_live"] = 0
        self.gate.proof("old_broker_gone", dict(run_id=RUN, nonce="b" * 32, client_identity=CLIENT,
                                              broker_identity=OLD, group_absent=True, listeners_absent=True))
        command = self.gate.next_command()
        self.assertEqual(command["command"], "old_gone")
        self.ack(command)

    def replacement(self):
        self.old_gone()
        self.gate.proof("replacement_ready", self.ready_proof())
        command = self.gate.next_command()
        self.assertEqual(command["command"], "grant_replacement")
        self.ack(command)
        self.event("connection_open", connection=1, epoch=2, backend="127.0.0.1:6650")

    @staticmethod
    def ready_proof():
        return dict(run_id=RUN, nonce="c" * 32, client_identity=CLIENT, broker_identity=NEW,
                    version="4.2.4", admin_ready=True, connected_ready=True,
                    lookup_echo="c" * 32, socket_owner_pid=NEW[0])

    def replay(self):
        self.replacement()
        self.state.update(replay_seen=True, producer_reconnects=1, consumer_reconnects=1)
        self.event("replay_send", connection=1, epoch=2, file="replayed-send.bin", state=self.snap())
        self.state["receipt_authorized"] = True
        self.event("receipt_release_authorized", connection=1, epoch=2, state=self.snap())

    def forwarded(self):
        self.event("new_receipt_forwarded", connection=1, epoch=2,
                   file="new-receipt.bin", state=self.snap())

    def outcome(self):
        self.state.update(completed=True, success=True)
        self.event("future_result", state=self.snap())

    def consumer_ack(self):
        self.state["received"] = 1
        self.event("received", state=self.snap(), message_id="synthetic", redelivery_count=0)
        self.state["acks"] = 1
        self.event("ack_confirmed", state=self.snap())

    def test_complete_trace_requires_four_challenges_and_original_future(self):
        self.replay(); self.forwarded(); self.outcome(); self.consumer_ack()
        command = self.gate.next_command()
        self.assertEqual(command["command"], "finish")
        self.ack(command)
        self.event("complete", state=self.snap())
        self.assertTrue(self.gate.complete)
        self.assertEqual(self.gate.original, RAW)

    def test_no_fault_without_a_real_held_receipt_record(self):
        self.original(held=False)
        self.assertIsNone(self.gate.next_command())

    def test_identical_event_retry_is_idempotent(self):
        self.startup()
        record = self.event("wait_entered", state=self.snap())
        count = len(self.gate.records)
        self.gate.consume(record)
        self.assertEqual(len(self.gate.records), count)

    def test_conflicting_duplicate_event_rejects(self):
        self.startup()
        record = self.event("wait_entered", state=self.snap())
        record["data"]["state"]["waits"] = 2
        with self.assertRaises(GateError): self.gate.consume(record)

    def test_wrong_run_and_skipped_event_indices_reject(self):
        for change in ({"run_id": "f" * 32}, {"seq": 2}):
            with self.subTest(change=change), self.assertRaises(GateError):
                self.gate.consume(dict(dict(run_id=RUN, seq=0, event="startup", data={}), **change))

    def test_second_waiter_rejects(self):
        self.startup()
        with self.assertRaises(GateError): self.event("wait_entered", state=self.snap(waits=2))

    def test_original_future_cannot_complete_before_release_authorization(self):
        self.original()
        with self.assertRaises(GateError):
            self.event("future_result", state=self.snap(completed=True, success=True))

    def test_changed_replay_wire_bytes_reject(self):
        self.replacement()
        self.files["replayed-send.bin"] = b"\x00\x00\x00\x04diff"
        with self.assertRaises(GateError):
            self.event("replay_send", connection=1, epoch=2, file="replayed-send.bin", state=self.snap())

    def test_old_receipt_after_replacement_remains_only_held_evidence(self):
        self.replacement()
        self.event("receipt_held", connection=0, epoch=1, file="old-receipt-2.bin", state=self.snap())
        self.assertFalse(self.gate.forwarded)
        self.assertEqual(len(self.gate.held_files), 2)

    def test_old_receipt_cannot_be_reported_as_new_forwarded(self):
        self.replay()
        with self.assertRaises(GateError):
            self.event("new_receipt_forwarded", connection=0, epoch=1,
                       file="old-receipt-1.bin", state=self.snap())

    def test_wrong_producer_or_sequence_cannot_satisfy_held_gate(self):
        self.original(held=False)
        for change in ({"target_producer": "8"}, {"target_sequence": "24"}):
            with self.subTest(change=change), self.assertRaises(GateError):
                self.event("receipt_held", connection=0, epoch=1,
                           file="old-receipt-1.bin", state=self.snap(**change))

    def test_another_connection_cannot_claim_the_target_receipt(self):
        self.original(held=False)
        with self.assertRaises(GateError):
            self.event("receipt_held", connection=99, epoch=1,
                       file="old-receipt-1.bin", state=self.snap())

    def test_old_broker_proof_requires_group_and_listener_absence(self):
        self.arm()
        with self.assertRaises(GateError):
            self.gate.proof("old_broker_gone", dict(run_id=RUN, nonce="b" * 32,
                            client_identity=CLIENT, broker_identity=OLD,
                            group_absent=True, listeners_absent=False))

    def test_replacement_cannot_precede_native_old_gone_ack(self):
        self.arm()
        with self.assertRaises(GateError): self.gate.proof("replacement_ready", self.ready_proof())

    def test_client_birth_identity_cannot_change(self):
        self.old_gone()
        changed = self.ready_proof(); changed["client_identity"] = [999, 999, 999]
        with self.assertRaises(GateError): self.gate.proof("replacement_ready", changed)

    def test_future_result_may_race_post_write_log_but_cannot_finish_early(self):
        self.replay(); self.outcome(); self.consumer_ack()
        self.assertIsNone(self.gate.next_command())
        self.forwarded()
        self.assertEqual(self.gate.next_command()["command"], "finish")

    def test_semantically_repeated_command_ack_rejects(self):
        self.original()
        command = self.gate.next_command(); self.ack(command)
        with self.assertRaises(GateError): self.ack(command)

    def test_repeat_original_send_with_a_new_event_index_rejects(self):
        self.original()
        with self.assertRaises(GateError):
            self.event("original_send", connection=0, epoch=1,
                       file="original-send.bin", state=self.snap())

    def test_endpoint_and_raw_path_boundaries(self):
        for value in ("localhost:6650", "0.0.0.0:6650", "[::1]:6650", "127.0.0.1:0", "127.0.0.1:06650"):
            with self.subTest(value=value), self.assertRaises(GateError): endpoint(value)
        with self.assertRaises(GateError): self.gate._frame("../credentials")
        self.startup()
        with self.assertRaises(GateError):
            self.event("connection_open", connection=2, epoch=1, backend="127.0.0.1:6651")

    def test_final_record_cannot_replace_the_original_attempt(self):
        self.replay(); self.forwarded(); self.outcome(); self.consumer_ack()
        self.ack(self.gate.next_command())
        with self.assertRaises(GateError):
            self.event("complete", state=self.snap(attempts=2))


if __name__ == "__main__":
    unittest.main()
