"""Portable plan checks using temporary files, without native processes or sockets."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import pending_replay_entry_test as entry


class EntryPlanTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="moonpulsar-entry-plan-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.artifact = self.root / "build/native/debug/build/pangbit/moonpulsar_examples/pending_replay/pending_replay.exe"
        self.artifact.parent.mkdir(parents=True)
        # A deliberately non-executable file proves that plan generation does
        # not try to run a fixture. Native entry tests validate the actual ELF.
        self.artifact.write_bytes(b"plan-only-placeholder")
        self.evidence = self.root / "evidence"
        self.evidence.mkdir()

    def plan(self):
        return entry.make_plan(self.artifact, self.evidence)

    def test_fresh_plans_keep_six_cases_and_create_no_evidence(self):
        first, second = self.plan(), self.plan()
        self.assertEqual(tuple(c["name"] for c in first["cases"]), entry.CASES)
        self.assertEqual(len(first["cases"]), 6)
        ids = [p["batch_nonce"] for p in (first, second)]
        ids += [c["nonce"] for p in (first, second) for c in p["cases"]]
        self.assertEqual(len(set(ids)), 14)
        for value in ids:
            self.assertRegex(value, r"^[1-9a-f][0-9a-f]{31}$")
        self.assertEqual(first["artifact"], str(self.artifact))
        self.assertEqual(Path(first["case_evidence"]).parent, self.evidence)
        self.assertEqual(list(self.evidence.iterdir()), [])

    def test_missing_or_wrong_artifact_is_rejected(self):
        wrong = self.root / "unrelated.exe"
        wrong.write_bytes(b"unrelated")
        with self.assertRaises(AssertionError):
            entry.make_plan(wrong, self.evidence)
        self.artifact.unlink()
        with self.assertRaises(AssertionError):
            self.plan()

    def test_checkout_cannot_receive_evidence(self):
        checkout = Path(entry.__file__).resolve().parents[1]
        with self.assertRaises(AssertionError):
            entry.make_plan(self.artifact, checkout)

    def test_artifact_and_evidence_symlinks_are_rejected(self):
        original = self.artifact.with_suffix(".original")
        self.artifact.rename(original)
        self.artifact.symlink_to(original)
        with self.assertRaises(AssertionError):
            self.plan()
        self.artifact.unlink()
        original.rename(self.artifact)
        link = self.root / "evidence-link"
        link.symlink_to(self.evidence, target_is_directory=True)
        with self.assertRaises(AssertionError):
            entry.make_plan(self.artifact, link)

    def test_print_plan_does_not_enter_the_native_runner(self):
        output = io.StringIO()
        argv = ["pending_replay_entry_test.py", "--artifact", str(self.artifact),
                "--evidence-root", str(self.evidence), "--print-plan"]
        with patch.object(sys, "argv", argv), contextlib.redirect_stdout(output), \
                patch.object(entry, "case", side_effect=AssertionError("native runner entered")) as runner:
            self.assertEqual(entry.main(), 0)
        runner.assert_not_called()
        plan = json.loads(output.getvalue())
        self.assertEqual(len(plan["cases"]), 6)
        self.assertEqual(list(self.evidence.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
