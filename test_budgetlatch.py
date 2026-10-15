from concurrent.futures import ThreadPoolExecutor
import contextlib
import hashlib
import io
import json
from pathlib import Path
import random
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from budgetlatch import BudgetDenied, Conflict, Ledger, main

HASH = hashlib.sha256(b"synthetic request").hexdigest()


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "ledger.db"
        self.ledger = Ledger(self.path)
        self.ledger.create_account("team", 1000)

    def reserve(self, request="r", amount=100):
        return self.ledger.reserve("team", request, HASH, amount)

    def test_initial_status(self):
        self.assertEqual(self.ledger.status("team")["available"], 1000)

    def test_integer_amount_validation(self):
        for invalid in (True, 1.5, "100", -1, 10**13, None):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.reserve(amount=invalid)

    def test_zero_reservation_rejected(self):
        with self.assertRaises(ValueError): self.reserve(amount=0)

    def test_identifiers_and_digest(self):
        for invalid in ("", "a b", "<script>", "x"*121, None):
            with self.assertRaises(ValueError): self.ledger.status(invalid)
        with self.assertRaises(ValueError): self.ledger.reserve("team", "r", "not-a-hash", 1)

    def test_account_creation_is_idempotent(self):
        self.ledger.create_account("team", 1000)
        self.assertEqual(len(self.ledger.audit("team")), 1)
        with self.assertRaises(Conflict): self.ledger.create_account("team", 2000)

    def test_zero_budget_denies_calls(self):
        self.ledger.create_account("empty", 0)
        with self.assertRaises(BudgetDenied): self.ledger.reserve("empty", "r", HASH, 1)

    def test_exact_budget_boundary(self):
        self.reserve(amount=1000)
        with self.assertRaises(BudgetDenied): self.reserve("next", 1)
        self.assertEqual(self.ledger.status("team")["available"], 0)

    def test_retry_does_not_reserve_twice(self):
        self.assertTrue(self.reserve()["created"])
        self.assertFalse(self.reserve()["created"])
        self.assertEqual(self.ledger.status("team")["held"], 100)
        self.assertEqual(len(self.ledger.audit("team")), 2)

    def test_idempotency_conflicts(self):
        self.reserve()
        with self.assertRaises(Conflict): self.reserve(amount=101)
        with self.assertRaises(Conflict): self.ledger.reserve("team", "r", "a"*64, 100)

    def test_concurrent_budget_admission(self):
        def attempt(i):
            try: return self.reserve(str(i), 100)["created"]
            except BudgetDenied: return False
        with ThreadPoolExecutor(max_workers=12) as pool:
            admitted = list(pool.map(attempt, range(60)))
        self.assertEqual(sum(admitted), 10)
        self.assertEqual(self.ledger.status("team")["held"], 1000)

    def test_concurrent_same_key(self):
        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(lambda _: self.reserve(), range(40)))
        self.assertEqual(sum(r["created"] for r in results), 1)
        self.assertEqual(self.ledger.status("team")["held"], 100)

    def test_timeout_keeps_hold(self):
        self.reserve()
        self.ledger.mark_uncertain("team", "r")
        self.ledger.mark_uncertain("team", "r")
        self.assertEqual(self.ledger.status("team")["uncertain"], 100)
        self.assertEqual(self.ledger.status("team")["available"], 900)
        with self.assertRaises(Conflict): self.ledger.cancel_before_dispatch("team", "r")

    def test_settlement_releases_unused_hold(self):
        self.reserve()
        self.ledger.settle("team", "r", 60, "receipt-1")
        status = self.ledger.status("team")
        self.assertEqual((status["spent"], status["held"], status["available"]), (60, 0, 940))

    def test_reconcile_uncertain(self):
        self.reserve()
        self.ledger.mark_uncertain("team", "r")
        self.ledger.settle("team", "r", 80, "receipt-1")
        self.assertEqual(self.ledger.status("team")["uncertain"], 0)

    def test_confirmed_no_charge(self):
        self.reserve()
        self.ledger.mark_uncertain("team", "r")
        self.ledger.settle("team", "r", 0, "confirmed-no-charge")
        self.assertEqual(self.ledger.status("team")["available"], 1000)

    def test_settlement_retry(self):
        self.reserve()
        self.ledger.settle("team", "r", 60, "receipt-1")
        self.ledger.settle("team", "r", 60, "receipt-1")
        self.assertEqual(len(self.ledger.audit("team")), 3)
        with self.assertRaises(Conflict): self.ledger.settle("team", "r", 61, "receipt-1")
        with self.assertRaises(Conflict): self.ledger.settle("team", "r", 60, "receipt-2")

    def test_actual_overrun_recorded_and_admission_blocked(self):
        self.reserve(amount=900)
        self.ledger.settle("team", "r", 1100, "real-charge")
        status = self.ledger.status("team")
        self.assertEqual(status["spent"], 1100)
        self.assertEqual(status["overcommitted"], 100)
        with self.assertRaises(BudgetDenied): self.reserve("next", 1)

    def test_cancel_releases_hold_once(self):
        self.reserve()
        self.ledger.cancel_before_dispatch("team", "r")
        self.ledger.cancel_before_dispatch("team", "r")
        self.assertEqual(self.ledger.status("team")["available"], 1000)
        self.assertEqual(len(self.ledger.audit("team")), 3)

    def test_cancelled_key_not_reactivated(self):
        self.reserve()
        self.ledger.cancel_before_dispatch("team", "r")
        replay = self.reserve()
        self.assertFalse(replay["created"])
        self.assertEqual(replay["state"], "cancelled")
        with self.assertRaises(Conflict): self.ledger.settle("team", "r", 1, "receipt")
        with self.assertRaises(Conflict): self.ledger.mark_uncertain("team", "r")

    def test_settled_key_not_reactivated(self):
        self.reserve()
        self.ledger.settle("team", "r", 10, "receipt")
        self.assertEqual(self.reserve()["state"], "settled")
        with self.assertRaises(Conflict): self.ledger.cancel_before_dispatch("team", "r")
        with self.assertRaises(Conflict): self.ledger.mark_uncertain("team", "r")

    def test_account_isolation(self):
        self.ledger.create_account("other", 50)
        self.reserve()
        self.ledger.reserve("other", "r", HASH, 40)
        self.assertEqual(self.ledger.status("other")["available"], 10)
        self.assertEqual(self.ledger.status("team")["available"], 900)

    def test_unknown_entities(self):
        with self.assertRaises(Conflict): self.ledger.status("missing")
        with self.assertRaises(Conflict): self.ledger.audit("missing")
        with self.assertRaises(Conflict): self.ledger.settle("team", "missing", 1, "r")
        with self.assertRaises(Conflict): self.ledger.reserve("missing", "r", HASH, 1)

    def test_reopen_preserves_uncertainty(self):
        self.reserve()
        self.ledger.mark_uncertain("team", "r")
        reopened = Ledger(self.path)
        self.assertEqual(reopened.status("team")["uncertain"], 100)
        self.assertFalse(reopened.reserve("team", "r", HASH, 100)["created"])

    def test_audit_failure_rolls_back_reservation(self):
        with patch.object(self.ledger, "_event", side_effect=sqlite3.OperationalError("injected failure")):
            with self.assertRaises(sqlite3.OperationalError): self.reserve()
        self.assertEqual(self.ledger.status("team")["held"], 0)
        self.assertTrue(self.reserve()["created"])

    def test_audit_failure_rolls_back_settlement(self):
        self.reserve()
        with patch.object(self.ledger, "_event", side_effect=sqlite3.OperationalError("injected failure")):
            with self.assertRaises(sqlite3.OperationalError): self.ledger.settle("team", "r", 60, "r")
        self.assertEqual(self.ledger.status("team")["held"], 100)
        self.assertEqual(self.ledger.status("team")["spent"], 0)

    def test_concurrent_conflicting_settlements(self):
        self.reserve()
        def settle(amount):
            try:
                self.ledger.settle("team", "r", amount, "r")
                return amount
            except Conflict: return None
        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(settle, range(20, 40)))
        winners = [x for x in outcomes if x is not None]
        self.assertEqual(len(winners), 1)
        self.assertEqual(self.ledger.status("team")["spent"], winners[0])

    def test_cancel_settle_race_has_one_terminal_state(self):
        self.reserve()
        def change(action):
            try:
                if action == "cancel": return self.ledger.cancel_before_dispatch("team", "r")["state"]
                return self.ledger.settle("team", "r", 50, "r")["state"]
            except Conflict: return "conflict"
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(change, ["cancel", "settle"]))
        self.assertEqual(outcomes.count("conflict"), 1)
        self.assertEqual(self.ledger.status("team")["held"], 0)

    def test_randomized_accounting_reference(self):
        rng = random.Random(42)
        held = spent = 0
        for i in range(100):
            amount = rng.randint(1, 15)
            if amount > 1000-held-spent:
                with self.assertRaises(BudgetDenied): self.reserve(str(i), amount)
                continue
            self.reserve(str(i), amount)
            action = rng.choice(["settle", "hold", "cancel", "uncertain"])
            if action == "settle":
                actual = rng.randint(0, amount+5)
                self.ledger.settle("team", str(i), actual, "receipt")
                spent += actual
            elif action == "cancel": self.ledger.cancel_before_dispatch("team", str(i))
            else:
                held += amount
                if action == "uncertain": self.ledger.mark_uncertain("team", str(i))
            status = self.ledger.status("team")
            self.assertEqual((status["spent"], status["held"], status["available"]), (spent, held, 1000-spent-held))

    def test_cli_codes_and_persistence(self):
        args = [sys.executable, "budgetlatch.py", "--db", str(self.path)]
        proc = subprocess.run(args+["reserve", "team", "r", "100", HASH], capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0)
        self.assertTrue(json.loads(proc.stdout)["created"])
        proc = subprocess.run(args+["reserve", "team", "x", "1000", HASH], capture_output=True, text=True)
        self.assertEqual(proc.returncode, 3)
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(main(["--db", str(self.path), "settle", "team", "missing", "0", "r"]), 2)

    def test_in_memory_database_rejected(self):
        with self.assertRaises(ValueError): Ledger(":memory:")

    def test_process_death_rolls_back_uncommitted_write(self):
        self.reserve()
        code = """import os,sqlite3,sys
c=sqlite3.connect(sys.argv[1])
c.execute('BEGIN IMMEDIATE')
c.execute("UPDATE requests SET state='settled',actual=60,receipt='crash' WHERE id='r'")
os._exit(7)
"""
        result = subprocess.run([sys.executable, "-c", code, str(self.path)], capture_output=True)
        self.assertEqual(result.returncode, 7)
        reopened = Ledger(self.path)
        self.assertEqual(reopened.status("team")["held"], 100)
        self.assertEqual(reopened.status("team")["spent"], 0)

    def test_multiple_processes_share_budget(self):
        # Separate interpreter processes, not just threads sharing one Ledger.
        command = [sys.executable, "budgetlatch.py", "--db", str(self.path), "reserve", "team"]
        processes = [subprocess.Popen(command+[f"p-{i}", "500", HASH], stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, text=True) for i in range(8)]
        for process in processes:
            process.communicate(timeout=30)
        self.assertEqual(sum(p.returncode == 0 for p in processes), 2)
        self.assertEqual(sum(p.returncode == 3 for p in processes), 6)
        self.assertEqual(self.ledger.status("team")["held"], 1000)


if __name__ == "__main__":
    unittest.main()
