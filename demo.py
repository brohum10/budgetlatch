"""Synthetic concurrent AI-spend simulation: no external APIs or real charges."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import tempfile

from budgetlatch import BudgetDenied, Ledger


def main():
    with tempfile.TemporaryDirectory() as folder:
        ledger = Ledger(Path(folder) / "demo.db")
        ledger.create_account("demo-2026-10", 1_000_000)

        def attempt(index):
            try:
                ledger.reserve("demo-2026-10", f"call-{index}", hashlib.sha256(str(index).encode()).hexdigest(), 100_000)
                return index
            except BudgetDenied:
                return None

        with ThreadPoolExecutor(max_workers=12) as pool:
            admitted = [x for x in pool.map(attempt, range(60)) if x is not None]
        assert len(admitted) == 10
        before = ledger.status("demo-2026-10")
        first, second = (f"call-{i}" for i in admitted[:2])
        ledger.mark_uncertain("demo-2026-10", first)
        ledger.settle("demo-2026-10", second, 60_000, "simulated-receipt")
        after = ledger.status("demo-2026-10")
        assert after["uncertain"] == 100_000
        assert after["available"] == 40_000
        print(json.dumps({"simulation_only": True, "attempts": 60, "admitted": len(admitted),
                          "denied": 60-len(admitted), "before": before, "after": after}, indent=2))


if __name__ == "__main__":
    main()
