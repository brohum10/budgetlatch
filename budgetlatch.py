"""Transactional budget reservations for AI calls; no provider execution."""

import argparse
from contextlib import contextmanager
import json
import re
import sqlite3
import sys

MAX_AMOUNT = 10**12


class BudgetDenied(ValueError):
    pass


class Conflict(ValueError):
    pass


def label(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,120}", value):
        raise ValueError("identifiers must be 1–120 ASCII letters, digits, or _ . : / -")
    return value


def money(value, *, positive=False):
    if type(value) is not int or not (1 if positive else 0) <= value <= MAX_AMOUNT:
        raise ValueError("amount must be integer microdollars within the supported range")
    return value


class Ledger:
    def __init__(self, path):
        self.path = str(path)
        if self.path == ":memory:":
            raise ValueError("use a file-backed database; each operation opens its own connection")
        with self.connection() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id TEXT PRIMARY KEY,
                    budget INTEGER NOT NULL CHECK(budget >= 0)
                );
                CREATE TABLE IF NOT EXISTS requests (
                    account TEXT NOT NULL REFERENCES accounts(id),
                    id TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    reserved INTEGER NOT NULL CHECK(reserved > 0),
                    state TEXT NOT NULL CHECK(state IN ('reserved','uncertain','settled','cancelled')),
                    actual INTEGER CHECK(actual >= 0),
                    receipt TEXT,
                    PRIMARY KEY(account, id),
                    CHECK((state = 'settled' AND actual IS NOT NULL AND receipt IS NOT NULL)
                       OR (state != 'settled' AND actual IS NULL AND receipt IS NULL))
                );
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    account TEXT NOT NULL REFERENCES accounts(id),
                    request TEXT,
                    action TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    recorded_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
                );
                CREATE INDEX IF NOT EXISTS events_account ON events(account, seq);
            """)

    @contextmanager
    def connection(self, *, transaction=False):
        conn = sqlite3.connect(self.path, timeout=20, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=FULL")
        try:
            if transaction:
                # All reads used for admission and their writes share the writer lock.
                conn.execute("BEGIN IMMEDIATE")
            yield conn
            if transaction:
                conn.commit()
        except BaseException:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _event(conn, account, request, action, amount):
        conn.execute("INSERT INTO events(account,request,action,amount) VALUES(?,?,?,?)",
                     (account, request, action, amount))

    @staticmethod
    def _status(conn, account):
        row = conn.execute("SELECT budget FROM accounts WHERE id=?", (account,)).fetchone()
        if row is None:
            raise Conflict("unknown account")
        sums = conn.execute("""SELECT
            COALESCE(SUM(CASE WHEN state='settled' THEN actual ELSE 0 END),0) spent,
            COALESCE(SUM(CASE WHEN state IN ('reserved','uncertain') THEN reserved ELSE 0 END),0) held,
            COALESCE(SUM(CASE WHEN state='uncertain' THEN reserved ELSE 0 END),0) uncertain
            FROM requests WHERE account=?""", (account,)).fetchone()
        available = row["budget"] - sums["spent"] - sums["held"]
        return {"account": account, "budget": row["budget"], **dict(sums),
                "available": available, "overcommitted": max(0, -available), "unit": "microdollars"}

    @staticmethod
    def _request(conn, account, request):
        row = conn.execute("SELECT * FROM requests WHERE account=? AND id=?", (account, request)).fetchone()
        if row is None:
            raise Conflict("unknown request")
        return dict(row)

    def create_account(self, account, budget):
        label(account)
        money(budget)
        with self.connection(transaction=True) as conn:
            previous = conn.execute("SELECT budget FROM accounts WHERE id=?", (account,)).fetchone()
            if previous is not None:
                if previous["budget"] != budget:
                    raise Conflict("account already exists with a different budget")
            else:
                conn.execute("INSERT INTO accounts VALUES(?,?)", (account, budget))
                self._event(conn, account, None, "account_created", budget)
            return self._status(conn, account)

    def reserve(self, account, request, fingerprint, amount):
        label(account)
        label(request)
        money(amount, positive=True)
        if not isinstance(fingerprint, str) or not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
            raise ValueError("fingerprint must be a lowercase SHA-256 digest")
        with self.connection(transaction=True) as conn:
            status = self._status(conn, account)
            old = conn.execute("SELECT * FROM requests WHERE account=? AND id=?", (account, request)).fetchone()
            if old is not None:
                if old["fingerprint"] != fingerprint or old["reserved"] != amount:
                    raise Conflict("idempotency key reused with different content or amount")
                return {**dict(old), "created": False}
            if amount > status["available"]:
                raise BudgetDenied("insufficient unreserved budget")
            conn.execute("INSERT INTO requests VALUES(?,?,?,?,'reserved',NULL,NULL)",
                         (account, request, fingerprint, amount))
            self._event(conn, account, request, "reserved", amount)
            return {**self._request(conn, account, request), "created": True}

    def mark_uncertain(self, account, request):
        label(account)
        label(request)
        with self.connection(transaction=True) as conn:
            row = self._request(conn, account, request)
            if row["state"] == "uncertain":
                return row
            if row["state"] != "reserved":
                raise Conflict("only reserved requests can become uncertain")
            conn.execute("UPDATE requests SET state='uncertain' WHERE account=? AND id=?", (account, request))
            self._event(conn, account, request, "uncertain", row["reserved"])
            return self._request(conn, account, request)

    def settle(self, account, request, actual, receipt):
        label(account)
        label(request)
        label(receipt)
        money(actual)
        with self.connection(transaction=True) as conn:
            row = self._request(conn, account, request)
            if row["state"] == "settled":
                if row["actual"] != actual or row["receipt"] != receipt:
                    raise Conflict("settlement retry conflicts with recorded charge")
                return row
            if row["state"] not in ("reserved", "uncertain"):
                raise Conflict("cancelled request cannot be settled")
            # Actual charges must be recorded even when the estimate was too low.
            conn.execute("UPDATE requests SET state='settled',actual=?,receipt=? WHERE account=? AND id=?",
                         (actual, receipt, account, request))
            self._event(conn, account, request, "settled", actual)
            return self._request(conn, account, request)

    def cancel_before_dispatch(self, account, request):
        """Caller asserts this request was never sent to a provider."""
        label(account)
        label(request)
        with self.connection(transaction=True) as conn:
            row = self._request(conn, account, request)
            if row["state"] == "cancelled":
                return row
            if row["state"] != "reserved":
                raise Conflict("only undispatched reserved requests may be cancelled")
            conn.execute("UPDATE requests SET state='cancelled' WHERE account=? AND id=?", (account, request))
            self._event(conn, account, request, "cancelled", row["reserved"])
            return self._request(conn, account, request)

    def status(self, account):
        label(account)
        with self.connection(transaction=True) as conn:
            return self._status(conn, account)

    def audit(self, account):
        label(account)
        with self.connection(transaction=True) as conn:
            self._status(conn, account)
            return [dict(row) for row in conn.execute("SELECT * FROM events WHERE account=? ORDER BY seq", (account,))]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="budgetlatch.db")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("init", "reserve", "unknown", "settle", "cancel", "status", "audit"):
        sub = commands.add_parser(command)
        sub.add_argument("account")
        if command in ("reserve", "unknown", "settle", "cancel"):
            sub.add_argument("request")
        if command in ("init", "reserve", "settle"):
            sub.add_argument("amount", type=int)
        if command == "reserve":
            sub.add_argument("fingerprint")
        if command == "settle":
            sub.add_argument("receipt")
    args = parser.parse_args(argv)
    try:
        ledger = Ledger(args.db)
        match args.command:
            case "init": result = ledger.create_account(args.account, args.amount)
            case "reserve": result = ledger.reserve(args.account, args.request, args.fingerprint, args.amount)
            case "unknown": result = ledger.mark_uncertain(args.account, args.request)
            case "settle": result = ledger.settle(args.account, args.request, args.amount, args.receipt)
            case "cancel": result = ledger.cancel_before_dispatch(args.account, args.request)
            case "status": result = ledger.status(args.account)
            case "audit": result = ledger.audit(args.account)
        print(json.dumps(result, indent=2))
        return 0
    except BudgetDenied as exc:
        print(json.dumps({"error": str(exc), "kind": "budget_denied"}), file=sys.stderr)
        return 3
    except (ValueError, sqlite3.Error, OSError) as exc:
        print(json.dumps({"error": str(exc), "kind": "invalid_operation"}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
