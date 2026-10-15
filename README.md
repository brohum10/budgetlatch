# BudgetLatch

**Reserve AI-call budget before dispatch. Keep uncertainty visible afterward.**

[![Tests](https://github.com/brohum10/budgetlatch/actions/workflows/tests.yml/badge.svg)](https://github.com/brohum10/budgetlatch/actions/workflows/tests.yml)

A local Python/SQLite budget-admission ledger for AI applications. Concurrent callers share one spend limit, retries reuse the same reservation, and ambiguous provider outcomes do not silently release money for another request.

Python 3.12+ · no dependencies · no API keys · local library and CLI

## Try it

Download the repository, open its folder, and run:

```sh
python demo.py
python -m unittest -v
```

The demo submits **60 synthetic requests using 12 worker threads** against a simulated $1 budget. Each asks to reserve $0.10:

| Result | Observed demo value |
| --- | ---: |
| Admitted | 10 |
| Denied | 50 |
| Total held after admission | $1.00 |
| Hold retained for one uncertain call | $0.10 |
| Available after another call settles for $0.06 | $0.04 |

These are reproducible correctness checks, **not real provider spend, savings, or throughput benchmarks**. No model calls occur. Temporary demo databases are automatically cleaned up.

## The problem

Checking `remaining_budget > estimated_cost` before an API call is unsafe if several workers read the same balance. Releasing a reservation on every timeout is also unsafe: a provider may have accepted and charged the request even when the response never arrived.

BudgetLatch makes admission and reservation one transaction. A timed-out or in-flight request retains its hold until the application supplies a reconciled actual charge. There is deliberately no automatic timeout-based release.

## Request lifecycle

```text
new ─ reserve ─ reserved ─ mark_uncertain ─ uncertain ─ settle ─ settled
                   │                                      
                   ├─ settle ─ settled
                   └─ cancel_before_dispatch ─ cancelled
```

`reserved` means budget is held but the caller has not marked dispatch. `uncertain` covers both in-flight calls and unresolved outcomes. Mark a request uncertain **before** sending it to a provider. This is a conservative local state transition, not proof that dispatch happened.

- Reserve: admit only if `amount <= budget - spent - held`.
- Retry: same account, key, fingerprint, and reservation amount returns the existing record with `created: false`; it does not authorize another dispatch.
- Changed retry: reject different request content or reservation amounts under the same key.
- Settle: replace a hold with actual spend, including zero if no charge is confirmed. Identical settlements are no-ops; conflicting amount or receipt retries are rejected.
- Overrun: record actual spend even if it exceeds the estimate or account budget. Report negative availability and block additional admission. Do not hide a real charge to preserve a cosmetic invariant.
- Cancel: release only a reserved request that the caller confirms was never dispatched. Uncertain requests cannot use this shortcut.

All amounts are integer **microdollars**: 1,000,000 units = $1. Floating-point values and booleans are rejected. Individual amounts are bounded to 10^12 units. Account identifiers may include a period (for example `support-2026-10`); rollover and account creation are explicit, never inferred from a clock.

## Library example

```python
import hashlib
from budgetlatch import Ledger

ledger = Ledger("budgetlatch.db")
ledger.create_account("support-2026-10", 10_000_000)

# Hash canonical request content, model/version, pricing version, and token caps.
fingerprint = hashlib.sha256(b"synthetic-request-v1").hexdigest()
reservation = ledger.reserve("support-2026-10", "request-001", fingerprint, 100_000)

if reservation["created"]:
    ledger.mark_uncertain("support-2026-10", "request-001")
    # Dispatch through your own provider adapter here, with a provider idempotency
    # key if supported. On timeout leave the hold intact and reconcile later.
    # This example intentionally makes NO provider call.
    ledger.settle("support-2026-10", "request-001", 60_000, "simulated-receipt-001")

print(ledger.status("support-2026-10"))
```

The fingerprint above and its settlement are synthetic. A real adapter must compute a defensible maximum cost, enforce output-token caps, use the returned `created` flag, obtain billing evidence, and settle the actual amount. Hashes are compared, not recomputed against secret prompts. The ledger does not validate receipt authenticity or contact the provider.

**Crash boundary:** a crash between reservation and dispatch can strand a hold. A crash after dispatch but before settlement can leave an unknown charge. Inspect and reconcile rather than automatically replaying the API call. Exactly-once external execution is not provided. Even identical receipt labels on different request keys are not deduplicated; use stable request keys and enforce provider receipt semantics in your adapter.

## CLI

```sh
python budgetlatch.py --db demo.db init support-2026-10 1000000
python budgetlatch.py --db demo.db reserve support-2026-10 call-1 100000 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
python budgetlatch.py --db demo.db unknown support-2026-10 call-1
python budgetlatch.py --db demo.db settle support-2026-10 call-1 60000 simulated-receipt
python budgetlatch.py --db demo.db status support-2026-10
python budgetlatch.py --db demo.db audit support-2026-10
```

The all-`a` hash is a placeholder, not a real request digest. To abandon a request **before dispatch**, use `cancel ACCOUNT REQUEST`. Once marked unknown, reconcile with `settle`, using zero only if absence of a charge has been confirmed. Existing account limits cannot be silently changed by rerunning `init`.

Successful commands print JSON and exit `0`. Insufficient budget exits `3`; invalid input, conflicts, or storage errors exit `2` with JSON on stderr. Argument parsing errors also exit `2` with argparse text. The CLI does not launch a hosted service or pay anything.

## Storage and concurrency

Three tables store accounts, request state, and an ordered audit trail. Every state mutation and its audit event commit together. Admission takes a SQLite `BEGIN IMMEDIATE` writer lock before reading balances. WAL mode and `synchronous=FULL` are configured; connections are closed after every operation, with a 20-second busy timeout.

Balances are derived from request records rather than maintained as a second mutable counter. This simplifies invariants and rollback at the cost of scanning an account's requests on admission/status. Status and audit reads also use a writer transaction for simple consistent snapshots, trading read concurrency for clarity. This is a small single-host reference implementation, not a high-throughput billing platform.

## Verification

The 32 tests run locally and in GitHub Actions on Python 3.12 and 3.14. They include:

- 60 competing admissions and 40 retries of one idempotency key.
- Eight separate interpreter processes competing for two reservation slots.
- Competing settlements and cancel/settle races.
- A subprocess terminating without committing, followed by recovery checks.
- Injected audit-write failures proving reservation and settlement rollback.
- 100 randomized lifecycle operations checked against an independent accounting tally.
- Reopening the database, unknown outcomes, zero-charge reconciliation, budget overruns, account isolation, input validation, and CLI exit codes.

## Boundaries and limitations

- This is AI infrastructure, not an AI model. No provider SDK or live-model integration is included or tested.
- A budget gate cannot prevent charges from callers that bypass it, underestimate costs, or ignore token limits. Recorded overruns may make the balance negative.
- There is no authentication, encryption, tamper-proof audit, administrative approval, or multi-host consensus. Account names provide data scoping, not tenant security. Use only trusted local callers and a protected local filesystem.
- Cancellation depends on truthful caller behavior. A database state cannot independently prove that an external request was never sent.
- No lease expiry, background reconciliation, refunds, post-settlement corrections, account top-ups, currency conversion, or automatic monthly reset. Unresolved holds require explicit investigation.
- SQLite serializes writes. Do not place this database on a shared network filesystem; do not treat the concurrency tests as a production scaling claim.
- Synthetic demo results establish local accounting behavior, not provider billing correctness. A deployment needs adapter-specific integration tests and operational review.

## License

MIT. See [LICENSE](LICENSE).
