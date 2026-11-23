# Plan — port order-service (gluon all-Rust Stage A, GREEN)

Spec: [spec.md](./spec.md).

## Phase 1: Port
- [x] Task 1.1: schema (the V1–V16 end state) and domain (status, lifecycle, pricing, address, guest email, cancellation, reconciliation, operator auth) — tests first c4a05c2
- [x] Task 1.2: Postgres store (the Scala SQL; transitions and their outbox, flag and audit rows in one transaction) and the history cache — tests first (throwaway Postgres, Redis) c4a05c2
- [x] Task 1.3: callees (inventory, payment) and routes (checkout, orders, admin, cancel, delete, reconciliations); docs — tests first c4a05c2
- [x] Task 1.4: consumers (payment-events, fulfilment-events, DLQ) — tests first c4a05c2
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md): gluon `bin/verify-port order-service` all MET; CI ≥90% lines — PASS (gluon docs/ports/evidence/order-service/green.txt; contract 29/29 first run, CI 96.99% lines)

## Review

Self-review against the Scala service: SQL, checkout's validation and step order, the
releases on each failure, the transition rules and flags, and log lines followed one for
one; deliberate differences recorded as gluon decision A13. Tests written with the code
(the RED contract was the failing test); no findings.
