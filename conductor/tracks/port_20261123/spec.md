# Spec — port order-service (gluon all-Rust Stage A, GREEN)

## Overview
The Scala order-service in idiomatic Rust on the template's shape: its tables (the Flyway
V1–V16 end state); checkout (validation, pricing, guest email; reserve with
inventory-service, authorize with payment-service, each through purerest's resilience;
release on every failure after the reserve; saved `authorized`); an order, a customer's
history cached in Redis (TTL-only), the admin list; cancel by customer or operator
(`OrderCancelled` to the outbox in its transaction, the cancellation audit), delete; the
`payment-events` and `fulfilment-events` consumers (the lifecycle's transitions,
`OrderConfirmed` / `OrderPaymentFailed` / `OrderCancelled`, release after DispatchFailed,
reconciliation flags, DLQ on a failure to store); reconciliations and operator auth;
readiness on Postgres; CORS; the docs (OpenAPI and AsyncAPI).

## Functional requirements
ORDER-F1–F18, ORDER-D1–D2 (tests/contract `docs/behaviours.md`).

## Acceptance criteria
`scripts/ci.sh` green, ≥90% lines; `bin/verify-port order-service` all MET.

## Out of scope
Wire rows: metrics (db durations, pool, journey and checkout outcomes, flag counter, event
age), log messages, tracing spans and the outbox's traceparent.
