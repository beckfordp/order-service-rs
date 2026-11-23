// SPDX-License-Identifier: MIT

//! The two consumers' routes, less the stream, over the Postgres store and a
//! callee stub: which envelopes they act on, what each does to the order, the
//! flags a rejected event writes, and dispatch failure's release. The
//! consumers against a broker are tests/contract's.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::assert_is_empty,
    clippy::unused_async_trait_impl
)]

mod support;

use kafka_event_consumer::envelope::{Envelope, Routes};
use order_service::consumers::{LAG_INTERVAL, settings};
use order_service::domain::{OrderEvent, OrderStatus};
use order_service::events::{
    EventsError, FULFILMENT_EVENTS, FULFILMENT_GROUP, FulfilmentEvent, FulfilmentEvents,
    PAYMENT_EVENTS, PAYMENT_GROUP, PaymentEvent, PaymentEvents,
};
use order_service::store::{Orders, PgOrders};
use serde_json::{Value, json};
use support::{
    Broken, Callee, Postgres, captured_logger, clients, messages, no_redis, placed, pool, state,
};
use uuid::Uuid;

fn envelope(event_type: &str, payload: &Value) -> Envelope {
    serde_json::from_value(json!({
        "eventId": "e1",
        "eventType": event_type,
        "timestamp": "2026-11-20T10:00:00Z",
        "source": "test",
        "correlationId": "c1",
        "payload": payload,
    }))
    .unwrap()
}

const ORDER: &str = "3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90";
const AT: &str = "2026-11-20T10:00:00Z";

fn order() -> Uuid {
    Uuid::parse_str(ORDER).unwrap()
}

#[tokio::test]
async fn payment_events_decode_and_other_types_are_skipped() {
    let (logger, _) = captured_logger();
    let route = PaymentEvents::new(Broken, logger);
    assert!(
        route
            .decode(&envelope("PaymentAuthorized", &json!({})))
            .is_none()
    );
    let decode = |kind, payload: Value| route.decode(&envelope(kind, &payload)).unwrap().unwrap();
    assert_eq!(
        decode(
            "PaymentCaptured",
            json!({"orderId": ORDER, "amountCents": 1, "timestamp": AT})
        ),
        PaymentEvent::Captured(order())
    );
    assert_eq!(
        decode(
            "PaymentAuthorizationExpired",
            json!({"orderId": ORDER, "timestamp": AT})
        ),
        PaymentEvent::Expired(order())
    );
    assert_eq!(
        decode(
            "PaymentFailed",
            json!({"orderId": ORDER, "paymentId": "p1", "amountCents": 1,
                   "reason": "capture_declined", "timestamp": AT})
        ),
        PaymentEvent::Failed(order(), "capture_declined".to_owned())
    );
    let poison = route.decode(&envelope("PaymentCaptured", &json!({"orderId": ORDER})));
    assert!(
        matches!(poison, Some(Err(EventsError::Payload(_)))),
        "{poison:?}"
    );
    let bad_id = route.decode(&envelope(
        "PaymentAuthorizationExpired",
        &json!({"orderId": "nope", "timestamp": AT}),
    ));
    assert!(
        matches!(bad_id, Some(Err(EventsError::OrderId(_)))),
        "{bad_id:?}"
    );
}

#[tokio::test]
async fn fulfilment_events_decode_and_a_confirmation_needs_its_items() {
    let callee = Callee::start().await;
    let (logger, _) = captured_logger();
    let route = FulfilmentEvents::new(Broken, clients(&callee).0, logger);
    assert!(
        route
            .decode(&envelope("DispatchRequested", &json!({})))
            .is_none()
    );
    let confirmed = json!({"orderId": ORDER, "trackingNo": "T1", "items": []});
    assert_eq!(
        route
            .decode(&envelope("DispatchConfirmed", &confirmed))
            .unwrap()
            .unwrap(),
        FulfilmentEvent::Confirmed(order())
    );
    let failed = json!({"orderId": ORDER, "reason": "lost"});
    assert_eq!(
        route
            .decode(&envelope("DispatchFailed", &failed))
            .unwrap()
            .unwrap(),
        FulfilmentEvent::Failed(order())
    );
    let no_items = route.decode(&envelope(
        "DispatchConfirmed",
        &json!({"orderId": ORDER, "trackingNo": "T1"}),
    ));
    assert!(
        matches!(no_items, Some(Err(EventsError::NoItems))),
        "{no_items:?}"
    );
    assert_eq!(
        EventsError::NoItems.to_string(),
        "Missing required field at 'items'"
    );
}

struct Fixture {
    _postgres: Postgres,
    store: PgOrders,
    callee: Callee,
    payments: PaymentEvents<PgOrders>,
    fulfilment: FulfilmentEvents<PgOrders>,
    log: support::LogBuffer,
    app: order_service::server::AppState<PgOrders>,
}

async fn fixture() -> Fixture {
    let postgres = Postgres::start();
    let store = PgOrders::new(pool(&postgres).await);
    let callee = Callee::start().await;
    let (app, _) = state(store.clone(), &callee, &no_redis());
    let (logger, log) = captured_logger();
    Fixture {
        _postgres: postgres,
        payments: PaymentEvents::new(store.clone(), logger.clone()),
        fulfilment: FulfilmentEvents::new(store.clone(), clients(&callee).0, logger),
        store,
        callee,
        log,
        app,
    }
}

async fn an_order(f: &Fixture) -> Uuid {
    let order = placed(&f.app, "ev1").await;
    Uuid::parse_str(order["id"].as_str().unwrap()).unwrap()
}

async fn status(f: &Fixture, id: Uuid) -> OrderStatus {
    f.store.get(id).await.unwrap().unwrap().0.status
}

async fn flags(f: &Fixture, id: Uuid) -> Vec<String> {
    f.store
        .reconciliations(None, Some(id))
        .await
        .unwrap()
        .into_iter()
        .map(|flag| flag.reason)
        .collect()
}

#[tokio::test]
async fn payment_events_move_the_order_and_a_rejection_flags_it_once() {
    let f = fixture().await;
    let captured = an_order(&f).await;
    f.payments
        .handle("e1", &PaymentEvent::Captured(captured))
        .await
        .unwrap();
    assert_eq!(status(&f, captured).await, OrderStatus::Confirmed);
    f.payments
        .handle("e2", &PaymentEvent::Captured(captured))
        .await
        .unwrap();
    assert!(
        flags(&f, captured).await.is_empty(),
        "a duplicate isn't flagged"
    );
    f.payments
        .handle("e3", &PaymentEvent::Expired(captured))
        .await
        .unwrap();
    f.payments
        .handle("e4", &PaymentEvent::Expired(captured))
        .await
        .unwrap();
    assert_eq!(
        flags(&f, captured).await,
        ["PaymentAuthorizationExpired rejected from confirmed"]
    );

    let failed = an_order(&f).await;
    f.payments
        .handle(
            "e5",
            &PaymentEvent::Failed(failed, "capture_declined".to_owned()),
        )
        .await
        .unwrap();
    assert_eq!(status(&f, failed).await, OrderStatus::PaymentFailed);
    assert_eq!(flags(&f, failed).await, ["capture_declined"]);
    assert_eq!(
        f.callee.releases(&failed.to_string()),
        0,
        "no release on a failed capture"
    );

    let expired = an_order(&f).await;
    f.payments
        .handle("e6", &PaymentEvent::Expired(expired))
        .await
        .unwrap();
    assert_eq!(status(&f, expired).await, OrderStatus::Cancelled);

    f.payments
        .handle("e7", &PaymentEvent::Captured(Uuid::new_v4()))
        .await
        .unwrap();
    let logged = messages(&f.log);
    for message in [
        "Order status updated",
        "Order not awaiting this transition - ignoring",
        "Unknown orderId - ignoring",
    ] {
        assert!(logged.contains(&message.to_owned()), "{message}");
    }
}

#[tokio::test]
async fn a_dispatch_moves_the_order_and_a_failure_releases_its_stock() {
    let f = fixture().await;
    let shipped = an_order(&f).await;
    f.fulfilment
        .handle("e1", &FulfilmentEvent::Confirmed(shipped))
        .await
        .unwrap();
    assert_eq!(status(&f, shipped).await, OrderStatus::Dispatched);
    f.fulfilment
        .handle("e2", &FulfilmentEvent::Failed(shipped))
        .await
        .unwrap();
    assert_eq!(
        flags(&f, shipped).await,
        ["DispatchFailed rejected from dispatched"]
    );
    assert_eq!(f.callee.releases(&shipped.to_string()), 0);

    let failed = an_order(&f).await;
    f.fulfilment
        .handle("e3", &FulfilmentEvent::Failed(failed))
        .await
        .unwrap();
    assert_eq!(status(&f, failed).await, OrderStatus::DispatchFailed);
    assert_eq!(f.callee.releases(&failed.to_string()), 1);
    f.fulfilment
        .handle("e4", &FulfilmentEvent::Failed(failed))
        .await
        .unwrap();
    assert_eq!(
        f.callee.releases(&failed.to_string()),
        1,
        "a duplicate doesn't release again"
    );
    assert!(flags(&f, failed).await.is_empty());
}

#[tokio::test]
async fn a_failed_release_flags_the_order() {
    let f = fixture().await;
    let failed = an_order(&f).await;
    f.callee.unavailable(|path| path.ends_with("/release"));
    f.fulfilment
        .handle("e1", &FulfilmentEvent::Failed(failed))
        .await
        .unwrap();
    let [reason] = <[_; 1]>::try_from(flags(&f, failed).await).unwrap();
    assert!(
        reason.starts_with("inventory release failed after DispatchFailed: "),
        "{reason}"
    );
    assert!(messages(&f.log).contains(
        &"Releasing reservations for a dispatch failure failed - reconciliation flagged".to_owned()
    ));
}

#[tokio::test]
async fn a_failing_store_is_an_error_to_retry() {
    let callee = Callee::start().await;
    let (logger, _) = captured_logger();
    let payments = PaymentEvents::new(Broken, logger.clone());
    let result = payments
        .handle("e1", &PaymentEvent::Captured(order()))
        .await;
    assert!(matches!(result, Err(EventsError::Store(_))), "{result:?}");
    let fulfilment = FulfilmentEvents::new(Broken, clients(&callee).0, logger);
    let result = fulfilment
        .handle("e1", &FulfilmentEvent::Failed(order()))
        .await;
    assert!(matches!(result, Err(EventsError::Store(_))), "{result:?}");
}

#[tokio::test]
async fn a_failed_release_whose_flag_fails_is_logged() {
    // The order moves in Postgres; the flag's write goes to a store that fails.
    #[derive(Clone)]
    struct FlagFails(PgOrders);
    impl Orders for FlagFails {
        async fn create_authorized(
            &self,
            o: &order_service::domain::Order,
            i: &[order_service::domain::OrderItem],
        ) -> Result<(time::OffsetDateTime, time::OffsetDateTime), order_service::store::StoreError>
        {
            self.0.create_authorized(o, i).await
        }
        async fn get(
            &self,
            id: Uuid,
        ) -> Result<Option<order_service::store::OrderWithItems>, order_service::store::StoreError>
        {
            self.0.get(id).await
        }
        async fn by_customer(
            &self,
            c: &str,
        ) -> Result<Vec<order_service::store::OrderWithItems>, order_service::store::StoreError>
        {
            self.0.by_customer(c).await
        }
        async fn all(
            &self,
            s: Option<OrderStatus>,
            c: Option<&str>,
        ) -> Result<Vec<order_service::store::OrderWithItems>, order_service::store::StoreError>
        {
            self.0.all(s, c).await
        }
        async fn transition(
            &self,
            id: Uuid,
            e: &OrderEvent,
        ) -> Result<
            Result<order_service::domain::Transitioned, order_service::domain::Rejected>,
            order_service::store::StoreError,
        > {
            self.0.transition(id, e).await
        }
        async fn flag(&self, _: Uuid, _: &str) -> Result<bool, order_service::store::StoreError> {
            Err(order_service::store::StoreError::Database(
                sqlx::Error::PoolClosed,
            ))
        }
        async fn reconciliations(
            &self,
            s: Option<order_service::domain::ReconciliationStatus>,
            o: Option<Uuid>,
        ) -> Result<Vec<order_service::domain::Reconciliation>, order_service::store::StoreError>
        {
            self.0.reconciliations(s, o).await
        }
        async fn resolve(
            &self,
            o: Uuid,
            op: &str,
            n: Option<&str>,
            at: time::OffsetDateTime,
        ) -> Result<order_service::domain::ResolveOutcome, order_service::store::StoreError>
        {
            self.0.resolve(o, op, n, at).await
        }
        async fn delete(&self, id: Uuid) -> Result<bool, order_service::store::StoreError> {
            self.0.delete(id).await
        }
        async fn ping(&self, t: std::time::Duration) -> bool {
            self.0.ping(t).await
        }
    }
    let f = fixture().await;
    let failed = an_order(&f).await;
    f.callee.unavailable(|path| path.ends_with("/release"));
    let (logger, log) = captured_logger();
    let route = FulfilmentEvents::new(FlagFails(f.store.clone()), clients(&f.callee).0, logger);
    route
        .handle("e1", &FulfilmentEvent::Failed(failed))
        .await
        .unwrap();
    assert!(messages(&log).contains(
        &"Inventory release failed AND the reconciliation flag could not be written".to_owned()
    ));
}

#[test]
fn the_consumers_read_their_topics_as_their_groups() {
    for (topic, group) in [
        (PAYMENT_EVENTS, PAYMENT_GROUP),
        (FULFILMENT_EVENTS, FULFILMENT_GROUP),
    ] {
        let settings = settings("kafka:9092", topic, group).unwrap();
        assert_eq!(settings.bootstrap, "kafka:9092");
        assert_eq!(settings.group_id.as_str(), group);
        assert_eq!(settings.topic.as_str(), topic);
        assert_eq!(settings.lag_interval, LAG_INTERVAL);
    }
    assert!(settings("kafka:9092", "bad topic!", PAYMENT_GROUP).is_err());
}
