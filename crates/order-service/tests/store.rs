// SPDX-License-Identifier: MIT

//! The Postgres store over a throwaway cluster: an authorized checkout stored
//! and read back, the lists' orders, the compare-and-set and what it writes
//! with a transition, flags first-wins, resolving, deleting.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::assert_is_empty,
    clippy::unused_async_trait_impl
)]

mod support;

use std::time::Duration;

use order_service::domain::{
    Address, Order, OrderEvent, OrderItem, OrderStatus, ReconciliationStatus, Rejected,
    ResolveOutcome,
};
use order_service::store::{Orders, PgOrders};
use sqlx::PgPool;
use support::{Postgres, event_types, outbox, pool};
use time::OffsetDateTime;
use time::macros::datetime;
use uuid::Uuid;

fn order(customer: &str) -> (Order, Vec<OrderItem>) {
    let now = OffsetDateTime::now_utc();
    let order = Order {
        id: Uuid::new_v4(),
        customer_id: customer.to_owned(),
        total_cents: 3098,
        status: OrderStatus::Authorized,
        created_at: now,
        updated_at: now,
        shipping_address: Some(Address {
            line1: "1 Example Street".to_owned(),
            line2: None,
            city: "London".to_owned(),
            postal_code: "EC1A 1AA".to_owned(),
            country: "GB".to_owned(),
        }),
        guest_email: Some("guest@example.com".to_owned()),
    };
    let item = |sku: &str, price, quantity| OrderItem {
        id: Uuid::new_v4(),
        sku: sku.to_owned(),
        product_name: sku.to_owned(),
        unit_price_cents: price,
        quantity,
    };
    (
        order,
        vec![item("SKU-MUG-001", 1299, 2), item("SKU-CUP-002", 500, 1)],
    )
}

async fn stored(store: &PgOrders, customer: &str) -> Order {
    let (mut order, items) = order(customer);
    let (created_at, updated_at) = store.create_authorized(&order, &items).await.unwrap();
    order.created_at = created_at;
    order.updated_at = updated_at;
    order
}

async fn setup() -> (Postgres, PgPool, PgOrders) {
    let postgres = Postgres::start();
    let pool = pool(&postgres).await;
    (postgres, pool.clone(), PgOrders::new(pool))
}

#[tokio::test]
async fn an_authorized_order_is_stored_with_its_lines_by_sku() {
    let (_postgres, _, store) = setup().await;
    let order = stored(&store, "c1").await;
    assert_eq!(order.created_at, order.updated_at);
    let (found, items) = store.get(order.id).await.unwrap().unwrap();
    assert_eq!(found, order);
    let skus: Vec<_> = items.iter().map(|item| item.sku.as_str()).collect();
    assert_eq!(skus, ["SKU-CUP-002", "SKU-MUG-001"]);
    assert_eq!(store.get(Uuid::new_v4()).await.unwrap(), None);
}

#[tokio::test]
async fn histories_and_the_admin_list_are_newest_first_and_filtered() {
    let (_postgres, _, store) = setup().await;
    let first = stored(&store, "c1").await;
    let second = stored(&store, "c1").await;
    let other = stored(&store, "c2").await;
    let ids = |orders: Vec<(Order, Vec<OrderItem>)>| -> Vec<Uuid> {
        orders.into_iter().map(|(order, _)| order.id).collect()
    };
    assert_eq!(
        ids(store.by_customer("c1").await.unwrap()),
        [second.id, first.id]
    );
    assert!(store.by_customer("nobody").await.unwrap().is_empty());
    assert_eq!(
        ids(store.all(None, None).await.unwrap()),
        [other.id, second.id, first.id]
    );
    store
        .transition(first.id, &OrderEvent::CustomerCancelled)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ids(store
            .all(Some(OrderStatus::Authorized), Some("c1"))
            .await
            .unwrap()),
        [second.id]
    );
    let (_, items) = &store.all(None, Some("c2")).await.unwrap()[0];
    assert_eq!(items.len(), 2);
}

#[tokio::test]
async fn a_transition_writes_its_event_its_flag_and_its_audit_with_it() {
    let (_postgres, pool, store) = setup().await;
    let order = stored(&store, "c1").await;
    let id = order.id.to_string();
    let moved = store
        .transition(
            order.id,
            &OrderEvent::OperatorCancelled("dev-operator".to_owned()),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            moved.order_id,
            moved.customer_id.as_str(),
            moved.total_cents,
            moved.from
        ),
        (order.id, "c1", 3098, OrderStatus::Authorized)
    );
    let [envelope] = <[_; 1]>::try_from(outbox(&pool, &id).await).unwrap();
    assert_eq!(envelope["eventType"], "OrderCancelled");
    assert_eq!(envelope["source"], "order-service");
    assert_eq!(envelope["correlationId"], id.as_str());
    assert_eq!(envelope["payload"]["reason"], "operator");
    assert_eq!(
        envelope["payload"]["actor"],
        serde_json::json!({"type": "operator", "id": "dev-operator"})
    );
    let audit: (String, String, String) = sqlx::query_as(
        "select actor_type, actor_id, reason from order_cancellation_audit where order_id = $1",
    )
    .bind(order.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        audit,
        (
            "operator".to_owned(),
            "dev-operator".to_owned(),
            "operator".to_owned()
        )
    );
    assert_eq!(
        store
            .transition(order.id, &OrderEvent::CustomerCancelled)
            .await
            .unwrap(),
        Err(Rejected::Illegal {
            from: OrderStatus::Cancelled,
            to: OrderStatus::Cancelled
        })
    );
    assert_eq!(
        store
            .transition(Uuid::new_v4(), &OrderEvent::DispatchConfirmed)
            .await
            .unwrap(),
        Err(Rejected::UnknownOrder)
    );
    assert_eq!(event_types(&pool, &id).await, ["OrderCancelled"]);
}

#[tokio::test]
async fn each_event_publishes_as_scala_does_and_a_failed_capture_flags() {
    let (_postgres, pool, store) = setup().await;
    let confirmed = stored(&store, "c1").await;
    let shipped = stored(&store, "c1").await;
    let failed = stored(&store, "c1").await;
    let expired = stored(&store, "c1").await;
    let go = |id, event| {
        let store = store.clone();
        async move { store.transition(id, &event).await.unwrap().unwrap() }
    };
    go(confirmed.id, OrderEvent::PaymentCaptured).await;
    go(shipped.id, OrderEvent::DispatchConfirmed).await;
    let moved = go(shipped.id, OrderEvent::PaymentCaptured).await;
    assert_eq!(moved.from, OrderStatus::Dispatched);
    go(
        failed.id,
        OrderEvent::PaymentFailed("capture_declined".to_owned()),
    )
    .await;
    go(expired.id, OrderEvent::AuthorizationExpired).await;
    for (order, kind) in [
        (&confirmed, "OrderConfirmed"),
        (&shipped, "OrderConfirmed"),
        (&failed, "OrderPaymentFailed"),
        (&expired, "OrderCancelled"),
    ] {
        assert_eq!(event_types(&pool, &order.id.to_string()).await, [kind]);
    }
    let payload = &outbox(&pool, &failed.id.to_string()).await[0]["payload"];
    assert_eq!(payload["reason"], "capture_declined");
    let flags = store.reconciliations(None, Some(failed.id)).await.unwrap();
    assert_eq!(flags.len(), 1);
    assert_eq!(flags[0].reason, "capture_declined");
    let (status,) = store
        .get(expired.id)
        .await
        .unwrap()
        .map(|(o, _)| (o.status,))
        .unwrap();
    assert_eq!(status, OrderStatus::Cancelled);
}

#[tokio::test]
async fn flags_are_first_wins_listed_newest_first_and_resolved_once() {
    let (_postgres, _, store) = setup().await;
    let first = stored(&store, "c1").await;
    let second = stored(&store, "c1").await;
    assert!(store.flag(first.id, "one").await.unwrap());
    assert!(!store.flag(first.id, "two").await.unwrap());
    assert!(!store.flag(Uuid::new_v4(), "nobody").await.unwrap());
    assert!(store.flag(second.id, "three").await.unwrap());
    let reasons = |flags: Vec<order_service::domain::Reconciliation>| -> Vec<String> {
        flags.into_iter().map(|flag| flag.reason).collect()
    };
    assert_eq!(
        reasons(store.reconciliations(None, None).await.unwrap()),
        ["three", "one"]
    );
    let at = datetime!(2026-11-23 10:00:00 UTC);
    let ResolveOutcome::Resolved(resolved) = store
        .resolve(first.id, "dev-operator", Some("done"), at)
        .await
        .unwrap()
    else {
        panic!("not resolved");
    };
    assert_eq!(resolved.status, ReconciliationStatus::Resolved);
    assert_eq!(resolved.operator_id.as_deref(), Some("dev-operator"));
    assert_eq!(resolved.resolved_at, Some(at));
    assert_eq!(
        store.resolve(first.id, "other", None, at).await.unwrap(),
        ResolveOutcome::AlreadyResolved(resolved)
    );
    assert_eq!(
        store.resolve(Uuid::new_v4(), "op", None, at).await.unwrap(),
        ResolveOutcome::NotFound
    );
    assert_eq!(
        reasons(
            store
                .reconciliations(Some(ReconciliationStatus::Open), None)
                .await
                .unwrap()
        ),
        ["three"]
    );
}

#[tokio::test]
async fn a_deleted_order_is_gone_and_the_store_answers_pings() {
    let (postgres, _, store) = setup().await;
    let order = stored(&store, "c1").await;
    assert!(store.delete(order.id).await.unwrap());
    assert!(!store.delete(order.id).await.unwrap());
    assert_eq!(store.get(order.id).await.unwrap(), None);
    assert!(store.ping(Duration::from_secs(2)).await);
    postgres.stop();
    assert!(!store.ping(Duration::from_secs(2)).await);
}
