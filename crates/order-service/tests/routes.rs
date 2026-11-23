// SPDX-License-Identifier: MIT

//! The routes over the Postgres store and a callee stub: checkout's steps,
//! refusals and releases; the reads; cancel and delete; reconciliations; a
//! failing store's 500; the docs.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::assert_is_empty,
    clippy::unused_async_trait_impl
)]

mod support;

use order_service::domain::OrderEvent;
use order_service::server::AppState;
use order_service::store::{Orders, PgOrders};
use serde_json::{Value, json};
use sqlx::PgPool;
use support::{
    Broken, Callee, DECLINING, METHOD, OUT, Postgres, UNKNOWN, call, checkout_body, event_types,
    messages, no_redis, placed, pool, state,
};
use uuid::Uuid;

const MISSING: &str = "00000000-0000-4000-8000-000000000000";
const TOKEN: &str = "Bearer dev-operator-token";

struct Fixture {
    _postgres: Postgres,
    pool: PgPool,
    callee: Callee,
    state: AppState<PgOrders>,
    log: support::LogBuffer,
}

async fn fixture() -> Fixture {
    let postgres = Postgres::start();
    let pool = pool(&postgres).await;
    let callee = Callee::start().await;
    let (state, log) = state(PgOrders::new(pool.clone()), &callee, &no_redis());
    Fixture {
        _postgres: postgres,
        pool,
        callee,
        state,
        log,
    }
}

async fn orders_of(pool: &PgPool, customer: &str) -> i64 {
    sqlx::query_scalar("select count(*) from \"order\" where customer_id = $1")
        .bind(customer)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn checkout(fixture: &Fixture, customer: &str, changes: Value) -> support::Answered {
    let body = checkout_body(customer, &changes);
    call(&fixture.state, "POST", "/orders", Some(&body), &[]).await
}

#[tokio::test]
async fn a_checkout_reserves_authorizes_and_saves_the_order() {
    let f = fixture().await;
    let order = placed(&f.state, "c1").await;
    let id = order["id"].as_str().unwrap();
    assert_eq!(order["status"], "authorized");
    assert_eq!(order["totalCents"], 3098);
    assert_eq!(order["createdAt"], order["updatedAt"]);
    assert_eq!(order["guestEmail"], Value::Null);
    let skus: Vec<_> = order["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["sku"].clone())
        .collect();
    assert_eq!(
        skus,
        [json!("SKU-MUG-001"), json!("SKU-CUP-002")],
        "as sent"
    );
    let asked = f.callee.naming(id);
    assert_eq!(asked.len(), 2);
    assert_eq!(asked[0].1, "/inventorys/order-reservations");
    let reserve: Value = serde_json::from_str(&asked[0].2).unwrap();
    assert_eq!(reserve["items"][0]["orderItemId"], order["items"][0]["id"]);
    let authorize: Value = serde_json::from_str(&asked[1].2).unwrap();
    assert_eq!(
        authorize,
        json!({"orderId": id, "amountCents": 3098, "paymentMethodId": METHOD})
    );
    let found = call(&f.state, "GET", &format!("/orders/{id}"), None, &[]).await;
    assert_eq!(found.status, 200);
    let skus: Vec<_> = found.json()["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["sku"].clone())
        .collect();
    assert_eq!(skus, [json!("SKU-CUP-002"), json!("SKU-MUG-001")], "by SKU");
    assert!(event_types(&f.pool, id).await.is_empty());
}

#[tokio::test]
async fn a_guest_email_is_normalised() {
    let f = fixture().await;
    let answered = checkout(&f, "g1", json!({"email": " Guest@Example.COM "})).await;
    assert_eq!(answered.status, 201);
    assert_eq!(answered.json()["guestEmail"], "guest@example.com");
}

#[tokio::test]
async fn validation_refuses_before_calling_anything() {
    let f = fixture().await;
    let cases = [
        (json!({"items": []}), "Order must have at least one item"),
        (
            json!({"items": [{"sku": "SKU-A", "productName": "A", "unitPriceCents": 100, "quantity": 0}]}),
            "quantity must be positive, got 0 for sku 'SKU-A'",
        ),
        (
            json!({"items": [{"sku": "SKU-A", "productName": "A", "unitPriceCents": 2_147_483_647, "quantity": 2}]}),
            "order total exceeds the wire contract's Int range: 4294967294",
        ),
        (
            json!({"shippingAddress": {"line1": "1", "city": "London", "postalCode": "E1", "country": "gb"}}),
            "country must be an ISO 3166-1 alpha-2 code",
        ),
        (
            json!({"paymentMethodId": "nope"}),
            "paymentMethodId must be a UUID",
        ),
        (
            json!({"email": "nope"}),
            "email must be a valid email address",
        ),
    ];
    for (changes, reason) in cases {
        let answered = checkout(&f, "v1", changes).await;
        assert_eq!(answered.status, 400, "{reason}");
        assert_eq!(answered.json(), json!({"error": reason}));
    }
    assert!(f.callee.seen().is_empty());
    assert_eq!(orders_of(&f.pool, "v1").await, 0);
    assert!(messages(&f.log).contains(&"Invalid checkout request".to_owned()));
}

#[tokio::test]
async fn an_undecodable_body_is_tapirs_text_400() {
    let f = fixture().await;
    for body in ["{\"customerId\":", "{\"customerId\":\"c\"}"] {
        let answered = call(&f.state, "POST", "/orders", Some(body), &[]).await;
        assert_eq!(answered.status, 400);
        assert!(
            answered.text.starts_with("Invalid value for: body ("),
            "{}",
            answered.text
        );
        assert_eq!(
            answered.content_type.as_deref(),
            Some("text/plain; charset=UTF-8")
        );
    }
}

#[tokio::test]
async fn out_of_stock_lines_are_409_and_nothing_is_released() {
    let f = fixture().await;
    let items = json!([
        {"sku": "SKU-A", "productName": "A", "unitPriceCents": 100, "quantity": 1},
        {"sku": OUT, "productName": "B", "unitPriceCents": 100, "quantity": 3},
    ]);
    let answered = checkout(&f, "s1", json!({"items": items})).await;
    assert_eq!(answered.status, 409);
    assert_eq!(
        answered.json(),
        json!({"error": "Insufficient stock",
               "reservationFailures": [{"sku": OUT, "reason": "insufficient_stock"}]})
    );
    assert_eq!(f.callee.seen().len(), 1, "no authorize, no release");
}

fn last_order(callee: &Callee) -> String {
    let (_, _, body) = callee
        .seen()
        .into_iter()
        .find(|(_, path, _)| path == "/inventorys/order-reservations")
        .unwrap();
    let reserve: Value = serde_json::from_str(&body).unwrap();
    reserve["orderId"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn a_decline_is_402_an_unknown_method_400_and_each_releases() {
    let f = fixture().await;
    let declined = checkout(&f, "d1", json!({"paymentMethodId": DECLINING})).await;
    assert_eq!(declined.status, 402);
    assert_eq!(
        declined.json(),
        json!({"error": "Payment declined", "paymentFailure": {"reason": "insufficient_funds"}})
    );
    assert_eq!(f.callee.releases(&last_order(&f.callee)), 1);
    let f = fixture().await;
    let unknown = checkout(&f, "d1", json!({"paymentMethodId": UNKNOWN})).await;
    assert_eq!(unknown.status, 400);
    assert_eq!(unknown.json(), json!({"error": "Payment method not found"}));
    assert_eq!(f.callee.releases(&last_order(&f.callee)), 1);
}

#[tokio::test]
async fn a_callee_unavailable_is_503_and_releases() {
    let f = fixture().await;
    f.callee.unavailable(|path| path == "/payments/authorize");
    let payment = checkout(&f, "u1", json!({})).await;
    assert_eq!(payment.status, 503);
    assert_eq!(
        payment.json(),
        json!({"error": "Payment service unavailable"})
    );
    let order = last_order(&f.callee);
    assert_eq!(f.callee.releases(&order), 1);
    assert_eq!(
        f.callee.naming(&order).len(),
        1 + 4 + 1,
        "authorize retried 3 times"
    );

    let f = fixture().await;
    f.callee
        .unavailable(|path| path == "/inventorys/order-reservations");
    let inventory = checkout(&f, "u1", json!({})).await;
    assert_eq!(inventory.status, 503);
    assert_eq!(
        inventory.json(),
        json!({"error": "Inventory service unavailable"})
    );
    assert_eq!(f.callee.releases(&last_order(&f.callee)), 1);
    assert_eq!(orders_of(&f.pool, "u1").await, 0);
}

#[tokio::test]
async fn a_conflict_naming_no_lines_or_an_unreadable_answer_is_503() {
    let f = fixture().await;
    f.callee
        .answering(std::sync::Arc::new(|_, path, _| match path {
            "/inventorys/order-reservations" => (409, json!({"error": "already released"})),
            _ => (204, Value::Null),
        }));
    assert_eq!(checkout(&f, "x1", json!({})).await.status, 503);
    let standard = support::standard();
    f.callee
        .answering(std::sync::Arc::new(move |method, path, body| match path {
            "/payments/authorize" => (200, json!({"no": "authId"})),
            _ => standard(method, path, body),
        }));
    assert_eq!(checkout(&f, "x1", json!({})).await.status, 503);
    let standard = support::standard();
    f.callee
        .answering(std::sync::Arc::new(move |method, path, body| match path {
            "/payments/authorize" => (400, Value::Null),
            _ => standard(method, path, body),
        }));
    assert_eq!(checkout(&f, "x1", json!({})).await.status, 503);
}

#[tokio::test]
async fn a_failed_release_is_logged_and_the_answer_stands() {
    let f = fixture().await;
    f.callee
        .unavailable(|path| path == "/payments/authorize" || path.ends_with("/release"));
    assert_eq!(checkout(&f, "r1", json!({})).await.status, 503);
    assert!(messages(&f.log).contains(&"Releasing the reservation failed".to_owned()));
}

#[tokio::test]
async fn an_order_not_saved_is_500_and_releases() {
    let callee = Callee::start().await;
    let (state, _) = state(Broken, &callee, &no_redis());
    let answered = call(
        &state,
        "POST",
        "/orders",
        Some(&checkout_body("b1", &json!({}))),
        &[],
    )
    .await;
    assert_eq!(answered.status, 500);
    assert_eq!(
        answered.json(),
        json!({"error": "Order could not be saved"})
    );
    assert_eq!(callee.releases(&last_order(&callee)), 1);
}

#[tokio::test]
async fn reads_find_404_and_refuse_malformed_ids() {
    let f = fixture().await;
    let missing = call(&f.state, "GET", &format!("/orders/{MISSING}"), None, &[]).await;
    assert_eq!(
        (missing.status, missing.json()),
        (404, json!({"error": "Order not found"}))
    );
    let malformed = call(&f.state, "GET", "/orders/not-a-uuid", None, &[]).await;
    assert_eq!(
        (malformed.status, malformed.text.as_str()),
        (400, "Invalid value for: path parameter id")
    );
    let history = call(&f.state, "GET", "/orders", None, &[]).await;
    assert_eq!(
        (history.status, history.text.as_str()),
        (
            400,
            "Invalid value for: query parameter customerId (missing)"
        )
    );
}

#[tokio::test]
async fn a_history_without_redis_is_read_from_postgres_newest_first() {
    let f = fixture().await;
    let first = placed(&f.state, "h1").await;
    let second = placed(&f.state, "h1").await;
    let history = call(&f.state, "GET", "/orders?customerId=h1", None, &[]).await;
    assert_eq!(history.status, 200);
    let ids: Vec<_> = history
        .json()
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].clone())
        .collect();
    assert_eq!(ids, [second["id"].clone(), first["id"].clone()]);
    assert!(
        messages(&f.log)
            .contains(&"Order-history cache read failed - falling back to Postgres".to_owned())
    );
}

#[tokio::test]
async fn the_admin_list_filters_by_status_and_customer() {
    let f = fixture().await;
    let first = placed(&f.state, "a1").await;
    let second = placed(&f.state, "a1").await;
    let id = first["id"].as_str().unwrap();
    assert_eq!(
        call(
            &f.state,
            "PATCH",
            &format!("/orders/{id}"),
            Some(r#"{"status":"cancelled"}"#),
            &[]
        )
        .await
        .status,
        200
    );
    let ids = |answered: support::Answered| -> Vec<Value> {
        answered
            .json()
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o["id"].clone())
            .collect()
    };
    assert_eq!(
        ids(call(&f.state, "GET", "/admin/orders?customerId=a1", None, &[]).await),
        [second["id"].clone(), first["id"].clone()]
    );
    assert_eq!(
        ids(call(
            &f.state,
            "GET",
            "/admin/orders?status=authorized&customerId=a1",
            None,
            &[]
        )
        .await),
        [second["id"].clone()]
    );
    assert_eq!(
        ids(call(
            &f.state,
            "GET",
            "/admin/orders?status=%20&customerId=%20",
            None,
            &[]
        )
        .await)
        .len(),
        2
    );
    let unknown = call(&f.state, "GET", "/admin/orders?status=shipped", None, &[]).await;
    assert_eq!(
        (unknown.status, unknown.json()),
        (400, json!({"error": "Invalid order status: 'shipped'"}))
    );
}

#[tokio::test]
async fn a_cancel_is_once_by_the_customer_or_the_operator() {
    let f = fixture().await;
    let order = placed(&f.state, "k1").await;
    let id = order["id"].as_str().unwrap();
    let path = format!("/orders/{id}");
    let body = Some(r#"{"status":"cancelled"}"#);
    let cancelled = call(&f.state, "PATCH", &path, body, &[]).await;
    assert_eq!(
        (cancelled.status, cancelled.json()["status"].clone()),
        (200, json!("cancelled"))
    );
    let again = call(&f.state, "PUT", &path, body, &[]).await;
    assert_eq!(again.status, 200);
    assert_eq!(event_types(&f.pool, id).await, ["OrderCancelled"]);

    let other = placed(&f.state, "k1").await;
    let id = other["id"].as_str().unwrap();
    let path = format!("/orders/{id}");
    let answered = call(
        &f.state,
        "PUT",
        &path,
        body,
        &[("authorization", "bearer  dev-operator-token ")],
    )
    .await;
    assert_eq!(answered.status, 200);
    let payload = &support::outbox(&f.pool, id).await[0]["payload"];
    assert_eq!(
        payload["actor"],
        json!({"type": "operator", "id": "dev-operator"})
    );
}

#[tokio::test]
async fn a_cancel_it_refuses_changes_nothing() {
    let f = fixture().await;
    let order = placed(&f.state, "n1").await;
    let id = order["id"].as_str().unwrap();
    let path = format!("/orders/{id}");
    let cancel = |status: &'static str| format!(r#"{{"status":"{status}"}}"#);
    let refused = call(
        &f.state,
        "PATCH",
        &path,
        Some(&cancel("cancelled")),
        &[("authorization", "Bearer wrong")],
    )
    .await;
    assert_eq!(
        (refused.status, refused.json()),
        (401, json!({"error": "Invalid operator token"}))
    );
    let unknown = call(&f.state, "PATCH", &path, Some(&cancel("shipped!")), &[]).await;
    assert_eq!(
        (unknown.status, unknown.json()),
        (400, json!({"error": "Invalid order status: 'shipped!'"}))
    );
    let other = call(&f.state, "PATCH", &path, Some(&cancel("confirmed")), &[]).await;
    assert_eq!(
        (other.status, other.json()),
        (
            409,
            json!({"error": "Illegal status transition to 'confirmed'"})
        )
    );
    let missing = call(
        &f.state,
        "PATCH",
        &format!("/orders/{MISSING}"),
        Some(&cancel("cancelled")),
        &[],
    )
    .await;
    assert_eq!(missing.status, 404);
    assert_eq!(
        call(
            &f.state,
            "PATCH",
            "/orders/nope",
            Some(&cancel("cancelled")),
            &[]
        )
        .await
        .status,
        400
    );
    assert_eq!(
        call(&f.state, "PATCH", &path, Some("{\"status\":"), &[])
            .await
            .status,
        400
    );
    assert!(event_types(&f.pool, id).await.is_empty());

    let uuid = Uuid::parse_str(id).unwrap();
    f.state
        .store
        .transition(uuid, &OrderEvent::DispatchConfirmed)
        .await
        .unwrap()
        .unwrap();
    let past = call(&f.state, "PATCH", &path, Some(&cancel("cancelled")), &[]).await;
    assert_eq!(
        (past.status, past.json()),
        (
            409,
            json!({"error": "Illegal status transition to 'cancelled'"})
        )
    );
}

#[tokio::test]
async fn a_delete_is_204_then_404() {
    let f = fixture().await;
    let order = placed(&f.state, "e1").await;
    let path = format!("/orders/{}", order["id"].as_str().unwrap());
    assert_eq!(call(&f.state, "DELETE", &path, None, &[]).await.status, 204);
    let again = call(&f.state, "DELETE", &path, None, &[]).await;
    assert_eq!(
        (again.status, again.json()),
        (404, json!({"error": "Order not found"}))
    );
    assert_eq!(
        call(&f.state, "DELETE", "/orders/nope", None, &[])
            .await
            .status,
        400
    );
}

#[tokio::test]
async fn reconciliations_are_listed_filtered_and_resolved_by_an_operator() {
    let f = fixture().await;
    let order = placed(&f.state, "q1").await;
    let id = order["id"].as_str().unwrap();
    let uuid = Uuid::parse_str(id).unwrap();
    assert!(f.state.store.flag(uuid, "capture_declined").await.unwrap());
    let listed = call(
        &f.state,
        "GET",
        &format!("/reconciliations?orderId={id}&status=open"),
        None,
        &[],
    )
    .await;
    assert_eq!(listed.json()[0]["reason"], "capture_declined");
    let all = call(&f.state, "GET", "/reconciliations", None, &[]).await;
    assert_eq!(all.json().as_array().unwrap().len(), 1);
    let none = call(&f.state, "GET", "/reconciliations?orderId=nope", None, &[]).await;
    assert_eq!(none.json(), json!([]));
    let bad = call(&f.state, "GET", "/reconciliations?status=closed", None, &[]).await;
    assert_eq!(
        (bad.status, bad.json()),
        (
            400,
            json!({"error": "Invalid reconciliation status: only 'resolved' is accepted"})
        )
    );

    let path = format!("/reconciliations/{id}");
    let body = Some(r#"{"status":"resolved","note":" Refunded manually "}"#);
    assert_eq!(call(&f.state, "PATCH", &path, body, &[]).await.status, 401);
    assert_eq!(
        call(
            &f.state,
            "PATCH",
            &path,
            Some(r#"{"status":"open"}"#),
            &[("authorization", TOKEN)]
        )
        .await
        .status,
        400
    );
    let missing = call(
        &f.state,
        "PATCH",
        &format!("/reconciliations/{MISSING}"),
        body,
        &[("authorization", TOKEN)],
    )
    .await;
    assert_eq!(
        (missing.status, missing.json()),
        (404, json!({"error": "Reconciliation flag not found"}))
    );
    assert_eq!(
        call(
            &f.state,
            "PATCH",
            "/reconciliations/nope",
            body,
            &[("authorization", TOKEN)]
        )
        .await
        .text,
        "Invalid value for: path parameter orderId"
    );
    let first = call(&f.state, "PATCH", &path, body, &[("authorization", TOKEN)])
        .await
        .json();
    assert_eq!(
        (
            first["status"].clone(),
            first["note"].clone(),
            first["operatorId"].clone()
        ),
        (
            json!("resolved"),
            json!("Refunded manually"),
            json!("dev-operator")
        )
    );
    let again = call(
        &f.state,
        "PATCH",
        &path,
        Some(r#"{"status":"resolved","note":"x"}"#),
        &[("authorization", TOKEN)],
    )
    .await
    .json();
    assert_eq!(again, first);
}

#[tokio::test]
async fn a_failing_store_is_500_and_logged() {
    let callee = Callee::start().await;
    let (state, log) = state(Broken, &callee, &no_redis());
    for (method, path) in [
        ("GET", format!("/orders/{MISSING}")),
        ("GET", "/orders?customerId=c1".to_owned()),
        ("GET", "/admin/orders".to_owned()),
        ("DELETE", format!("/orders/{MISSING}")),
        ("GET", "/reconciliations".to_owned()),
    ] {
        let answered = call(&state, method, &path, None, &[]).await;
        assert_eq!(
            (answered.status, answered.text.as_str()),
            (500, "Internal server error"),
            "{path}"
        );
    }
    let cancel = call(
        &state,
        "PATCH",
        &format!("/orders/{MISSING}"),
        Some(r#"{"status":"cancelled"}"#),
        &[],
    )
    .await;
    assert_eq!(cancel.status, 500);
    let resolve = call(
        &state,
        "PATCH",
        &format!("/reconciliations/{MISSING}"),
        Some(r#"{"status":"resolved"}"#),
        &[("authorization", TOKEN)],
    )
    .await;
    assert_eq!(resolve.status, 500);
    assert!(messages(&log).contains(&"Request failed".to_owned()));
    let ready = call(&state, "GET", "/health/ready", None, &[]).await;
    assert_eq!(ready.status, 503);
}

#[tokio::test]
async fn the_docs_describe_the_routes_and_events() {
    let callee = Callee::start().await;
    let (state, _) = state(Broken, &callee, &no_redis());
    let page = call(&state, "GET", "/docs/", None, &[]).await;
    assert_eq!(page.status, 200);
    let yaml = call(&state, "GET", "/docs/docs.yaml", None, &[]).await;
    for path in [
        "/orders",
        "/orders/{id}",
        "/admin/orders",
        "/reconciliations/{orderId}",
    ] {
        assert!(yaml.text.contains(path), "{path} in {}", yaml.text);
    }
    let asyncapi = call(&state, "GET", "/docs/asyncapi.json", None, &[])
        .await
        .json();
    let channels = asyncapi["channels"].as_object().unwrap();
    let mut names: Vec<_> = channels.keys().cloned().collect();
    names.sort();
    assert_eq!(
        names,
        ["fulfilment-events", "order-events", "payment-events"]
    );
}
