// SPDX-License-Identifier: MIT

//! Shared by the integration tests: gluon-rs `service-testing`'s throwaway
//! PostgreSQL cluster, this service's store over it and its binary under
//! test, a store that always fails, a callee stub standing in for
//! inventory-service and payment-service, the app over them, and a logger
//! whose lines the test can read back.

#![allow(
    dead_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unused_async_trait_impl
)]

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{Body, Bytes, to_bytes};
use axum::extract::State;
use axum::http::{Method, Request, StatusCode, Uri, header};
use axum::response::IntoResponse;
use order_service::cache::{self, History};
use order_service::clients::{self, Inventory, Payment};
use order_service::config::{PORT_VARIABLE, PostgresConfig};
use order_service::db;
use order_service::domain::{
    OperatorAuth, Order, OrderEvent, OrderItem, OrderStatus, Reconciliation, ReconciliationStatus,
    Rejected, ResolveOutcome, Transitioned,
};
use order_service::server::{self, AppState};
use order_service::store::{OrderWithItems, Orders, PgOrders, StoreError};
use redis_cache::Cache;
use serde_json::{Value, json};
use sqlx::PgPool;
use telemetry::logging::Logger;
use telemetry::metrics::Metrics;
use telemetry::tracing::Tracing;
use time::OffsetDateTime;
use tower::ServiceExt;
use uuid::Uuid;

// Not every test file uses both.
#[allow(unused_imports)]
pub use service_testing::{Postgres, free_port};

/// The order-service binary, on a fresh port.
pub struct Service(service_testing::Service);

impl Service {
    /// Starts it with exactly `extra` (plus its port); does not wait.
    pub fn spawn(extra: &[(&str, String)]) -> Self {
        Self(service_testing::Service::spawn(
            env!("CARGO_BIN_EXE_order-service"),
            PORT_VARIABLE,
            extra,
        ))
    }

    /// Starts it and waits until `/health` answers.
    pub fn start(extra: &[(&str, String)]) -> Self {
        Self(service_testing::Service::start(
            env!("CARGO_BIN_EXE_order-service"),
            PORT_VARIABLE,
            extra,
        ))
    }
}

impl Deref for Service {
    type Target = service_testing::Service;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Service {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// The service's store over `postgres`, migrated as the service migrates it.
pub async fn store(postgres: &Postgres) -> PgOrders {
    PgOrders::new(pool(postgres).await)
}

/// A pool on `postgres`, migrated as the service migrates it.
pub async fn pool(postgres: &Postgres) -> PgPool {
    let env: HashMap<_, _> = postgres.env().into_iter().collect();
    let config = PostgresConfig {
        host: env["POSTGRES_HOST"].clone(),
        port: env["POSTGRES_PORT"].parse().unwrap(),
        database: env["POSTGRES_DB"].clone(),
        user: env["POSTGRES_USER"].clone(),
        password: env["POSTGRES_PASSWORD"].clone(),
    };
    let pool = db::connect(&config).await.unwrap();
    db::migrate(&pool).await.unwrap();
    pool
}

/// A store whose every call fails as a database would, and whose ping fails.
#[derive(Clone, Default)]
pub struct Broken;

fn broken<T>() -> Result<T, StoreError> {
    Err(StoreError::Database(sqlx::Error::PoolClosed))
}

impl Orders for Broken {
    async fn create_authorized(
        &self,
        _: &Order,
        _: &[OrderItem],
    ) -> Result<(OffsetDateTime, OffsetDateTime), StoreError> {
        broken()
    }
    async fn get(&self, _: Uuid) -> Result<Option<OrderWithItems>, StoreError> {
        broken()
    }
    async fn by_customer(&self, _: &str) -> Result<Vec<OrderWithItems>, StoreError> {
        broken()
    }
    async fn all(
        &self,
        _: Option<OrderStatus>,
        _: Option<&str>,
    ) -> Result<Vec<OrderWithItems>, StoreError> {
        broken()
    }
    async fn transition(
        &self,
        _: Uuid,
        _: &OrderEvent,
    ) -> Result<Result<Transitioned, Rejected>, StoreError> {
        broken()
    }
    async fn flag(&self, _: Uuid, _: &str) -> Result<bool, StoreError> {
        broken()
    }
    async fn reconciliations(
        &self,
        _: Option<ReconciliationStatus>,
        _: Option<Uuid>,
    ) -> Result<Vec<Reconciliation>, StoreError> {
        broken()
    }
    async fn resolve(
        &self,
        _: Uuid,
        _: &str,
        _: Option<&str>,
        _: OffsetDateTime,
    ) -> Result<ResolveOutcome, StoreError> {
        broken()
    }
    async fn delete(&self, _: Uuid) -> Result<bool, StoreError> {
        broken()
    }
    async fn ping(&self, _: Duration) -> bool {
        false
    }
}

/// A payment method the callee authorizes.
pub const METHOD: &str = "9b2e4c7a-1f3d-4a6b-8c5e-2d7f0a1b3c4e";
/// One it declines.
pub const DECLINING: &str = "00000000-0000-4000-8000-00000000d402";
/// One it doesn't know.
pub const UNKNOWN: &str = "00000000-0000-4000-8000-000000000404";
/// A SKU it can't reserve.
pub const OUT: &str = "SKU-OUT-1";

/// One request a [`Callee`] got: method, path, body.
pub type Seen = (String, String, String);

/// How a [`Callee`] answers (method, path, body): status and JSON body.
pub type Answer = Arc<dyn Fn(&str, &str, &Value) -> (u16, Value) + Send + Sync>;

/// inventory-service's and payment-service's answers, as tests/contract's stubs give them.
pub fn standard() -> Answer {
    Arc::new(|_, path, body| match path {
        "/inventorys/order-reservations" => {
            let items = body["items"].as_array().cloned().unwrap_or_default();
            let out: Vec<Value> = items
                .iter()
                .filter(|item| {
                    item["sku"]
                        .as_str()
                        .is_some_and(|sku| sku.starts_with("SKU-OUT"))
                })
                .map(|item| {
                    json!({"orderItemId": item["orderItemId"], "sku": item["sku"],
                           "reason": "insufficient_stock"})
                })
                .collect();
            if out.is_empty() {
                (201, body.clone())
            } else {
                (409, json!({"error": "Insufficient stock", "failures": out}))
            }
        }
        "/payments/authorize" => match body["paymentMethodId"].as_str() {
            Some(DECLINING) => (
                402,
                json!({"error": "Payment declined", "reason": "insufficient_funds"}),
            ),
            Some(UNKNOWN) => (404, json!({"error": "Payment method not found"})),
            _ => (
                200,
                json!({"authId": "auth_1", "expiresAt": "2026-11-30T00:00:00Z"}),
            ),
        },
        release if release.ends_with("/release") => (204, Value::Null),
        _ => (404, Value::Null),
    })
}

/// A local HTTP server standing in for both callees: answers as told,
/// recording every request.
#[derive(Clone)]
pub struct Callee {
    /// Its base URL.
    pub url: String,
    seen: Arc<Mutex<Vec<Seen>>>,
    answer: Arc<Mutex<Answer>>,
}

type Shared = (Arc<Mutex<Vec<Seen>>>, Arc<Mutex<Answer>>);

async fn answer(
    State((seen, answer)): State<Shared>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> impl IntoResponse {
    let text = String::from_utf8_lossy(&body).into_owned();
    seen.lock()
        .unwrap()
        .push((method.to_string(), uri.path().to_owned(), text));
    let json = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let answer = answer.lock().unwrap().clone();
    let (status, reply) = answer(method.as_str(), uri.path(), &json);
    let body = if reply.is_null() {
        String::new()
    } else {
        reply.to_string()
    };
    (
        StatusCode::from_u16(status).unwrap(),
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
}

impl Callee {
    /// Started on a fresh port, answering [`standard`].
    pub async fn start() -> Self {
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let answer = Arc::new(Mutex::new(standard()));
        let app = axum::Router::new()
            .fallback(self::answer)
            .with_state((seen.clone(), answer.clone()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, seen, answer }
    }

    /// Answers with `answer` from now on.
    pub fn answering(&self, answer: Answer) {
        *self.answer.lock().unwrap() = answer;
    }

    /// Answers 503 to every path `unavailable` says, otherwise as [`standard`].
    pub fn unavailable(&self, unavailable: fn(&str) -> bool) {
        let standard = standard();
        self.answering(Arc::new(move |method, path, body| {
            if unavailable(path) {
                (503, Value::Null)
            } else {
                standard(method, path, body)
            }
        }));
    }

    /// Every request so far.
    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// The requests whose path or body names `text`.
    pub fn naming(&self, text: &str) -> Vec<Seen> {
        self.seen()
            .into_iter()
            .filter(|(_, path, body)| path.contains(text) || body.contains(text))
            .collect()
    }

    /// How many times `order` was released.
    pub fn releases(&self, order: &str) -> usize {
        let path = format!("/inventorys/order-reservations/{order}/release");
        self.seen().iter().filter(|(_, p, _)| *p == path).count()
    }
}

/// The two clients, both calling `callee`.
pub fn clients(callee: &Callee) -> (Inventory, Payment) {
    let tracing = Tracing::with_exporter(
        "order-service",
        1.0,
        opentelemetry_sdk::trace::InMemorySpanExporter::default(),
    );
    let metrics = Metrics::new("order-service");
    let client = || clients::resilient(tracing.tracer(), &metrics).unwrap();
    (
        Inventory::new(&callee.url, client()),
        Payment::new(&callee.url, client()),
    )
}

/// A history cache on `redis_url`, logging to `logger`.
pub fn history(redis_url: &str, logger: &Logger) -> History {
    let cache = Arc::new(Cache::new(redis_url, cache::TIMEOUT).unwrap());
    History::new(cache, Duration::from_secs(60), logger.clone())
}

/// A Redis URL nothing listens on.
pub fn no_redis() -> String {
    format!("redis://127.0.0.1:{}", free_port())
}

/// The handlers' state over `store`, calling `callee`, caching in `redis_url`.
pub fn state<S: Orders>(store: S, callee: &Callee, redis_url: &str) -> (AppState<S>, LogBuffer) {
    let (logger, buffer) = captured_logger();
    let (inventory, payment) = clients(callee);
    let state = AppState {
        store,
        history: history(redis_url, &logger),
        inventory,
        payment,
        auth: OperatorAuth::new("dev-operator-token", "dev-operator"),
        logger,
    };
    (state, buffer)
}

/// What the app answered: status, content type, body.
pub struct Answered {
    /// The status.
    pub status: u16,
    /// Its Content-Type.
    pub content_type: Option<String>,
    /// The body, as text.
    pub text: String,
}

impl Answered {
    /// The body as JSON.
    pub fn json(&self) -> Value {
        serde_json::from_str(&self.text).unwrap_or_else(|_| panic!("not JSON: {}", self.text))
    }
}

/// Sends one request to the app over `state`.
pub async fn call<S: Orders>(
    state: &AppState<S>,
    method: &str,
    path: &str,
    body: Option<&str>,
    headers: &[(&str, &str)],
) -> Answered {
    let mut request = Request::builder().method(method).uri(path);
    if body.is_some() {
        request = request.header(header::CONTENT_TYPE, "application/json");
    }
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let request = request
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_owned())))
        .unwrap();
    let response = server::app(state.clone()).oneshot(request).await.unwrap();
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap().to_owned());
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    Answered {
        status,
        content_type,
        text: String::from_utf8(bytes.to_vec()).unwrap(),
    }
}

/// The checkout body the contract checks send, with `changes` merged in.
pub fn checkout_body(customer: &str, changes: &Value) -> String {
    let mut body = json!({
        "customerId": customer,
        "items": [
            {"sku": "SKU-MUG-001", "productName": "Gluon Mug", "unitPriceCents": 1299, "quantity": 2},
            {"sku": "SKU-CUP-002", "productName": "Cup", "unitPriceCents": 500, "quantity": 1},
        ],
        "shippingAddress": {"line1": "1 Example Street", "line2": "Flat 2", "city": "London",
                            "postalCode": "EC1A 1AA", "country": "GB"},
        "paymentMethodId": METHOD,
    });
    for (key, value) in changes.as_object().unwrap() {
        body[key] = value.clone();
    }
    body.to_string()
}

/// Places an order for `customer` through the app (201); its body.
pub async fn placed<S: Orders>(state: &AppState<S>, customer: &str) -> Value {
    let answered = call(
        state,
        "POST",
        "/orders",
        Some(&checkout_body(customer, &json!({}))),
        &[],
    )
    .await;
    assert_eq!(answered.status, 201, "{}", answered.text);
    answered.json()
}

/// The order's outbox rows' envelopes, in write order.
pub async fn outbox(pool: &PgPool, order_id: &str) -> Vec<Value> {
    let rows: Vec<(sqlx::types::Json<Value>,)> = sqlx::query_as(
        "select payload from outbox_events where aggregate_id = $1 order by created_at, id",
    )
    .bind(order_id)
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter().map(|(json,)| json.0).collect()
}

/// The eventTypes of the order's outbox rows, in write order.
pub async fn event_types(pool: &PgPool, order_id: &str) -> Vec<String> {
    outbox(pool, order_id)
        .await
        .iter()
        .map(|envelope| envelope["eventType"].as_str().unwrap().to_owned())
        .collect()
}

/// Where a [`captured_logger`] writes.
pub type LogBuffer = Arc<Mutex<Vec<u8>>>;

/// A logger named as the service's, writing to a buffer the test reads back.
pub fn captured_logger() -> (Logger, LogBuffer) {
    let buffer = LogBuffer::default();
    (Logger::to_writer("order-service", buffer.clone()), buffer)
}

/// The lines logged to `buffer` so far, each as JSON.
pub fn log_lines(buffer: &LogBuffer) -> Vec<Value> {
    let text = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
    text.lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// The messages logged to `buffer` so far.
pub fn messages(buffer: &LogBuffer) -> Vec<String> {
    log_lines(buffer)
        .iter()
        .map(|line| line["message"].as_str().unwrap().to_owned())
        .collect()
}
