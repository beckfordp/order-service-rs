// SPDX-License-Identifier: MIT

//! The routes, as the Scala `OrderRoutes` and `ReconciliationRoutes`: the path,
//! query and body decoded first (tapir's plain-text 400), then the rules (400
//! with the JSON reason), then the callees and the store. A store failure is a
//! 500.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use service_basics::jvm::java_instant;
use service_basics::text_error::TextError;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::clients::{Authorization, Reservation, ReservationLine, StockFailure};
use crate::domain::{
    Address, ItemsError, Line, NewItem, Order, OrderEvent, OrderItem, OrderStatus, Reconciliation,
    ReconciliationStatus, Rejected, ResolveOutcome, guest_email, parse_uuid, total_cents,
    validate_items,
};
use crate::server::AppState;
use crate::store::{OrderWithItems, Orders, StoreError};

/// One line to buy.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateOrderItemRequest {
    /// Catalog SKU.
    pub sku: String,
    /// Name shown on the order.
    pub product_name: String,
    /// Unit price in cents, at least 0.
    pub unit_price_cents: i32,
    /// Units to buy, at least 1.
    pub quantity: i32,
}

/// A checkout: what to buy, where to ship it and how to pay.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateOrderRequest {
    /// Id (UUID) of the customer placing the order.
    pub customer_id: String,
    /// Lines to buy; at least one.
    pub items: Vec<CreateOrderItemRequest>,
    /// Where to ship the order.
    pub shipping_address: Address,
    /// Saved payment method (UUID) to authorize against.
    pub payment_method_id: String,
    /// Guest checkout email (ADM-10): where order emails go when the customerId has no
    /// account. Optional; blank means absent.
    pub email: Option<String>,
}

/// A status change.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct UpdateOrderRequest {
    /// Target status; only `cancelled` is accepted.
    pub status: String,
}

/// One line of an order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrderItemResponse {
    /// Line id (UUID).
    pub id: String,
    /// Catalog SKU.
    pub sku: String,
    /// Name shown on the order.
    pub product_name: String,
    /// Unit price in cents.
    pub unit_price_cents: i32,
    /// Units bought.
    pub quantity: i32,
}

impl From<&OrderItem> for OrderItemResponse {
    fn from(item: &OrderItem) -> Self {
        Self {
            id: item.id.to_string(),
            sku: item.sku.clone(),
            product_name: item.product_name.clone(),
            unit_price_cents: item.unit_price_cents,
            quantity: item.quantity,
        }
    }
}

/// An order with its line items (also the cached history's entries, as the
/// Scala service caches them).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OrderResponse {
    /// Order id (UUID).
    pub id: String,
    /// Id (UUID) of the customer who placed the order.
    pub customer_id: String,
    /// Sum of the line totals, in cents.
    pub total_cents: i32,
    /// Lifecycle status.
    pub status: String,
    /// The order's lines, by SKU.
    pub items: Vec<OrderItemResponse>,
    /// When the order was placed (UTC).
    pub created_at: String,
    /// When the order last changed (UTC).
    pub updated_at: String,
    /// Where to ship; absent on orders placed before US-11.1.
    pub shipping_address: Option<Address>,
    /// Guest checkout email (ADM-10); absent for signed-in customers.
    pub guest_email: Option<String>,
}

impl OrderResponse {
    fn of(order: &Order, items: &[OrderItem]) -> Self {
        Self {
            id: order.id.to_string(),
            customer_id: order.customer_id.clone(),
            total_cents: order.total_cents,
            status: order.status.as_str().to_owned(),
            items: items.iter().map(Into::into).collect(),
            created_at: java_instant(order.created_at),
            updated_at: java_instant(order.updated_at),
            shipping_address: order.shipping_address.clone(),
            guest_email: order.guest_email.clone(),
        }
    }
}

impl From<OrderWithItems> for OrderResponse {
    fn from((order, items): OrderWithItems) -> Self {
        Self::of(&order, &items)
    }
}

/// Error body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
pub struct ErrorResponse {
    /// Human-readable reason.
    pub error: String,
}

/// The provider's decline detail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
pub struct PaymentFailure {
    /// Why the payment provider declined.
    pub reason: String,
}

/// Error body for a declined payment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PaymentDeclinedResponse {
    /// Human-readable reason.
    pub error: String,
    /// The provider's decline detail.
    pub payment_failure: PaymentFailure,
}

/// A line that couldn't be reserved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
pub struct ReservationFailureResponse {
    /// SKU of the line.
    pub sku: String,
    /// Why it couldn't be reserved.
    pub reason: String,
}

/// Error body for lines that couldn't be reserved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InsufficientStockResponse {
    /// Human-readable reason.
    pub error: String,
    /// Every line that couldn't be reserved.
    pub reservation_failures: Vec<ReservationFailureResponse>,
}

/// One manual-reconciliation flag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationResponse {
    /// Order the flag belongs to.
    pub order_id: String,
    /// Why manual reconciliation is needed.
    pub reason: String,
    /// `open` or `resolved`.
    pub status: String,
    /// When the flag was recorded.
    pub created_at: String,
    /// Operator who resolved it, when resolved.
    pub operator_id: Option<String>,
    /// Operator's note, when supplied.
    pub note: Option<String>,
    /// When it was resolved.
    pub resolved_at: Option<String>,
}

impl From<Reconciliation> for ReconciliationResponse {
    fn from(flag: Reconciliation) -> Self {
        Self {
            order_id: flag.order_id.to_string(),
            reason: flag.reason,
            status: flag.status.as_str().to_owned(),
            created_at: java_instant(flag.created_at),
            operator_id: flag.operator_id,
            note: flag.note,
            resolved_at: flag.resolved_at.map(java_instant),
        }
    }
}

/// Resolve a reconciliation flag.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct ResolveReconciliationRequest {
    /// Only `resolved` is accepted.
    pub status: String,
    /// Optional operator note.
    pub note: Option<String>,
}

/// The query parameters the reads take (each optional to decode; the
/// history's `customerId` is required by the route).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    /// A customer.
    pub customer_id: Option<String>,
    /// A status.
    pub status: Option<String>,
    /// An order.
    pub order_id: Option<String>,
}

/// The reconciliation-status 400's reason, whatever status was asked for (as
/// tapir's single error variant maps it).
const ONLY_RESOLVED: &str = "Invalid reconciliation status: only 'resolved' is accepted";

/// Why a route didn't answer its success.
#[derive(Debug)]
pub enum RouteError {
    /// tapir's text 400: what didn't decode (`path parameter id`, `body (…)`).
    Decode(String),
    /// A rule's 400, a 401, 404, 409, 500 or 503, with its JSON reason.
    Refused(StatusCode, String),
    /// 402: the provider declined, with its reason.
    Declined(String),
    /// 409: lines inventory couldn't reserve.
    Stock(Vec<StockFailure>),
    /// Postgres failed.
    Store(StoreError),
}

impl From<StoreError> for RouteError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl IntoResponse for RouteError {
    fn into_response(self) -> Response {
        match self {
            Self::Decode(what) => text(
                StatusCode::BAD_REQUEST,
                format!("Invalid value for: {what}"),
            ),
            Self::Refused(status, error) => (status, Json(ErrorResponse { error })).into_response(),
            Self::Declined(reason) => (
                StatusCode::PAYMENT_REQUIRED,
                Json(PaymentDeclinedResponse {
                    error: "Payment declined".to_owned(),
                    payment_failure: PaymentFailure { reason },
                }),
            )
                .into_response(),
            Self::Stock(failures) => (
                StatusCode::CONFLICT,
                Json(InsufficientStockResponse {
                    error: "Insufficient stock".to_owned(),
                    reservation_failures: failures
                        .into_iter()
                        .map(|failure| ReservationFailureResponse {
                            sku: failure.sku,
                            reason: failure.reason,
                        })
                        .collect(),
                }),
            )
                .into_response(),
            Self::Store(_) => text(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_owned(),
            ),
        }
    }
}

fn text(status: StatusCode, message: String) -> Response {
    TextError { status, message }.into_response()
}

fn refused(status: StatusCode, reason: &str) -> RouteError {
    RouteError::Refused(status, reason.to_owned())
}

fn bad_request(reason: String) -> RouteError {
    RouteError::Refused(StatusCode::BAD_REQUEST, reason)
}

fn not_found() -> RouteError {
    refused(StatusCode::NOT_FOUND, "Order not found")
}

fn unauthorized() -> RouteError {
    refused(StatusCode::UNAUTHORIZED, "Invalid operator token")
}

fn path_id(raw: &str, name: &str) -> Result<Uuid, RouteError> {
    parse_uuid(raw).ok_or_else(|| RouteError::Decode(format!("path parameter {name}")))
}

fn body<T: serde::de::DeserializeOwned>(bytes: &Bytes) -> Result<T, RouteError> {
    serde_json::from_slice(bytes).map_err(|error| RouteError::Decode(format!("body ({error})")))
}

fn authorization(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
}

/// Trimmed, and absent when blank (as the Scala routes read their filters).
fn filter(raw: Option<&str>) -> Option<&str> {
    raw.map(str::trim).filter(|text| !text.is_empty())
}

/// Logs a failure the way the Scala routes do, and passes it on.
fn logged<T, S>(state: &AppState<S>, result: Result<T, RouteError>) -> Result<T, RouteError> {
    match &result {
        Err(RouteError::Store(error)) => state
            .logger
            .error(&[("error", &error.to_string())], "Request failed"),
        Err(RouteError::Refused(_, reason)) => {
            state.logger.warn(&[("reason", reason)], "Request refused");
        }
        _ => {}
    }
    result
}

/// Everything checkout checks before calling anything: the items, the
/// address, the method id and the email, in that order, then the total.
fn validate_checkout(
    request: &CreateOrderRequest,
) -> Result<(Vec<NewItem>, Option<String>, i32), RouteError> {
    let items: Vec<NewItem> = request
        .items
        .iter()
        .map(|item| NewItem {
            sku: item.sku.clone(),
            product_name: item.product_name.clone(),
            unit_price_cents: item.unit_price_cents,
            quantity: item.quantity,
        })
        .collect();
    validate_items(&items).map_err(|error| match error {
        ItemsError::Empty => bad_request("Order must have at least one item".to_owned()),
        ItemsError::Invalid(reason) => bad_request(reason),
    })?;
    request.shipping_address.validate().map_err(bad_request)?;
    parse_uuid(&request.payment_method_id)
        .ok_or_else(|| bad_request("paymentMethodId must be a UUID".to_owned()))?;
    let email = guest_email(request.email.as_deref()).map_err(bad_request)?;
    let lines: Vec<Line> = items
        .iter()
        .map(|item| Line {
            unit_price_cents: item.unit_price_cents,
            quantity: item.quantity,
        })
        .collect();
    let total = total_cents(&lines).map_err(bad_request)?;
    Ok((items, email, total))
}

/// `POST /orders` (201): validate, mint the ids, reserve every line, authorize
/// the total, save the order `authorized`. Nothing is saved unless every step
/// succeeds; every failure after the reserve call releases what it holds.
///
/// # Errors
/// An undecodable body or a broken rule (400, an unknown method too); a decline
/// (402); lines out of stock (409); not saved (500); a callee unavailable (503).
pub async fn checkout<S: Orders>(
    State(state): State<AppState<S>>,
    bytes: Bytes,
) -> Result<(StatusCode, Json<OrderResponse>), RouteError> {
    let request: CreateOrderRequest = body(&bytes)?;
    state.logger.info(
        &[("method", "POST"), ("path", "/orders")],
        "Received request",
    );
    let (items, email, total) = validate_checkout(&request).inspect_err(|error| {
        if let RouteError::Refused(_, reason) = error {
            state.logger.warn(
                &[("customer_id", &request.customer_id), ("reason", reason)],
                "Invalid checkout request",
            );
        }
    })?;
    let now = OffsetDateTime::now_utc();
    let mut order = Order {
        id: Uuid::new_v4(),
        customer_id: request.customer_id.clone(),
        total_cents: total,
        status: OrderStatus::Authorized,
        created_at: now,
        updated_at: now,
        shipping_address: Some(request.shipping_address.clone()),
        guest_email: email,
    };
    let lines: Vec<OrderItem> = items
        .into_iter()
        .map(|item| OrderItem {
            id: Uuid::new_v4(),
            sku: item.sku,
            product_name: item.product_name,
            unit_price_cents: item.unit_price_cents,
            quantity: item.quantity,
        })
        .collect();
    let id = order.id.to_string();
    let placed = place(&state, &mut order, &lines, &request.payment_method_id).await;
    if let Err(Failed { release: true, .. }) = &placed {
        release(&state, &id).await;
    }
    placed.map_err(|failure| failure.error)?;
    state.logger.info(&[("order_id", &id)], "Request completed");
    Ok((StatusCode::CREATED, Json(OrderResponse::of(&order, &lines))))
}

/// A failed checkout: its answer, and whether stock may be held.
struct Failed {
    error: RouteError,
    release: bool,
}

fn failed(error: RouteError, release: bool) -> Failed {
    Failed { error, release }
}

async fn place<S: Orders>(
    state: &AppState<S>,
    order: &mut Order,
    lines: &[OrderItem],
    payment_method_id: &str,
) -> Result<(), Failed> {
    let id = order.id.to_string();
    let reservation: Vec<ReservationLine> = lines
        .iter()
        .map(|line| ReservationLine {
            order_item_id: line.id.to_string(),
            sku: line.sku.clone(),
            quantity: line.quantity,
        })
        .collect();
    let unavailable = |service: &str| {
        refused(
            StatusCode::SERVICE_UNAVAILABLE,
            &format!("{service} service unavailable"),
        )
    };
    match state.inventory.reserve(&id, &reservation).await {
        Err(error) => {
            state.logger.warn(
                &[("order_id", &id), ("error", &error.to_string())],
                "Reservation call failed",
            );
            // It may have landed.
            return Err(failed(unavailable("Inventory"), true));
        }
        Ok(Reservation::Rejected(failures)) => {
            state.logger.warn(
                &[
                    ("order_id", &id),
                    ("failed_lines", &failures.len().to_string()),
                ],
                "Reservation rejected",
            );
            return Err(failed(RouteError::Stock(failures), false));
        }
        Ok(Reservation::Reserved) => {}
    }
    match state
        .payment
        .authorize(&id, order.total_cents, payment_method_id)
        .await
    {
        Err(error) => {
            state.logger.warn(
                &[("order_id", &id), ("error", &error.to_string())],
                "Authorize call failed",
            );
            return Err(failed(unavailable("Payment"), true));
        }
        Ok(Authorization::Declined(reason)) => {
            state.logger.warn(
                &[("order_id", &id), ("reason", &reason)],
                "Payment declined",
            );
            return Err(failed(RouteError::Declined(reason), true));
        }
        Ok(Authorization::UnknownMethod) => {
            state
                .logger
                .warn(&[("order_id", &id)], "Unknown payment method");
            return Err(failed(
                bad_request("Payment method not found".to_owned()),
                true,
            ));
        }
        Ok(Authorization::Authorized { .. }) => {}
    }
    match state.store.create_authorized(order, lines).await {
        Ok((created_at, updated_at)) => {
            order.created_at = created_at;
            order.updated_at = updated_at;
            Ok(())
        }
        Err(error) => {
            // The authorization lapses via payment-service's expiry sweep.
            state.logger.error(
                &[("order_id", &id), ("error", &error.to_string())],
                "Persisting the order failed",
            );
            Err(failed(
                refused(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Order could not be saved",
                ),
                true,
            ))
        }
    }
}

/// Releases what `order_id` holds; a failure is logged (the release is
/// idempotent, and a missed one is TD-8's orphaned reservation).
async fn release<S>(state: &AppState<S>, order_id: &str) {
    if let Err(error) = state.inventory.release(order_id).await {
        state.logger.warn(
            &[("order_id", order_id), ("error", &error.to_string())],
            "Releasing the reservation failed",
        );
    }
}

/// `GET /orders?customerId`: the customer's history, newest first, cache-aside.
///
/// # Errors
/// No `customerId` (400); a store failure (500).
pub async fn history<S: Orders>(
    State(state): State<AppState<S>>,
    Query(query): Query<Params>,
) -> Result<Json<Vec<OrderResponse>>, RouteError> {
    let customer = query
        .customer_id
        .as_deref()
        .ok_or_else(|| RouteError::Decode("query parameter customerId (missing)".to_owned()))?;
    if let Some(cached) = state.history.get(customer).await {
        return Ok(Json(cached));
    }
    let result = state.store.by_customer(customer).await.map_err(Into::into);
    let history: Vec<OrderResponse> = logged(&state, result)?
        .into_iter()
        .map(Into::into)
        .collect();
    state.history.set(customer, &history).await;
    Ok(Json(history))
}

/// `GET /admin/orders?status&customerId`: every order, newest first, from Postgres.
///
/// # Errors
/// An unknown status (400); a store failure (500).
pub async fn admin_orders<S: Orders>(
    State(state): State<AppState<S>>,
    Query(query): Query<Params>,
) -> Result<Json<Vec<OrderResponse>>, RouteError> {
    let result = async {
        let status = filter(query.status.as_deref())
            .map(|raw| {
                OrderStatus::parse(raw)
                    .ok_or_else(|| bad_request(format!("Invalid order status: '{raw}'")))
            })
            .transpose()?;
        let customer = filter(query.customer_id.as_deref());
        Ok(state.store.all(status, customer).await?)
    }
    .await;
    let orders = logged(&state, result)?;
    Ok(Json(orders.into_iter().map(Into::into).collect()))
}

/// `GET /orders/{id}`.
///
/// # Errors
/// A malformed id (400), no such order (404), a store failure (500).
pub async fn get<S: Orders>(
    State(state): State<AppState<S>>,
    Path(raw): Path<String>,
) -> Result<Json<OrderResponse>, RouteError> {
    let id = path_id(&raw, "id")?;
    let result = state
        .store
        .get(id)
        .await
        .map_err(RouteError::from)
        .and_then(|found| found.ok_or_else(not_found));
    Ok(Json(logged(&state, result)?.into()))
}

/// `PATCH` (and `PUT`) `/orders/{id}` {status}: only `cancelled`, by the
/// customer, or by the operator a `Bearer` token names; a repeat is 200.
///
/// # Errors
/// A malformed id or body (400); a bad token (401); an unknown status (400);
/// another known status or an order past cancelling (409); no such order
/// (404); a store failure (500).
pub async fn cancel<S: Orders>(
    State(state): State<AppState<S>>,
    Path(raw): Path<String>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Json<OrderResponse>, RouteError> {
    let id = path_id(&raw, "id")?;
    let request: UpdateOrderRequest = body(&bytes)?;
    let result = async {
        let operator = state
            .auth
            .authenticate(authorization(&headers))
            .map_err(|_| unauthorized())?;
        match OrderStatus::parse(&request.status) {
            None => Err(bad_request(format!(
                "Invalid order status: '{}'",
                request.status
            ))),
            Some(OrderStatus::Cancelled) => {
                let event = operator.map_or(OrderEvent::CustomerCancelled, |operator| {
                    OrderEvent::OperatorCancelled(operator.to_owned())
                });
                match state.store.transition(id, &event).await? {
                    // A repeat cancel is idempotent: the order as it is, nothing new.
                    Ok(_)
                    | Err(Rejected::Illegal {
                        from: OrderStatus::Cancelled,
                        ..
                    }) => state.store.get(id).await?.ok_or_else(not_found),
                    Err(Rejected::UnknownOrder) => Err(not_found()),
                    Err(Rejected::Illegal { to, .. }) => Err(refused(
                        StatusCode::CONFLICT,
                        &format!("Illegal status transition to '{}'", to.as_str()),
                    )),
                }
            }
            Some(other) => Err(refused(
                StatusCode::CONFLICT,
                &format!("Illegal status transition to '{}'", other.as_str()),
            )),
        }
    }
    .await;
    Ok(Json(logged(&state, result)?.into()))
}

/// `DELETE /orders/{id}` (204): the order and its lines, no event.
///
/// # Errors
/// A malformed id (400), no such order (404), a store failure (500).
pub async fn delete<S: Orders>(
    State(state): State<AppState<S>>,
    Path(raw): Path<String>,
) -> Result<StatusCode, RouteError> {
    let id = path_id(&raw, "id")?;
    let result = match state.store.delete(id).await {
        Ok(true) => Ok(StatusCode::NO_CONTENT),
        Ok(false) => Err(not_found()),
        Err(error) => Err(error.into()),
    };
    logged(&state, result)
}

/// `GET /reconciliations?status&orderId`: flags, newest first; an orderId
/// that isn't a UUID has none.
///
/// # Errors
/// An unknown status (400); a store failure (500).
pub async fn reconciliations<S: Orders>(
    State(state): State<AppState<S>>,
    Query(query): Query<Params>,
) -> Result<Json<Vec<ReconciliationResponse>>, RouteError> {
    let result = async {
        let status = filter(query.status.as_deref())
            .map(|raw| {
                ReconciliationStatus::parse(raw)
                    .ok_or_else(|| refused(StatusCode::BAD_REQUEST, ONLY_RESOLVED))
            })
            .transpose()?;
        match filter(query.order_id.as_deref()) {
            Some(raw) => match parse_uuid(raw) {
                Some(order) => Ok(state.store.reconciliations(status, Some(order)).await?),
                None => Ok(Vec::new()),
            },
            None => Ok(state.store.reconciliations(status, None).await?),
        }
    }
    .await;
    let flags = logged(&state, result)?;
    Ok(Json(flags.into_iter().map(Into::into).collect()))
}

/// `PATCH /reconciliations/{orderId}`: an operator resolves the flag; the
/// first resolver wins.
///
/// # Errors
/// A malformed id or body (400); no valid operator token (401); a status other
/// than `resolved` (400); no flag (404); a store failure (500).
pub async fn resolve<S: Orders>(
    State(state): State<AppState<S>>,
    Path(raw): Path<String>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Json<ReconciliationResponse>, RouteError> {
    let order = path_id(&raw, "orderId")?;
    let request: ResolveReconciliationRequest = body(&bytes)?;
    let result = async {
        let operator = state
            .auth
            .require_operator(authorization(&headers))
            .map_err(|_| unauthorized())?;
        if request.status.trim() != ReconciliationStatus::Resolved.as_str() {
            return Err(refused(StatusCode::BAD_REQUEST, ONLY_RESOLVED));
        }
        let note = request
            .note
            .as_deref()
            .map(str::trim)
            .filter(|note| !note.is_empty());
        match state
            .store
            .resolve(order, operator, note, OffsetDateTime::now_utc())
            .await?
        {
            ResolveOutcome::Resolved(flag) | ResolveOutcome::AlreadyResolved(flag) => Ok(flag),
            ResolveOutcome::NotFound => Err(refused(
                StatusCode::NOT_FOUND,
                "Reconciliation flag not found",
            )),
        }
    }
    .await;
    Ok(Json(logged(&state, result)?.into()))
}
