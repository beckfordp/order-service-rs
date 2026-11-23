// SPDX-License-Identifier: MIT

//! The service's API description: what tapir generated from the Scala
//! endpoints (capture 20261123 openapi.yaml), stated once here next to the DTO
//! types it derives the schemas from, served with Swagger UI (ORDER-D1); and
//! the AsyncAPI document of what it publishes through the outbox and the two
//! topics it consumes, with its page (ORDER-D2). The cancel and resolve
//! routes' `Authorization` header parameter is not listed (the builder has path
//! and query parameters only).

use axum::Router;
use serde_json::{Value, json};
use telemetry::docs::asyncapi::{self, Channel};
use telemetry::docs::openapi::{Api, Operation};
use telemetry::docs::swagger;

use crate::domain::Address;
use crate::events::{
    DISPATCH_CONFIRMED, DISPATCH_FAILED, DispatchConfirmed, DispatchFailed, FULFILMENT_EVENTS,
    PAYMENT_AUTHORIZATION_EXPIRED, PAYMENT_CAPTURED, PAYMENT_EVENTS, PAYMENT_FAILED,
    PaymentAuthorizationExpired, PaymentCaptured, PaymentFailed,
};
use crate::published::{
    ORDER_CANCELLED, ORDER_CONFIRMED, ORDER_PAYMENT_FAILED, OrderCancelled, OrderConfirmed,
    OrderPaymentFailed, TOPIC,
};
use crate::routes::{
    CreateOrderItemRequest, CreateOrderRequest, ErrorResponse, InsufficientStockResponse,
    OrderItemResponse, OrderResponse, PaymentDeclinedResponse, PaymentFailure,
    ReconciliationResponse, ReservationFailureResponse, ResolveReconciliationRequest,
    UpdateOrderRequest,
};

const ORDER: &str = "3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90";
const CUSTOMER: &str = "7d1e4b2a-9c3f-4e8a-b5d6-1f2a3b4c5d6e";
const METHOD: &str = "9b2e4c7a-1f3d-4a6b-8c5e-2d7f0a1b3c4e";
const OPERATOR: &str = "The presented operator token is malformed or does not match (US-22 dev \
                        token).";

fn address() -> Value {
    json!({
        "line1": "1 Example Street",
        "line2": "Flat 2",
        "city": "London",
        "postalCode": "EC1A 1AA",
        "country": "GB",
    })
}

fn order() -> Value {
    json!({
        "id": ORDER,
        "customerId": CUSTOMER,
        "totalCents": 2598,
        "status": "authorized",
        "items": [{
            "id": "5c8d2e1f-3a4b-4c6d-9e7f-0a1b2c3d4e5f",
            "sku": "SKU-MUG-001",
            "productName": "Gluon Mug",
            "unitPriceCents": 1299,
            "quantity": 2,
        }],
        "createdAt": "2026-11-01T09:30:00Z",
        "updatedAt": "2026-11-01T09:45:00Z",
        "shippingAddress": address(),
    })
}

fn flag() -> Value {
    json!({
        "orderId": ORDER,
        "reason": "capture_declined",
        "status": "open",
        "createdAt": "2026-11-02T10:15:30Z",
    })
}

/// A JSON body: its schema component and example. (Always `Some`: the
/// builder takes `None` for a response without one.)
#[allow(clippy::unnecessary_wraps)]
fn body(schema: &'static str, example: Value) -> Option<(&'static str, Value)> {
    Some((schema, example))
}

#[allow(clippy::unnecessary_wraps)]
fn error(message: &str) -> Option<(&'static str, Value)> {
    Some(("ErrorResponse", json!({ "error": message })))
}

fn by_id(operation: Operation, summary: &str, description: &str) -> Operation {
    operation
        .tag("orders")
        .summary(summary)
        .description(description)
        .path_parameter(
            "id",
            "Order id (UUID). A malformed id returns 400.",
            json!(ORDER),
        )
        .response(404, "No order with this id.", error("Order not found"))
}

fn checkout(api: Api) -> Api {
    api.operation(
        Operation::post("/orders")
            .tag("orders")
            .summary("Place an order")
            .description(
                "Checkout: validates the items, shipping address and payment method, reserves \
                 stock for every line, authorizes payment, then saves the order as \
                 `authorized`. Nothing is saved unless every step succeeds, and stock already \
                 reserved is released on failure. Not idempotent: each call places a new order.",
            )
            .request_body(
                "CreateOrderRequest",
                json!({
                    "customerId": CUSTOMER,
                    "items": [{
                        "sku": "SKU-MUG-001",
                        "productName": "Gluon Mug",
                        "unitPriceCents": 1299,
                        "quantity": 2,
                    }],
                    "shippingAddress": address(),
                    "paymentMethodId": METHOD,
                }),
            )
            .response(201, "", body("OrderResponse", order()))
            .response(
                400,
                "`items` is empty, a line's quantity is below 1 or its unit price below 0, the \
                 shipping address is invalid, or `paymentMethodId` isn't a UUID or isn't known.",
                error("Order must have at least one item"),
            )
            .response(
                402,
                "The payment provider declined; any reserved stock was released.",
                body(
                    "PaymentDeclinedResponse",
                    json!({
                        "error": "Payment declined",
                        "paymentFailure": {"reason": "insufficient_funds"},
                    }),
                ),
            )
            .response(
                409,
                "One or more lines couldn't be reserved; nothing was reserved.",
                body(
                    "InsufficientStockResponse",
                    json!({
                        "error": "Insufficient stock",
                        "reservationFailures": [{"sku": "SKU-MUG-001", "reason": "out_of_stock"}],
                    }),
                ),
            )
            .response(
                500,
                "Payment was authorized but the order couldn't be saved; stock was released and \
                 the authorization lapses.",
                error("Order could not be saved"),
            )
            .response(
                503,
                "payment-service or inventory-service couldn't be reached; any reserved stock was \
                 released.",
                error("Payment service unavailable"),
            ),
    )
}

fn reads(api: Api) -> Api {
    api.operation(
        Operation::get("/orders")
            .tag("orders")
            .summary("List a customer's orders")
            .description(
                "The customer's order history, newest first. Served from a short-TTL cache, so \
                 a just-placed order can take up to the TTL to appear.",
            )
            .query_parameter(
                "customerId",
                "Id (UUID) of the customer whose orders to list.",
                json!(CUSTOMER),
            )
            .response(200, "", body("OrderResponse[]", json!([order()]))),
    )
    .operation(
        Operation::get("/admin/orders")
            .tag("admin")
            .summary("List all orders (admin)")
            .description(
                "Every customer's orders, newest first, optionally filtered by status and \
                 customer. Served directly from Postgres (no history cache) and not paginated — \
                 a demo-scale admin read (ADM-4). Unknown status → 400.",
            )
            .query_parameter(
                "status",
                "Only orders in this lifecycle status.",
                json!("authorized"),
            )
            .query_parameter(
                "customerId",
                "Only this customer's orders.",
                json!(CUSTOMER),
            )
            .response(200, "", body("OrderResponse[]", json!([order()])))
            .response(
                400,
                "`status` isn't a known order status.",
                error("Invalid order status: 'shipped!'"),
            ),
    )
    .operation(
        by_id(
            Operation::get("/orders/{id}"),
            "Get an order",
            "One order with its line items.",
        )
        .response(200, "", body("OrderResponse", order())),
    )
}

fn changes(api: Api) -> Api {
    let cancel = |operation: Operation, summary: &str, description: &str| {
        by_id(operation, summary, description)
            .request_body("UpdateOrderRequest", json!({"status": "cancelled"}))
            .response(200, "", body("OrderResponse", order()))
            .response(
                400,
                "`status` isn't a known order status.",
                error("Invalid order status: 'shipped!'"),
            )
            .response(401, OPERATOR, error("Invalid operator token"))
            .response(
                409,
                "The order can't move to the requested status: it's past cancelling, or the \
                 target isn't `cancelled`.",
                error("Illegal status transition to 'dispatched'"),
            )
    };
    api.operation(cancel(
        Operation::patch("/orders/{id}"),
        "Cancel an order",
        "Only `cancelled` is accepted; cancelling publishes `OrderCancelled`. Any other known \
         status returns 409. A configured operator Bearer token records an operator actor; an \
         invalid one returns 401.",
    ))
    .operation(cancel(
        Operation::put("/orders/{id}"),
        "Cancel an order (replace)",
        "Same as PATCH; kept for clients that use PUT.",
    ))
    .operation(
        by_id(
            Operation::delete("/orders/{id}"),
            "Delete an order",
            "Hard-deletes the order and its lines. No event is published.",
        )
        .response(204, "", None),
    )
}

fn reconciliations(api: Api) -> Api {
    let only = "Only `resolved` is accepted.";
    api.operation(
        Operation::get("/reconciliations")
            .tag("reconciliations")
            .summary("List reconciliation flags")
            .description(
                "The manual-reconciliation queue, newest first. Both filters are optional and \
                 ANDed; an unknown status → 400 and an unknown orderId returns an empty list. \
                 Unauthenticated local/demo read (ADM-4).",
            )
            .query_parameter("status", "open or resolved (default: both).", json!("open"))
            .query_parameter("orderId", "Only this order's flag.", json!(ORDER))
            .response(200, "", body("ReconciliationResponse[]", json!([flag()])))
            .response(400, only, error("Invalid reconciliation status: 'open'")),
    )
    .operation(
        Operation::patch("/reconciliations/{orderId}")
            .tag("reconciliations")
            .summary("Resolve a reconciliation flag")
            .description(
                "Marks an open flag resolved, recording the operator id from the token, an \
                 optional note and the timestamp. The first resolver wins: a repeat is an \
                 idempotent 200 and never overwrites the original. 401 without a valid token, \
                 404 when no flag exists.",
            )
            .path_parameter("orderId", "Order id whose flag to resolve.", json!(ORDER))
            .request_body(
                "ResolveReconciliationRequest",
                json!({"status": "resolved", "note": "Refunded manually"}),
            )
            .response(200, "", body("ReconciliationResponse", flag()))
            .response(400, only, error("Invalid reconciliation status: 'open'"))
            .response(401, OPERATOR, error("Invalid operator token"))
            .response(
                404,
                "No flag for this order.",
                error("Reconciliation flag not found"),
            ),
    )
}

/// The OpenAPI document: checkout, the reads, cancel and delete,
/// reconciliations, then the two health checks.
#[must_use]
pub fn api() -> Api {
    let api = Api::new("Order Service", "1.0")
        .schema::<Address>()
        .schema::<CreateOrderItemRequest>()
        .schema::<CreateOrderRequest>()
        .schema::<ErrorResponse>()
        .schema::<InsufficientStockResponse>()
        .schema::<OrderItemResponse>()
        .schema::<OrderResponse>()
        .schema::<PaymentDeclinedResponse>()
        .schema::<PaymentFailure>()
        .schema::<ReconciliationResponse>()
        .schema::<ReservationFailureResponse>()
        .schema::<ResolveReconciliationRequest>()
        .schema::<UpdateOrderRequest>();
    reconciliations(changes(reads(checkout(api))))
        .operation(
            Operation::get("/health")
                .tag("health")
                .summary("Liveness probe")
                .description("`200` while the process is up. Polled by Kubernetes.")
                .response(200, "", None),
        )
        .operation(
            Operation::get("/health/ready")
                .tag("health")
                .summary("Readiness probe")
                .description(
                    "`200` when Postgres is reachable, `503` otherwise. Polled by Kubernetes.",
                )
                .response(200, "", None)
                .response(503, "Postgres is unreachable.", None),
        )
}

/// The AsyncAPI document: the three `order-events` sent, the three
/// `payment-events` and two `fulfilment-events` received.
#[must_use]
pub fn asyncapi() -> Value {
    asyncapi::document(
        "Order Service",
        "1.0",
        &[
            Channel::publish_cdc::<OrderConfirmed>(TOPIC, ORDER_CONFIRMED),
            Channel::publish_cdc::<OrderPaymentFailed>(TOPIC, ORDER_PAYMENT_FAILED),
            Channel::publish_cdc::<OrderCancelled>(TOPIC, ORDER_CANCELLED),
            Channel::subscribe::<PaymentCaptured>(PAYMENT_EVENTS, PAYMENT_CAPTURED),
            Channel::subscribe::<PaymentAuthorizationExpired>(
                PAYMENT_EVENTS,
                PAYMENT_AUTHORIZATION_EXPIRED,
            ),
            Channel::subscribe::<PaymentFailed>(PAYMENT_EVENTS, PAYMENT_FAILED),
            Channel::subscribe::<DispatchConfirmed>(FULFILMENT_EVENTS, DISPATCH_CONFIRMED),
            Channel::subscribe::<DispatchFailed>(FULFILMENT_EVENTS, DISPATCH_FAILED),
        ],
    )
}

/// Swagger UI at `/docs/` (`/docs/docs.yaml`), and the AsyncAPI document and page.
pub fn router() -> Router {
    swagger::router(api().to_yaml()).merge(asyncapi::router_with_page(&asyncapi()))
}
