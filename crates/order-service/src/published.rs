// SPDX-License-Identifier: MIT

//! What the service publishes through the outbox on [`TOPIC`] (Scala
//! `OrderEvents`), each written in the transaction that changes the order's
//! status (`store.rs`), keyed and correlated by the order id. Checkout
//! publishes nothing: payment-service publishes `PaymentAuthorized`.

use schemars::JsonSchema;
use serde::Serialize;

use crate::domain::Actor;

/// The topic Debezium routes the outbox to.
pub const TOPIC: &str = "order-events";

/// The service's name as the envelope's `source`.
pub const SOURCE: &str = "order-service";

/// The order was confirmed.
pub const ORDER_CONFIRMED: &str = "OrderConfirmed";
/// The order's capture failed.
pub const ORDER_PAYMENT_FAILED: &str = "OrderPaymentFailed";
/// The order was cancelled.
pub const ORDER_CANCELLED: &str = "OrderCancelled";

/// Order confirmed: order-service consumed `PaymentCaptured` and moved the order to confirmed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "OrderConfirmedEvent")]
pub struct OrderConfirmed {
    /// Order UUID (order-service's order id).
    pub order_id: String,
    /// Customer who placed the order.
    pub customer_id: String,
    /// When the order was confirmed (ISO-8601 instant).
    pub timestamp: String,
}

/// Payment capture failed: order-service consumed `PaymentFailed` and moved the order to
/// `payment_failed`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "OrderPaymentFailedEvent")]
pub struct OrderPaymentFailed {
    /// Order UUID (order-service's order id).
    pub order_id: String,
    /// Customer who placed the order.
    pub customer_id: String,
    /// payment-service's capture-failure reason.
    pub reason: String,
    /// When the order moved to `payment_failed` (ISO-8601 instant).
    pub timestamp: String,
}

/// Order cancelled: a customer/operator cancel or a consumed `PaymentAuthorizationExpired`
/// moved the order to cancelled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "OrderCancelledEvent")]
pub struct OrderCancelled {
    /// Order UUID (order-service's order id).
    pub order_id: String,
    /// Customer who placed the order.
    pub customer_id: String,
    /// When the order was cancelled (ISO-8601 instant).
    pub timestamp: String,
    /// Cancellation trigger: customer | `auth_expired` | stale | operator. Always sent by the
    /// producer.
    pub reason: Option<String>,
    /// Who cancelled. Always sent by the producer; absent on old messages.
    pub actor: Option<Actor>,
}
