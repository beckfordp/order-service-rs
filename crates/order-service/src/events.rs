// SPDX-License-Identifier: MIT

//! The consumers' routes, as the Scala `PaymentEventConsumer` and
//! `FulfilmentEventConsumer`: each event becomes the lifecycle's transition
//! (`OrderStore.transition`, its outbox event and flag in the transaction); a
//! `DispatchFailed` releases the order's stock, a failed release flagging it.
//! An event the order's status doesn't allow is skipped, and flagged for
//! reconciliation unless it is a duplicate (the status it would move to); an
//! unknown order is skipped. Undecodable envelopes and payloads, and an
//! orderId that isn't a UUID, are logged and skipped (the Scala consumers
//! decode outside the DLQ guard); only a failing store is dead-lettered.

use kafka_event_consumer::envelope::{Envelope, Routes};
use schemars::JsonSchema;
use serde::Deserialize;
use telemetry::logging::Logger;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::clients::Inventory;
use crate::domain::{OrderEvent, Rejected, parse_uuid};
use crate::store::{Orders, StoreError};

/// The topic payment outcomes come on.
pub const PAYMENT_EVENTS: &str = "payment-events";
/// Its consumer group.
pub const PAYMENT_GROUP: &str = "order-service-payment-events";
/// The topic dispatch outcomes come on.
pub const FULFILMENT_EVENTS: &str = "fulfilment-events";
/// Its consumer group.
pub const FULFILMENT_GROUP: &str = "order-service-fulfilment-events";

/// Money moved: capture succeeded after `DispatchConfirmed`.
pub const PAYMENT_CAPTURED: &str = "PaymentCaptured";
/// The authorization lapsed uncaptured.
pub const PAYMENT_AUTHORIZATION_EXPIRED: &str = "PaymentAuthorizationExpired";
/// The capture was declined.
pub const PAYMENT_FAILED: &str = "PaymentFailed";
/// Dispatch succeeded.
pub const DISPATCH_CONFIRMED: &str = "DispatchConfirmed";
/// Dispatch failed.
pub const DISPATCH_FAILED: &str = "DispatchFailed";

/// Money moved: capture succeeded after `DispatchConfirmed`. Published once per order.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "PaymentCapturedEvent")]
pub struct PaymentCaptured {
    /// Order UUID (order-service's order id).
    pub order_id: String,
    /// Amount captured, in cents.
    pub amount_cents: i32,
    /// When the capture happened (ISO-8601 instant).
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub timestamp: OffsetDateTime,
}

/// Authorization lapsed uncaptured: found by the background expiry sweep.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "PaymentAuthorizationExpiredEvent")]
pub struct PaymentAuthorizationExpired {
    /// Order UUID (order-service's order id).
    pub order_id: String,
    /// When the sweep expired the hold (ISO-8601 instant).
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub timestamp: OffsetDateTime,
}

/// Capture declined after `DispatchConfirmed`. Authorize declines are synchronous and
/// publish nothing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "PaymentFailedEvent")]
pub struct PaymentFailed {
    /// Order UUID (order-service's order id).
    pub order_id: String,
    /// payment-service's own Payment id, for log/trace correlation only.
    pub payment_id: String,
    /// Amount that failed to charge, in cents.
    pub amount_cents: i32,
    /// Failure reason.
    pub reason: String,
    /// When the capture was declined (ISO-8601 instant).
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub timestamp: OffsetDateTime,
}

/// One dispatched line.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(inline)]
pub struct FulfilmentItem {
    /// Product SKU.
    pub sku: String,
    /// Product name at dispatch time.
    pub product_name: String,
    /// Units dispatched.
    pub quantity: i32,
    /// Unit price, in cents.
    pub unit_price_cents: i32,
}

/// Order dispatched: pick/pack/dispatch succeeded. Carries a full item snapshot so
/// consumers need no order lookup.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "DispatchConfirmedEvent")]
pub struct DispatchConfirmed {
    /// Order UUID (order-service's order id).
    pub order_id: String,
    /// Carrier tracking number.
    pub tracking_no: String,
    /// Items dispatched. Always sent.
    // Optional in the schema, as tapir states it; required to decode, as circe does.
    pub items: Option<Vec<FulfilmentItem>>,
}

/// Dispatch failed: pick/pack/dispatch hit an error. payment-service voids the hold;
/// order-service marks the order failed.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(rename = "DispatchFailedEvent")]
pub struct DispatchFailed {
    /// Order UUID (order-service's order id).
    pub order_id: String,
    /// Why dispatch failed.
    pub reason: String,
}

/// Why an event wasn't handled.
#[derive(Debug, thiserror::Error)]
pub enum EventsError {
    /// The payload isn't the event's (logged and skipped).
    #[error(transparent)]
    Payload(#[from] serde_json::Error),
    /// A `DispatchConfirmed` without its items (logged and skipped).
    #[error("Missing required field at 'items'")]
    NoItems,
    /// The payload's orderId isn't a UUID (logged and skipped).
    #[error("Invalid order id: '{0}'")]
    OrderId(String),
    /// The store failed: retried, then dead-lettered.
    #[error(transparent)]
    Store(#[from] StoreError),
}

fn decoded<T: serde::de::DeserializeOwned>(
    envelope: &Envelope,
    order_id: impl Fn(&T) -> &str,
) -> Result<(Uuid, T), EventsError> {
    let event: T = serde_json::from_value(envelope.payload.clone())?;
    let raw = order_id(&event);
    let id = parse_uuid(raw).ok_or_else(|| EventsError::OrderId(raw.to_owned()))?;
    Ok((id, event))
}

/// Applies `event` to the order: a transition rejected from another status
/// than its target is flagged "<label> rejected from <status>" (first wins).
/// True when it moved.
async fn apply<S: Orders>(
    store: &S,
    logger: &Logger,
    id: Uuid,
    event: &OrderEvent,
    label: &str,
) -> Result<bool, StoreError> {
    let order = id.to_string();
    match store.transition(id, event).await? {
        Ok(_) => {
            logger.info(
                &[("order_id", &order), ("event", label)],
                "Order status updated",
            );
            Ok(true)
        }
        Err(Rejected::UnknownOrder) => {
            logger.info(
                &[("order_id", &order), ("event", label)],
                "Unknown orderId - ignoring",
            );
            Ok(false)
        }
        Err(Rejected::Illegal { from, to }) => {
            logger.info(
                &[
                    ("order_id", &order),
                    ("event", label),
                    ("from", from.as_str()),
                    ("to", to.as_str()),
                ],
                "Order not awaiting this transition - ignoring",
            );
            if from != to {
                store
                    .flag(id, &format!("{label} rejected from {}", from.as_str()))
                    .await?;
            }
            Ok(false)
        }
    }
}

/// A payment outcome, decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaymentEvent {
    /// `PaymentCaptured`.
    Captured(Uuid),
    /// `PaymentAuthorizationExpired`.
    Expired(Uuid),
    /// `PaymentFailed`, with its reason.
    Failed(Uuid, String),
}

/// `payment-events`: captured → confirmed, failed → `payment_failed` with a
/// flag (and deliberately no release: the goods may be shipped), expired →
/// cancelled by the system.
#[derive(Debug, Clone)]
pub struct PaymentEvents<S> {
    store: S,
    logger: Logger,
}

impl<S> PaymentEvents<S> {
    /// Over `store`, logging to `logger`.
    #[must_use]
    pub fn new(store: S, logger: Logger) -> Self {
        Self { store, logger }
    }
}

impl<S: Orders> Routes for PaymentEvents<S> {
    type Event = PaymentEvent;
    type Error = EventsError;
    const DEAD_LETTERS_POISON: bool = false;

    fn decode(&self, envelope: &Envelope) -> Option<Result<PaymentEvent, EventsError>> {
        Some(match envelope.event_type.as_str() {
            PAYMENT_CAPTURED => decoded(envelope, |e: &PaymentCaptured| &e.order_id)
                .map(|(id, _)| PaymentEvent::Captured(id)),
            PAYMENT_AUTHORIZATION_EXPIRED => {
                decoded(envelope, |e: &PaymentAuthorizationExpired| &e.order_id)
                    .map(|(id, _)| PaymentEvent::Expired(id))
            }
            PAYMENT_FAILED => decoded(envelope, |e: &PaymentFailed| &e.order_id)
                .map(|(id, event)| PaymentEvent::Failed(id, event.reason)),
            _ => return None,
        })
    }

    async fn handle(&self, _: &str, event: &PaymentEvent) -> Result<(), EventsError> {
        let (id, event, label) = match event {
            PaymentEvent::Captured(id) => (*id, OrderEvent::PaymentCaptured, PAYMENT_CAPTURED),
            PaymentEvent::Expired(id) => (
                *id,
                OrderEvent::AuthorizationExpired,
                PAYMENT_AUTHORIZATION_EXPIRED,
            ),
            PaymentEvent::Failed(id, reason) => (
                *id,
                OrderEvent::PaymentFailed(reason.clone()),
                PAYMENT_FAILED,
            ),
        };
        apply(&self.store, &self.logger, id, &event, label).await?;
        Ok(())
    }
}

/// A dispatch outcome, decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FulfilmentEvent {
    /// `DispatchConfirmed`.
    Confirmed(Uuid),
    /// `DispatchFailed`.
    Failed(Uuid),
}

/// `fulfilment-events`: confirmed → dispatched; failed → `dispatch_failed`
/// and the stock released (a failed release flagged).
#[derive(Debug, Clone)]
pub struct FulfilmentEvents<S> {
    store: S,
    inventory: Inventory,
    logger: Logger,
}

impl<S> FulfilmentEvents<S> {
    /// Over `store`, releasing through `inventory`, logging to `logger`.
    #[must_use]
    pub fn new(store: S, inventory: Inventory, logger: Logger) -> Self {
        Self {
            store,
            inventory,
            logger,
        }
    }
}

impl<S: Orders> Routes for FulfilmentEvents<S> {
    type Event = FulfilmentEvent;
    type Error = EventsError;
    const DEAD_LETTERS_POISON: bool = false;

    fn decode(&self, envelope: &Envelope) -> Option<Result<FulfilmentEvent, EventsError>> {
        Some(match envelope.event_type.as_str() {
            DISPATCH_CONFIRMED => {
                decoded(envelope, |e: &DispatchConfirmed| &e.order_id).and_then(|(id, event)| {
                    match event.items {
                        Some(_) => Ok(FulfilmentEvent::Confirmed(id)),
                        None => Err(EventsError::NoItems),
                    }
                })
            }
            DISPATCH_FAILED => decoded(envelope, |e: &DispatchFailed| &e.order_id)
                .map(|(id, _)| FulfilmentEvent::Failed(id)),
            _ => return None,
        })
    }

    async fn handle(&self, _: &str, event: &FulfilmentEvent) -> Result<(), EventsError> {
        match event {
            FulfilmentEvent::Confirmed(id) => {
                apply(
                    &self.store,
                    &self.logger,
                    *id,
                    &OrderEvent::DispatchConfirmed,
                    DISPATCH_CONFIRMED,
                )
                .await?;
            }
            FulfilmentEvent::Failed(id) => {
                let moved = apply(
                    &self.store,
                    &self.logger,
                    *id,
                    &OrderEvent::DispatchFailed,
                    DISPATCH_FAILED,
                )
                .await?;
                if moved {
                    self.release(*id).await;
                }
            }
        }
        Ok(())
    }
}

impl<S: Orders> FulfilmentEvents<S> {
    /// Releases a failed dispatch's stock; a failure flags the order (and a
    /// failed flag is logged, not retried).
    async fn release(&self, id: Uuid) {
        let order = id.to_string();
        let Err(error) = self.inventory.release(&order).await else {
            return;
        };
        let reason = format!("inventory release failed after DispatchFailed: {error}");
        match self.store.flag(id, &reason).await {
            Ok(_) => self.logger.warn(
                &[("order_id", &order), ("error", &error.to_string())],
                "Releasing reservations for a dispatch failure failed - reconciliation flagged",
            ),
            Err(flag_error) => self.logger.error(
                &[("order_id", &order), ("error", &flag_error.to_string())],
                "Inventory release failed AND the reconciliation flag could not be written",
            ),
        }
    }
}
