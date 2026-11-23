// SPDX-License-Identifier: MIT

//! The synchronous calls checkout and the fulfilment consumer make (Scala
//! `InventoryClient`, `PaymentClient`), each through its own purerest
//! resilience (gluon-rs `telemetry`'s `ResilientClient`: retry outside the
//! circuit breaker, `application.conf`'s 3 retries from 100 ms and 5 failures
//! for 30 s). Business outcomes are values; anything else (5xx after the
//! retries, an open breaker, a transport failure, an unexpected answer) is a
//! [`CallError`], which checkout answers 503.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use telemetry::client::{ClientError, ResilientClient, TracedClient};
use telemetry::metrics::Metrics;
use telemetry::resilience::{CircuitBreakerConfig, RetryConfig};
use telemetry::tracing::SdkTracer;

/// Each request's timeout.
pub const TIMEOUT: Duration = Duration::from_secs(10);

/// `application.conf`'s `*-client.resilience.retry`.
pub const RETRY: RetryConfig = RetryConfig {
    max_retries: 3,
    base_delay: Duration::from_millis(100),
};

/// `application.conf`'s `*-client.resilience.circuit-breaker`.
pub const BREAKER: CircuitBreakerConfig = CircuitBreakerConfig {
    failure_threshold: 5,
    reset_timeout: Duration::from_secs(30),
};

/// A call that gave no business outcome.
#[derive(Debug, thiserror::Error)]
pub enum CallError {
    /// No response: an open breaker or a transport failure.
    #[error(transparent)]
    Client(#[from] ClientError),
    /// A response it doesn't expect, as the Scala message words it.
    #[error("Unexpected response from {service}: {status}")]
    Unexpected {
        /// The callee.
        service: &'static str,
        /// Its status, with the reason phrase (`503 Service Unavailable`).
        status: String,
    },
    /// A response whose body isn't the one expected.
    #[error("{0}")]
    Body(String),
}

/// One client: the callee's base URL behind its own breaker.
///
/// # Errors
/// When the HTTP client can't be built.
pub fn resilient(tracer: SdkTracer, metrics: &Metrics) -> Result<ResilientClient, reqwest::Error> {
    Ok(ResilientClient::new(
        TracedClient::new(TIMEOUT, tracer, metrics)?,
        RETRY,
        BREAKER,
        metrics,
    ))
}

fn unexpected(service: &'static str, response: &reqwest::Response) -> CallError {
    let status = response.status();
    CallError::Unexpected {
        service,
        status: format!(
            "{} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or_default()
        )
        .trim()
        .to_owned(),
    }
}

async fn body<T: serde::de::DeserializeOwned>(response: reqwest::Response) -> Result<T, CallError> {
    response
        .json()
        .await
        .map_err(|error| CallError::Body(error.to_string()))
}

/// One line to reserve.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReservationLine {
    /// The order line's id.
    pub order_item_id: String,
    /// Its SKU.
    pub sku: String,
    /// Units.
    pub quantity: i32,
}

/// A line inventory-service couldn't reserve (`reason`: its wire value,
/// passed through to gshop).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StockFailure {
    /// The order line's id.
    pub order_item_id: String,
    /// Its SKU.
    pub sku: String,
    /// `insufficient_stock` | `unknown_sku`.
    pub reason: String,
}

/// A bulk reserve's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reservation {
    /// Every line reserved.
    Reserved,
    /// Nothing reserved; these lines couldn't be.
    Rejected(Vec<StockFailure>),
}

#[derive(Deserialize)]
struct RejectedBody {
    error: String,
    failures: Option<Vec<StockFailure>>,
}

/// inventory-service's bulk reservations (TD-7, ADR 0018).
#[derive(Debug, Clone)]
pub struct Inventory {
    client: ResilientClient,
    base_url: String,
}

impl Inventory {
    /// Calls `base_url` through `client`.
    #[must_use]
    pub fn new(base_url: &str, client: ResilientClient) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_owned(),
        }
    }

    /// All-or-nothing reserve of `items` for `order_id`.
    ///
    /// # Errors
    /// A [`CallError`]; a 409 naming no lines too.
    pub async fn reserve(
        &self,
        order_id: &str,
        items: &[ReservationLine],
    ) -> Result<Reservation, CallError> {
        let url = format!("{}/inventorys/order-reservations", self.base_url);
        let request = json!({"orderId": order_id, "items": items});
        let response = self.client.post_json(&url, &request).await?;
        match response.status().as_u16() {
            201 => Ok(Reservation::Reserved),
            409 => {
                let rejected: RejectedBody = body(response).await?;
                match rejected.failures.filter(|failures| !failures.is_empty()) {
                    Some(failures) => Ok(Reservation::Rejected(failures)),
                    None => Err(CallError::Body(format!(
                        "inventory-service rejected order {order_id}: {}",
                        rejected.error
                    ))),
                }
            }
            _ => Err(unexpected("inventory-service", &response)),
        }
    }

    /// Idempotent release of everything `order_id` holds.
    ///
    /// # Errors
    /// A [`CallError`].
    pub async fn release(&self, order_id: &str) -> Result<(), CallError> {
        let url = format!(
            "{}/inventorys/order-reservations/{order_id}/release",
            self.base_url
        );
        let response = self.client.send(reqwest::Method::POST, &url, None).await?;
        match response.status().as_u16() {
            204 => Ok(()),
            _ => Err(unexpected("inventory-service", &response)),
        }
    }
}

/// An authorize's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authorization {
    /// The hold was placed.
    Authorized {
        /// The gateway's reference.
        auth_id: String,
        /// When it lapses.
        expires_at: String,
    },
    /// The provider declined, with its reason.
    Declined(String),
    /// payment-service doesn't know the method.
    UnknownMethod,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuthorizedBody {
    auth_id: String,
    expires_at: String,
}

#[derive(Deserialize)]
struct DeclinedBody {
    reason: String,
}

/// payment-service's synchronous authorize (US-14.2, ADR 0018); safe to
/// retry, as payment-service replays an order's stored outcome (TD-10.1).
#[derive(Debug, Clone)]
pub struct Payment {
    client: ResilientClient,
    base_url: String,
}

impl Payment {
    /// Calls `base_url` through `client`.
    #[must_use]
    pub fn new(base_url: &str, client: ResilientClient) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_owned(),
        }
    }

    /// A hold of `amount_cents` for `order_id` on the method.
    ///
    /// # Errors
    /// A [`CallError`].
    pub async fn authorize(
        &self,
        order_id: &str,
        amount_cents: i32,
        payment_method_id: &str,
    ) -> Result<Authorization, CallError> {
        let url = format!("{}/payments/authorize", self.base_url);
        let request = json!({
            "orderId": order_id,
            "amountCents": amount_cents,
            "paymentMethodId": payment_method_id,
        });
        let response = self.client.post_json(&url, &request).await?;
        match response.status().as_u16() {
            200 => {
                let authorized: AuthorizedBody = body(response).await?;
                Ok(Authorization::Authorized {
                    auth_id: authorized.auth_id,
                    expires_at: authorized.expires_at,
                })
            }
            402 => {
                let declined: DeclinedBody = body(response).await?;
                Ok(Authorization::Declined(declined.reason))
            }
            404 => Ok(Authorization::UnknownMethod),
            _ => Err(unexpected("payment-service", &response)),
        }
    }
}
