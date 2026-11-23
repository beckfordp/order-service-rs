// SPDX-License-Identifier: MIT

//! The order domain, as the Scala `OrderStatus`, `OrderLifecycle`, `Pricing`,
//! `Address`, `GuestEmail`, `OrderCancellation`, `Reconciliation` and
//! `OperatorAuth`: values parsed at the edge, the lifecycle's one rule for every
//! status change, and the checkout's validation in its order.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

/// Java's `UUID.fromString` for the canonical form: 8-4-4-4-12 hex digits.
/// (Rust's parser also takes braced, URN and unhyphenated forms; Java doesn't.)
#[must_use]
pub fn parse_uuid(raw: &str) -> Option<Uuid> {
    let canonical = raw.len() == 36
        && raw
            .char_indices()
            .all(|(i, c)| matches!(i, 8 | 13 | 18 | 23) == (c == '-'));
    canonical.then(|| Uuid::parse_str(raw).ok()).flatten()
}

/// An order's lifecycle status (`authorized` is where every order starts, ADR 0018).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderStatus {
    /// Payment captured.
    Confirmed,
    /// The capture failed.
    PaymentFailed,
    /// Cancelled by a customer, an operator or an expired authorization.
    Cancelled,
    /// Stock reserved and payment authorized: placed.
    Authorized,
    /// Fulfilment dispatched it.
    Dispatched,
    /// Fulfilment failed to dispatch it.
    DispatchFailed,
}

impl OrderStatus {
    /// The wire and column value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::PaymentFailed => "payment_failed",
            Self::Cancelled => "cancelled",
            Self::Authorized => "authorized",
            Self::Dispatched => "dispatched",
            Self::DispatchFailed => "dispatch_failed",
        }
    }

    /// The status `raw` names, if it names one.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "confirmed" => Self::Confirmed,
            "payment_failed" => Self::PaymentFailed,
            "cancelled" => Self::Cancelled,
            "authorized" => Self::Authorized,
            "dispatched" => Self::Dispatched,
            "dispatch_failed" => Self::DispatchFailed,
            _ => return None,
        })
    }
}

/// What can happen to an order: HTTP and Kafka paths all become one of these
/// before touching its status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderEvent {
    /// payment-service captured the payment.
    PaymentCaptured,
    /// payment-service's capture failed, with its reason.
    PaymentFailed(String),
    /// The authorization lapsed uncaptured.
    AuthorizationExpired,
    /// Fulfilment dispatched the order.
    DispatchConfirmed,
    /// Fulfilment failed to dispatch the order.
    DispatchFailed,
    /// The customer cancelled.
    CustomerCancelled,
    /// An operator cancelled.
    OperatorCancelled(String),
}

impl OrderEvent {
    /// The event's name in flags and logs.
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::PaymentCaptured => "PaymentCaptured",
            Self::PaymentFailed(_) => "PaymentFailed",
            Self::AuthorizationExpired => "AuthorizationExpired",
            Self::DispatchConfirmed => "DispatchConfirmed",
            Self::DispatchFailed => "DispatchFailed",
            Self::CustomerCancelled => "CustomerCancelled",
            Self::OperatorCancelled(_) => "OperatorCancelled",
        }
    }

    /// The statuses the event may move an order from (the store's
    /// compare-and-set uses the same list).
    #[must_use]
    pub const fn allowed_from(&self) -> &'static [OrderStatus] {
        match self {
            // The consumers are independent streams: a capture can overtake its dispatch.
            Self::PaymentCaptured | Self::PaymentFailed(_) => {
                &[OrderStatus::Authorized, OrderStatus::Dispatched]
            }
            _ => &[OrderStatus::Authorized],
        }
    }

    /// The status the event moves an order to.
    #[must_use]
    pub const fn target(&self) -> OrderStatus {
        match self {
            Self::PaymentCaptured => OrderStatus::Confirmed,
            Self::PaymentFailed(_) => OrderStatus::PaymentFailed,
            Self::AuthorizationExpired | Self::CustomerCancelled | Self::OperatorCancelled(_) => {
                OrderStatus::Cancelled
            }
            Self::DispatchConfirmed => OrderStatus::Dispatched,
            Self::DispatchFailed => OrderStatus::DispatchFailed,
        }
    }

    /// The order's next status from `current`, or why not.
    ///
    /// # Errors
    /// [`Rejected::Illegal`] when the event isn't allowed from `current`.
    pub fn transition(&self, current: OrderStatus) -> Result<OrderStatus, Rejected> {
        if self.allowed_from().contains(&current) {
            Ok(self.target())
        } else {
            Err(Rejected::Illegal {
                from: current,
                to: self.target(),
            })
        }
    }

    /// A cancellation's reason (`OrderCancelled.reason`, the audit's), or
    /// `None` for an event that doesn't cancel.
    #[must_use]
    pub const fn cancellation_reason(&self) -> Option<&'static str> {
        match self {
            Self::CustomerCancelled => Some("customer"),
            Self::OperatorCancelled(_) => Some("operator"),
            Self::AuthorizationExpired => Some("auth_expired"),
            _ => None,
        }
    }

    /// Who cancelled an order placed by `customer_id`, or `None` for an event
    /// that doesn't cancel.
    #[must_use]
    pub fn actor(&self, customer_id: &str) -> Option<Actor> {
        match self {
            Self::CustomerCancelled => Some(Actor::new(Actor::CUSTOMER, customer_id)),
            Self::OperatorCancelled(operator) => Some(Actor::new(Actor::OPERATOR, operator)),
            Self::AuthorizationExpired => Some(Actor::new(Actor::SYSTEM, "payment-service")),
            _ => None,
        }
    }
}

/// Why a transition didn't happen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejected {
    /// No such order.
    UnknownOrder,
    /// The order is in a status the event isn't allowed from.
    Illegal {
        /// Its status.
        from: OrderStatus,
        /// Where the event would have moved it.
        to: OrderStatus,
    },
}

/// Who triggered a cancellation (`OrderCancelled.actor`, the audit's).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[schemars(inline)]
pub struct Actor {
    /// Actor type: customer | operator | system.
    #[serde(rename = "type")]
    pub kind: String,
    /// Actor id: the customerId, operator id, or system component.
    pub id: String,
}

impl Actor {
    /// A customer.
    pub const CUSTOMER: &'static str = "customer";
    /// An operator.
    pub const OPERATOR: &'static str = "operator";
    /// The system.
    pub const SYSTEM: &'static str = "system";

    fn new(kind: &str, id: &str) -> Self {
        Self {
            kind: kind.to_owned(),
            id: id.to_owned(),
        }
    }
}

/// The minimal projection a transition gives back: what an outbox payload needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transitioned {
    /// The order.
    pub order_id: Uuid,
    /// Who placed it.
    pub customer_id: String,
    /// Its total.
    pub total_cents: i32,
    /// The status it left.
    pub from: OrderStatus,
}

/// A postal address (system-design.md "REST contracts").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_address())]
pub struct Address {
    /// First address line; required.
    pub line1: String,
    /// Second address line; optional.
    #[serde(default)]
    pub line2: Option<String>,
    /// Town or city; required.
    pub city: String,
    /// Postal code; required.
    pub postal_code: String,
    /// ISO 3166-1 alpha-2 country code.
    pub country: String,
}

fn example_address() -> Address {
    Address {
        line1: "1 Example Street".to_owned(),
        line2: Some("Flat 2".to_owned()),
        city: "London".to_owned(),
        postal_code: "EC1A 1AA".to_owned(),
        country: "GB".to_owned(),
    }
}

impl Address {
    /// The first broken rule: line1, city and postal code required, the
    /// country two capital letters.
    ///
    /// # Errors
    /// The rule's reason.
    pub fn validate(&self) -> Result<(), String> {
        required("line1", &self.line1)?;
        required("city", &self.city)?;
        required("postalCode", &self.postal_code)?;
        let alpha2 =
            self.country.len() == 2 && self.country.bytes().all(|b| b.is_ascii_uppercase());
        if alpha2 {
            Ok(())
        } else {
            Err("country must be an ISO 3166-1 alpha-2 code".to_owned())
        }
    }
}

fn required(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("{field} is required"))
    } else {
        Ok(())
    }
}

/// A guest checkout's email (ADM-10): blank means absent; a present one is
/// trimmed, lower-cased and must look like `local@domain.tld` (as user-service
/// normalises at signup).
///
/// # Errors
/// "email must be a valid email address".
pub fn guest_email(raw: Option<&str>) -> Result<Option<String>, String> {
    let Some(value) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let normalized = value.to_lowercase();
    if looks_like_email(&normalized) && normalized.chars().count() <= 254 {
        Ok(Some(normalized))
    } else {
        Err("email must be a valid email address".to_owned())
    }
}

/// `^[^@\s]+@[^@\s]+\.[^@\s]+$`.
fn looks_like_email(text: &str) -> bool {
    let no_space_or_at = |part: &str| {
        !part.is_empty() && !part.contains(['@']) && !part.chars().any(char::is_whitespace)
    };
    let Some((local, domain)) = text.split_once('@') else {
        return false;
    };
    // The last dot with something either side (the regex backtracks to any dot).
    no_space_or_at(local)
        && !domain.contains('@')
        && domain
            .char_indices()
            .filter(|(_, c)| *c == '.')
            .any(|(i, _)| no_space_or_at(&domain[..i]) && no_space_or_at(&domain[i + 1..]))
}

/// One line to price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Line {
    /// Unit price, cents.
    pub unit_price_cents: i32,
    /// Units.
    pub quantity: i32,
}

/// The lines' total, summed wide and only then narrowed to the wire's `Int`.
///
/// # Errors
/// A negative price or quantity, or a total past `Int`'s range.
pub fn total_cents(lines: &[Line]) -> Result<i32, String> {
    let mut total: i64 = 0;
    for line in lines {
        if line.unit_price_cents < 0 {
            return Err(format!(
                "unitPriceCents must be non-negative, got {}",
                line.unit_price_cents
            ));
        }
        if line.quantity < 0 {
            return Err(format!(
                "quantity must be non-negative, got {}",
                line.quantity
            ));
        }
        // Two i32s multiply within an i64, and a basket's sum of them can't wrap it here.
        total = total.saturating_add(i64::from(line.unit_price_cents) * i64::from(line.quantity));
    }
    i32::try_from(total)
        .map_err(|_| format!("order total exceeds the wire contract's Int range: {total}"))
}

/// A line to buy, as checked before anything is called.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewItem {
    /// Catalog SKU.
    pub sku: String,
    /// Name shown on the order.
    pub product_name: String,
    /// Unit price, cents.
    pub unit_price_cents: i32,
    /// Units.
    pub quantity: i32,
}

/// Why a checkout's items were refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemsError {
    /// No lines.
    Empty,
    /// A line's quantity or price, with the reason.
    Invalid(String),
}

/// The first broken line rule: at least one line, each quantity at least 1,
/// each price at least 0.
///
/// # Errors
/// [`ItemsError`].
pub fn validate_items(items: &[NewItem]) -> Result<(), ItemsError> {
    if items.is_empty() {
        return Err(ItemsError::Empty);
    }
    for item in items {
        if item.quantity <= 0 {
            return Err(ItemsError::Invalid(format!(
                "quantity must be positive, got {} for sku '{}'",
                item.quantity, item.sku
            )));
        }
        if item.unit_price_cents < 0 {
            return Err(ItemsError::Invalid(format!(
                "unitPriceCents must be non-negative, got {} for sku '{}'",
                item.unit_price_cents, item.sku
            )));
        }
    }
    Ok(())
}

/// An order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Order {
    /// Its id.
    pub id: Uuid,
    /// Who placed it.
    pub customer_id: String,
    /// Sum of the line totals.
    pub total_cents: i32,
    /// Lifecycle status.
    pub status: OrderStatus,
    /// When placed.
    pub created_at: OffsetDateTime,
    /// When last changed.
    pub updated_at: OffsetDateTime,
    /// Where to ship (absent on orders placed before US-11.1).
    pub shipping_address: Option<Address>,
    /// A guest checkout's email.
    pub guest_email: Option<String>,
}

/// One line of an order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderItem {
    /// Line id.
    pub id: Uuid,
    /// Catalog SKU.
    pub sku: String,
    /// Name shown on the order.
    pub product_name: String,
    /// Unit price, cents.
    pub unit_price_cents: i32,
    /// Units.
    pub quantity: i32,
}

/// A reconciliation flag's state (ADM-4, ADR 0026).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationStatus {
    /// Waiting for an operator.
    Open,
    /// An operator resolved it.
    Resolved,
}

impl ReconciliationStatus {
    /// The wire and column value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Resolved => "resolved",
        }
    }

    /// The status `raw` names, if it names one.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "open" => Some(Self::Open),
            "resolved" => Some(Self::Resolved),
            _ => None,
        }
    }
}

/// A first-wins manual-reconciliation flag, and its resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reconciliation {
    /// The flagged order.
    pub order_id: Uuid,
    /// Why it needs reconciling.
    pub reason: String,
    /// Open or resolved.
    pub status: ReconciliationStatus,
    /// When flagged.
    pub created_at: OffsetDateTime,
    /// Who resolved it.
    pub operator_id: Option<String>,
    /// The operator's note.
    pub note: Option<String>,
    /// When resolved.
    pub resolved_at: Option<OffsetDateTime>,
}

/// What resolving a flag found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveOutcome {
    /// Resolved now.
    Resolved(Reconciliation),
    /// Already resolved: the first resolution, untouched.
    AlreadyResolved(Reconciliation),
    /// No flag for the order.
    NotFound,
}

/// The dev/demo operator gate (US-22, ADR 0024): not a real authorization boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorAuth {
    token: Option<String>,
    operator_id: String,
}

/// A presented operator token that is malformed or doesn't match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unauthorized;

impl OperatorAuth {
    /// From config: a blank token is none (every presented token refused).
    #[must_use]
    pub fn new(token: &str, operator_id: &str) -> Self {
        let token = token.trim();
        Self {
            token: (!token.is_empty()).then(|| token.to_owned()),
            operator_id: operator_id.trim().to_owned(),
        }
    }

    /// A cancel's actor: no header is the customer (`None`); a matching
    /// `Bearer` token the operator.
    ///
    /// # Errors
    /// [`Unauthorized`] for any other header.
    pub fn authenticate(&self, authorization: Option<&str>) -> Result<Option<&str>, Unauthorized> {
        match authorization {
            None => Ok(None),
            Some(_) => self.require_operator(authorization).map(Some),
        }
    }

    /// The operator, when `authorization` is `Bearer <token>` (the scheme in
    /// any case, the token trimmed; compared in constant time).
    ///
    /// # Errors
    /// [`Unauthorized`] otherwise, a missing header too.
    pub fn require_operator(&self, authorization: Option<&str>) -> Result<&str, Unauthorized> {
        const SCHEME: &str = "bearer ";
        let expected = self.token.as_deref().ok_or(Unauthorized)?;
        let raw = authorization.ok_or(Unauthorized)?;
        if raw.len() <= SCHEME.len() || !raw.is_char_boundary(SCHEME.len()) {
            return Err(Unauthorized);
        }
        let (scheme, presented) = raw.split_at(SCHEME.len());
        if scheme.eq_ignore_ascii_case(SCHEME) && constant_eq(presented.trim(), expected) {
            Ok(&self.operator_id)
        } else {
            Err(Unauthorized)
        }
    }
}

/// `MessageDigest.isEqual`: the whole of both compared, whatever differs first.
fn constant_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0_u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
