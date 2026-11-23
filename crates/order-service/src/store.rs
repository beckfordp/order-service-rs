// SPDX-License-Identifier: MIT

//! The tables, as the Scala `OrderStore` uses them (the same SQL): orders and
//! their lines; the one status-changing call, a compare-and-set whose outbox
//! row, reconciliation flag and cancellation audit are written in its
//! transaction; the reconciliation flags. Handlers reach them through the
//! [`Orders`] trait, so they are testable with a store that fails
//! (`tests/support`); [`PgOrders`] is the Postgres one.

use service_basics::jvm::java_instant;
use service_basics::outbox;
use sqlx::postgres::PgRow;
use sqlx::{FromRow, PgPool, Postgres, Row, Transaction};
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::domain::{
    Address, Order, OrderEvent, OrderItem, OrderStatus, Reconciliation, ReconciliationStatus,
    Rejected, ResolveOutcome, Transitioned,
};
use crate::published::{
    ORDER_CANCELLED, ORDER_CONFIRMED, ORDER_PAYMENT_FAILED, OrderCancelled, OrderConfirmed,
    OrderPaymentFailed, SOURCE,
};

/// Why a store call failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The database refused or failed.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    /// An event payload didn't serialise.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// An order with its lines, by SKU.
pub type OrderWithItems = (Order, Vec<OrderItem>);

/// What the service needs from its tables.
pub trait Orders: Clone + Send + Sync + 'static {
    /// Stores an authorized checkout, ids as minted, order and lines in one
    /// transaction; when it was created and updated (no outbox row).
    fn create_authorized(
        &self,
        order: &Order,
        items: &[OrderItem],
    ) -> impl Future<Output = Result<(OffsetDateTime, OffsetDateTime), StoreError>> + Send;

    /// The order with `id`, if there is one.
    fn get(
        &self,
        id: Uuid,
    ) -> impl Future<Output = Result<Option<OrderWithItems>, StoreError>> + Send;

    /// A customer's orders, newest first.
    fn by_customer(
        &self,
        customer_id: &str,
    ) -> impl Future<Output = Result<Vec<OrderWithItems>, StoreError>> + Send;

    /// Every order, newest first, by status and customer.
    fn all(
        &self,
        status: Option<OrderStatus>,
        customer_id: Option<&str>,
    ) -> impl Future<Output = Result<Vec<OrderWithItems>, StoreError>> + Send;

    /// The one status change: `event`'s transition as a compare-and-set, with
    /// its outbox event, its flag (a failed capture's reason) and its
    /// cancellation audit in the same transaction.
    fn transition(
        &self,
        id: Uuid,
        event: &OrderEvent,
    ) -> impl Future<Output = Result<Result<Transitioned, Rejected>, StoreError>> + Send;

    /// Flags the order for manual reconciliation, its status untouched; the
    /// first flag wins. False for an unknown order or one already flagged.
    fn flag(&self, id: Uuid, reason: &str)
    -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Flags, newest first, by status and order.
    fn reconciliations(
        &self,
        status: Option<ReconciliationStatus>,
        order_id: Option<Uuid>,
    ) -> impl Future<Output = Result<Vec<Reconciliation>, StoreError>> + Send;

    /// Resolves the order's open flag as `operator`; the first resolver wins.
    fn resolve(
        &self,
        id: Uuid,
        operator: &str,
        note: Option<&str>,
        at: OffsetDateTime,
    ) -> impl Future<Output = Result<ResolveOutcome, StoreError>> + Send;

    /// Deletes the order and its lines; false when there was none.
    fn delete(&self, id: Uuid) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// True when the database answers within `within` (readiness).
    fn ping(&self, within: Duration) -> impl Future<Output = bool> + Send;
}

/// The tables in Postgres.
#[derive(Debug, Clone)]
pub struct PgOrders {
    pool: PgPool,
}

impl PgOrders {
    /// Over `pool`, migrated.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

const ORDER_COLUMNS: &str = "id, customer_id, total_cents, status, created_at, updated_at, \
     shipping_address::text as shipping_address, guest_email";

const RECONCILIATION_COLUMNS: &str =
    "order_id, reason, status, created_at, operator_id, note, resolved_at";

struct OrderRow(Order);

impl FromRow<'_, PgRow> for OrderRow {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let status: String = row.try_get("status")?;
        let status = OrderStatus::parse(&status).ok_or_else(|| decode("status", &status))?;
        let address: Option<String> = row.try_get("shipping_address")?;
        let shipping_address = address
            .map(|raw| {
                serde_json::from_str::<Address>(&raw)
                    .map_err(|error| decode("shipping_address", &error.to_string()))
            })
            .transpose()?;
        Ok(Self(Order {
            id: row.try_get("id")?,
            customer_id: row.try_get("customer_id")?,
            total_cents: row.try_get("total_cents")?,
            status,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
            shipping_address,
            guest_email: row.try_get("guest_email")?,
        }))
    }
}

struct ReconciliationRow(Reconciliation);

impl FromRow<'_, PgRow> for ReconciliationRow {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let status: String = row.try_get("status")?;
        let status =
            ReconciliationStatus::parse(&status).ok_or_else(|| decode("status", &status))?;
        Ok(Self(Reconciliation {
            order_id: row.try_get("order_id")?,
            reason: row.try_get("reason")?,
            status,
            created_at: row.try_get("created_at")?,
            operator_id: row.try_get("operator_id")?,
            note: row.try_get("note")?,
            resolved_at: row.try_get("resolved_at")?,
        }))
    }
}

fn decode(column: &str, value: &str) -> sqlx::Error {
    sqlx::Error::ColumnDecode {
        index: column.to_owned(),
        source: format!("unknown value {value:?}").into(),
    }
}

/// The outbox event a transition publishes, if any (Scala's callers pass the
/// same `PendingOutboxEvent` for each event).
fn outgoing(
    event: &OrderEvent,
    moved: &Transitioned,
    now: OffsetDateTime,
) -> Result<Option<(&'static str, serde_json::Value)>, serde_json::Error> {
    let order_id = moved.order_id.to_string();
    let customer_id = moved.customer_id.clone();
    let timestamp = java_instant(now);
    Ok(Some(match event {
        OrderEvent::PaymentCaptured => (
            ORDER_CONFIRMED,
            serde_json::to_value(OrderConfirmed {
                order_id,
                customer_id,
                timestamp,
            })?,
        ),
        OrderEvent::PaymentFailed(reason) => (
            ORDER_PAYMENT_FAILED,
            serde_json::to_value(OrderPaymentFailed {
                order_id,
                customer_id,
                reason: reason.clone(),
                timestamp,
            })?,
        ),
        OrderEvent::AuthorizationExpired
        | OrderEvent::CustomerCancelled
        | OrderEvent::OperatorCancelled(_) => (
            ORDER_CANCELLED,
            serde_json::to_value(OrderCancelled {
                reason: event.cancellation_reason().map(str::to_owned),
                actor: event.actor(&customer_id),
                order_id,
                customer_id,
                timestamp,
            })?,
        ),
        OrderEvent::DispatchConfirmed | OrderEvent::DispatchFailed => return Ok(None),
    }))
}

impl PgOrders {
    /// Attaches each order's lines (by SKU) in one query.
    async fn with_items(&self, orders: Vec<OrderRow>) -> Result<Vec<OrderWithItems>, StoreError> {
        if orders.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<Uuid> = orders.iter().map(|row| row.0.id).collect();
        let rows: Vec<(Uuid, Uuid, String, String, i32, i32)> = sqlx::query_as(
            "select order_id, id, sku, product_name, unit_price_cents, quantity \
             from order_items where order_id = any($1) order by order_id, sku",
        )
        .bind(&ids)
        .fetch_all(&self.pool)
        .await?;
        let mut items: HashMap<Uuid, Vec<OrderItem>> = HashMap::new();
        for (order_id, id, sku, product_name, unit_price_cents, quantity) in rows {
            items.entry(order_id).or_default().push(OrderItem {
                id,
                sku,
                product_name,
                unit_price_cents,
                quantity,
            });
        }
        Ok(orders
            .into_iter()
            .map(|OrderRow(order)| {
                let lines = items.remove(&order.id).unwrap_or_default();
                (order, lines)
            })
            .collect())
    }
}

/// Writes `event_type` for `order_id` in `tx`, stamped `now`.
async fn publish(
    tx: &mut Transaction<'_, Postgres>,
    event_type: &str,
    order_id: &str,
    payload: &serde_json::Value,
    now: OffsetDateTime,
) -> Result<(), StoreError> {
    let event = outbox::envelope(event_type, SOURCE, order_id, order_id, payload, now)?;
    // No span yet to parent consumers' traces.
    outbox::write(tx, &event, None).await?;
    Ok(())
}

impl Orders for PgOrders {
    async fn create_authorized(
        &self,
        order: &Order,
        items: &[OrderItem],
    ) -> Result<(OffsetDateTime, OffsetDateTime), StoreError> {
        let mut tx = self.pool.begin().await?;
        let address = order
            .shipping_address
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let stamps: (OffsetDateTime, OffsetDateTime) = sqlx::query_as(
            "insert into \"order\" (id, customer_id, total_cents, status, shipping_address, \
             guest_email) values ($1, $2, $3, $4, $5::jsonb, $6) returning created_at, updated_at",
        )
        .bind(order.id)
        .bind(&order.customer_id)
        .bind(order.total_cents)
        .bind(order.status.as_str())
        .bind(address)
        .bind(&order.guest_email)
        .fetch_one(&mut *tx)
        .await?;
        for item in items {
            sqlx::query(
                "insert into order_items (id, order_id, sku, product_name, unit_price_cents, \
                 quantity) values ($1, $2, $3, $4, $5, $6)",
            )
            .bind(item.id)
            .bind(order.id)
            .bind(&item.sku)
            .bind(&item.product_name)
            .bind(item.unit_price_cents)
            .bind(item.quantity)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(stamps)
    }

    async fn get(&self, id: Uuid) -> Result<Option<OrderWithItems>, StoreError> {
        let row: Option<OrderRow> = sqlx::query_as(&format!(
            "select {ORDER_COLUMNS} from \"order\" where id = $1"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(self
            .with_items(row.into_iter().collect())
            .await?
            .into_iter()
            .next())
    }

    async fn by_customer(&self, customer_id: &str) -> Result<Vec<OrderWithItems>, StoreError> {
        let rows: Vec<OrderRow> = sqlx::query_as(&format!(
            "select {ORDER_COLUMNS} from \"order\" where customer_id = $1 order by created_at desc"
        ))
        .bind(customer_id)
        .fetch_all(&self.pool)
        .await?;
        self.with_items(rows).await
    }

    async fn all(
        &self,
        status: Option<OrderStatus>,
        customer_id: Option<&str>,
    ) -> Result<Vec<OrderWithItems>, StoreError> {
        let rows: Vec<OrderRow> = sqlx::query_as(&format!(
            "select {ORDER_COLUMNS} from \"order\" \
             where ($1::text is null or status = $1) and ($2::text is null or customer_id = $2) \
             order by created_at desc, id"
        ))
        .bind(status.map(OrderStatus::as_str))
        .bind(customer_id)
        .fetch_all(&self.pool)
        .await?;
        self.with_items(rows).await
    }

    async fn transition(
        &self,
        id: Uuid,
        event: &OrderEvent,
    ) -> Result<Result<Transitioned, Rejected>, StoreError> {
        let allowed: Vec<&str> = event
            .allowed_from()
            .iter()
            .map(|status| status.as_str())
            .collect();
        let target = event.target();
        let mut tx = self.pool.begin().await?;
        let moved: Option<(Uuid, String, i32, String)> = sqlx::query_as(
            "with prev as (select id, status from \"order\" where id = $1 for update) \
             update \"order\" o set status = $2, updated_at = now() from prev \
             where o.id = prev.id and o.status = any($3) \
             returning o.id, o.customer_id, o.total_cents, prev.status",
        )
        .bind(id)
        .bind(target.as_str())
        .bind(&allowed)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((order_id, customer_id, total_cents, from)) = moved else {
            let current: Option<String> =
                sqlx::query_scalar("select status from \"order\" where id = $1")
                    .bind(id)
                    .fetch_optional(&mut *tx)
                    .await?;
            tx.commit().await?;
            return Ok(Err(match current.as_deref().and_then(OrderStatus::parse) {
                Some(from) => Rejected::Illegal { from, to: target },
                None => Rejected::UnknownOrder,
            }));
        };
        let moved = Transitioned {
            order_id,
            customer_id,
            total_cents,
            from: OrderStatus::parse(&from).ok_or_else(|| decode("status", &from))?,
        };
        let now = OffsetDateTime::now_utc();
        if let Some((event_type, payload)) = outgoing(event, &moved, now)? {
            publish(&mut tx, event_type, &id.to_string(), &payload, now).await?;
        }
        if let OrderEvent::PaymentFailed(reason) = event {
            sqlx::query(
                "insert into order_reconciliation (order_id, reason) values ($1, $2) \
                 on conflict (order_id) do nothing",
            )
            .bind(id)
            .bind(reason)
            .execute(&mut *tx)
            .await?;
        }
        if let (Some(reason), Some(actor)) =
            (event.cancellation_reason(), event.actor(&moved.customer_id))
        {
            sqlx::query(
                "insert into order_cancellation_audit (order_id, actor_type, actor_id, reason) \
                 values ($1, $2, $3, $4) on conflict (order_id) do nothing",
            )
            .bind(id)
            .bind(&actor.kind)
            .bind(&actor.id)
            .bind(reason)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(Ok(moved))
    }

    async fn flag(&self, id: Uuid, reason: &str) -> Result<bool, StoreError> {
        let flagged: Option<Uuid> = sqlx::query_scalar(
            "insert into order_reconciliation (order_id, reason) select $1, $2 \
             where exists (select 1 from \"order\" where id = $1) \
             on conflict (order_id) do nothing returning order_id",
        )
        .bind(id)
        .bind(reason)
        .fetch_optional(&self.pool)
        .await?;
        Ok(flagged.is_some())
    }

    async fn reconciliations(
        &self,
        status: Option<ReconciliationStatus>,
        order_id: Option<Uuid>,
    ) -> Result<Vec<Reconciliation>, StoreError> {
        let rows: Vec<ReconciliationRow> = sqlx::query_as(&format!(
            "select {RECONCILIATION_COLUMNS} from order_reconciliation \
             where ($1::text is null or status = $1) and ($2::uuid is null or order_id = $2) \
             order by created_at desc, order_id"
        ))
        .bind(status.map(ReconciliationStatus::as_str))
        .bind(order_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|row| row.0).collect())
    }

    async fn resolve(
        &self,
        id: Uuid,
        operator: &str,
        note: Option<&str>,
        at: OffsetDateTime,
    ) -> Result<ResolveOutcome, StoreError> {
        let mut tx = self.pool.begin().await?;
        let resolved: Option<ReconciliationRow> = sqlx::query_as(&format!(
            "update order_reconciliation set status = 'resolved', operator_id = $1, \
             note = $2, resolved_at = $3 where order_id = $4 and status = 'open' \
             returning {RECONCILIATION_COLUMNS}"
        ))
        .bind(operator)
        .bind(note)
        .bind(at)
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        let outcome = if let Some(row) = resolved {
            ResolveOutcome::Resolved(row.0)
        } else {
            let existing: Option<ReconciliationRow> = sqlx::query_as(&format!(
                "select {RECONCILIATION_COLUMNS} from order_reconciliation where order_id = $1"
            ))
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
            existing.map_or(ResolveOutcome::NotFound, |row| {
                ResolveOutcome::AlreadyResolved(row.0)
            })
        };
        tx.commit().await?;
        Ok(outcome)
    }

    async fn delete(&self, id: Uuid) -> Result<bool, StoreError> {
        let deleted = sqlx::query("delete from \"order\" where id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(deleted.rows_affected() > 0)
    }

    async fn ping(&self, within: Duration) -> bool {
        service_basics::postgres::ping(&self.pool, within)
            .await
            .is_ok()
    }
}
