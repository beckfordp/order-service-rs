// SPDX-License-Identifier: MIT

//! A customer's order history cached in Redis (Scala `OrderHistoryCache`):
//! cache-aside at [`key`] for the TTL (`ORDER_HISTORY_CACHE_TTL_SECONDS`, 60 s),
//! TTL-only freshness (no invalidation on a write). Redis is never the source
//! of truth: down, slow or holding something unreadable, the history comes from
//! the store, within the cache's timeout (the Scala client hangs instead:
//! gluon decision A13).

use std::sync::Arc;
use std::time::Duration;

use redis_cache::Cache;
use telemetry::logging::Logger;

use crate::routes::OrderResponse;

/// How long a cache call may take before the store answers instead.
pub const TIMEOUT: Duration = Duration::from_millis(500);

/// The key a customer's history is cached at (`v2`: TD-21 retired statuses
/// older entries may hold).
#[must_use]
pub fn key(customer_id: &str) -> String {
    format!("order-history:v2:{customer_id}")
}

/// The history cache.
#[derive(Debug, Clone)]
pub struct History {
    cache: Arc<Cache>,
    ttl: Duration,
    logger: Logger,
}

impl History {
    /// Histories in `cache` for `ttl`, failures logged to `logger`.
    #[must_use]
    pub fn new(cache: Arc<Cache>, ttl: Duration, logger: Logger) -> Self {
        Self { cache, ttl, logger }
    }

    /// The cached history, if one is there and reads; `None` otherwise (a
    /// failure or an unreadable entry logged).
    pub async fn get(&self, customer_id: &str) -> Option<Vec<OrderResponse>> {
        let key = key(customer_id);
        match self.cache.get(&key).await {
            Ok(Some(raw)) => match serde_json::from_str(&raw) {
                Ok(history) => Some(history),
                Err(error) => {
                    self.logger.warn(
                        &[
                            ("cache_key", &key),
                            ("bytes", &raw.len().to_string()),
                            ("error", &error.to_string()),
                        ],
                        "Discarding malformed order-history cache entry",
                    );
                    None
                }
            },
            Ok(None) => None,
            Err(error) => {
                self.logger.warn(
                    &[
                        ("customer_id", customer_id),
                        ("cache", "read_error"),
                        ("error", &error.to_string()),
                    ],
                    "Order-history cache read failed - falling back to Postgres",
                );
                None
            }
        }
    }

    /// Caches `history` for the TTL; a failure is logged, the response unaffected.
    pub async fn set(&self, customer_id: &str, history: &[OrderResponse]) {
        let written = match serde_json::to_string(history) {
            Ok(value) => self
                .cache
                .set(&key(customer_id), &value, self.ttl)
                .await
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        if let Err(error) = written {
            self.logger.warn(
                &[
                    ("customer_id", customer_id),
                    ("cache", "write_error"),
                    ("error", &error),
                ],
                "Order-history cache write failed - response unaffected",
            );
        }
    }
}
