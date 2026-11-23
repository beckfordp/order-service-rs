// SPDX-License-Identifier: MIT

//! The config contract: the Scala service's environment variables and
//! defaults (its `application.conf`). Pure: an environment map in, a
//! [`Config`] or every problem out; the pieces every service reads the same
//! way are gluon-rs `service-basics` and `telemetry`.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::time::Duration;

pub use service_basics::config::{ConfigError, PostgresConfig};
use service_basics::config::{port, postgres};
pub use telemetry::config::TracingConfig;
use telemetry::config::tracing_from_env;

/// The HTTP port's variable.
pub const PORT_VARIABLE: &str = "ORDER_SERVICE_PORT";

/// The history TTL's variable.
pub const TTL_VARIABLE: &str = "ORDER_HISTORY_CACHE_TTL_SECONDS";

/// Everything the service reads from its environment.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// `ORDER_SERVICE_PORT`: the HTTP API.
    pub port: u16,
    /// `ORDER_SERVICE_NAME`: `service.name` on spans, the logger name.
    pub service_name: String,
    /// `POSTGRES_*`.
    pub postgres: PostgresConfig,
    /// `KAFKA_BOOTSTRAP_SERVERS`: the consumers' and DLQ producer's brokers.
    pub kafka_bootstrap: String,
    /// `REDIS_URI`: the history cache (never the source of truth).
    pub redis_uri: String,
    /// `ORDER_HISTORY_CACHE_TTL_SECONDS`: how long a cached history stands (60 s).
    pub history_ttl: Duration,
    /// `INVENTORY_SERVICE_URL`: inventory-service.
    pub inventory_url: String,
    /// `PAYMENT_SERVICE_URL`: payment-service.
    pub payment_url: String,
    /// `ORDER_OPERATOR_TOKEN`, else `OPERATOR_TOKEN`: the dev operator token (ADR 0024).
    pub operator_token: String,
    /// `ORDER_OPERATOR_ID`, else `OPERATOR_ID`: the operator the token stands for.
    pub operator_id: String,
    /// `OTEL_*`.
    pub tracing: TracingConfig,
}

/// Reads the config from an environment map.
///
/// # Errors
/// [`ConfigError`] listing every invalid variable.
pub fn from_env<S: BuildHasher>(env: &HashMap<String, String, S>) -> Result<Config, ConfigError> {
    let mut problems = Vec::new();
    let text = |name: &str, default: &str| env.get(name).map_or(default, String::as_str).to_owned();
    let http_port = port(env, PORT_VARIABLE, 8080, &mut problems);
    let postgres = postgres(env, "order", &mut problems);
    let tracing = tracing_from_env(env, &mut problems);
    let history_ttl = match env.get(TTL_VARIABLE).map(|raw| raw.trim().parse::<u64>()) {
        None => 60,
        Some(Ok(seconds)) => seconds,
        Some(Err(_)) => {
            problems.push(format!("{TTL_VARIABLE} must be a whole number of seconds"));
            60
        }
    };
    let first = |names: [&str; 2], default: &str| {
        names
            .iter()
            .find_map(|name| env.get(*name))
            .map_or(default, String::as_str)
            .to_owned()
    };
    let operator_id = first(["ORDER_OPERATOR_ID", "OPERATOR_ID"], "dev-operator");
    if operator_id.trim().is_empty() {
        problems.push("operator-auth.operator-id must not be blank".to_owned());
    }
    if !problems.is_empty() {
        return Err(ConfigError { problems });
    }
    Ok(Config {
        port: http_port,
        service_name: text("ORDER_SERVICE_NAME", "order-service"),
        postgres,
        kafka_bootstrap: text("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        redis_uri: text("REDIS_URI", "redis://localhost:6379"),
        history_ttl: Duration::from_secs(history_ttl),
        inventory_url: text("INVENTORY_SERVICE_URL", "http://localhost:8081"),
        payment_url: text("PAYMENT_SERVICE_URL", "http://localhost:8082"),
        operator_token: first(
            ["ORDER_OPERATOR_TOKEN", "OPERATOR_TOKEN"],
            "dev-operator-token",
        ),
        operator_id,
        tracing,
    })
}
