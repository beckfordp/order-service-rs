// SPDX-License-Identifier: MIT

//! The config contract: the Scala service's environment variables and defaults.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;
use std::time::Duration;

use order_service::config::{Config, PostgresConfig, TracingConfig, from_env};

fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

#[test]
fn defaults() {
    assert_eq!(
        from_env(&HashMap::new()).unwrap(),
        Config {
            port: 8080,
            service_name: "order-service".to_owned(),
            postgres: PostgresConfig {
                host: "localhost".to_owned(),
                port: 5432,
                database: "order".to_owned(),
                user: "order".to_owned(),
                password: "order".to_owned(),
            },
            kafka_bootstrap: "localhost:9092".to_owned(),
            redis_uri: "redis://localhost:6379".to_owned(),
            history_ttl: Duration::from_secs(60),
            inventory_url: "http://localhost:8081".to_owned(),
            payment_url: "http://localhost:8082".to_owned(),
            operator_token: "dev-operator-token".to_owned(),
            operator_id: "dev-operator".to_owned(),
            tracing: TracingConfig {
                otlp_endpoint: None,
                sampling_ratio: 1.0,
            },
        }
    );
}

#[test]
fn every_variable_overrides_its_default() {
    let config = from_env(&env(&[
        ("ORDER_SERVICE_PORT", "18080"),
        ("ORDER_SERVICE_NAME", "order-service-rs"),
        ("POSTGRES_HOST", "127.0.0.1"),
        ("POSTGRES_PORT", "15432"),
        ("POSTGRES_DB", "db"),
        ("POSTGRES_USER", "u"),
        ("POSTGRES_PASSWORD", "p"),
        ("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092"),
        ("REDIS_URI", "redis://127.0.0.1:16379"),
        ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318"),
        ("OTEL_TRACES_SAMPLER_ARG", "0.25"),
        ("ORDER_HISTORY_CACHE_TTL_SECONDS", "5"),
        ("INVENTORY_SERVICE_URL", "http://inventory:8080"),
        ("PAYMENT_SERVICE_URL", "http://payment:8080"),
        ("OPERATOR_TOKEN", "shared"),
        ("OPERATOR_ID", "ops"),
    ]))
    .unwrap();
    assert_eq!(config.port, 18080);
    assert_eq!(config.service_name, "order-service-rs");
    assert_eq!(config.postgres.port, 15432);
    assert_eq!(config.kafka_bootstrap, "127.0.0.1:19092");
    assert_eq!(config.redis_uri, "redis://127.0.0.1:16379");
    assert_eq!(config.tracing.sampling_ratio, 0.25);
    assert_eq!(config.history_ttl, Duration::from_secs(5));
    assert_eq!(config.inventory_url, "http://inventory:8080");
    assert_eq!(config.payment_url, "http://payment:8080");
    assert_eq!(
        (config.operator_token.as_str(), config.operator_id.as_str()),
        ("shared", "ops")
    );
}

#[test]
fn the_order_operator_variables_win_over_the_shared_ones() {
    let config = from_env(&env(&[
        ("OPERATOR_TOKEN", "shared"),
        ("ORDER_OPERATOR_TOKEN", "mine"),
        ("OPERATOR_ID", "ops"),
        ("ORDER_OPERATOR_ID", "order-ops"),
    ]))
    .unwrap();
    assert_eq!(config.operator_token, "mine");
    assert_eq!(config.operator_id, "order-ops");
}

#[test]
fn every_problem_is_reported() {
    let error = from_env(&env(&[
        ("ORDER_SERVICE_PORT", "http"),
        ("POSTGRES_PORT", "x"),
        ("ORDER_HISTORY_CACHE_TTL_SECONDS", "soon"),
        ("OPERATOR_ID", " "),
    ]))
    .unwrap_err();
    let text = error.to_string();
    assert!(text.contains("ORDER_HISTORY_CACHE_TTL_SECONDS"), "{text}");
    assert!(text.contains("operator-id"), "{text}");
    assert!(text.contains("ORDER_SERVICE_PORT"), "{text}");
    assert!(text.contains("POSTGRES_PORT"), "{text}");
}
