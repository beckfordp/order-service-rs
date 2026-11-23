// SPDX-License-Identifier: MIT

//! The config contract: the Scala service's environment variables and defaults.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;

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
    ]))
    .unwrap();
    assert_eq!(config.port, 18080);
    assert_eq!(config.service_name, "order-service-rs");
    assert_eq!(config.postgres.port, 15432);
    assert_eq!(config.kafka_bootstrap, "127.0.0.1:19092");
    assert_eq!(config.redis_uri, "redis://127.0.0.1:16379");
    assert_eq!(config.tracing.sampling_ratio, 0.25);
}

#[test]
fn every_problem_is_reported() {
    let error = from_env(&env(&[
        ("ORDER_SERVICE_PORT", "http"),
        ("POSTGRES_PORT", "x"),
    ]))
    .unwrap_err();
    let text = error.to_string();
    assert!(text.contains("ORDER_SERVICE_PORT"), "{text}");
    assert!(text.contains("POSTGRES_PORT"), "{text}");
}
