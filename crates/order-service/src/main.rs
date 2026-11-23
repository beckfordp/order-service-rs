// SPDX-License-Identifier: MIT

//! The `order-service` binary: reads the config contract, migrates the
//! database, then serves the HTTP API and runs the Kafka
//! consumers until SIGTERM or SIGINT. A consumer or
//! server that stops by itself stops the service, non-zero (Scala
//! `KafkaConsumers.watched`).

use kafka_client::supervised::ConsumerTelemetry;
use order_service::cache::{self, History};
use order_service::clients::{self, Inventory, Payment};
use order_service::config::{self, Config};
use order_service::domain::OperatorAuth;
use order_service::server::AppState;
use order_service::store::PgOrders;
use order_service::{consumers, lifecycle};
use order_service::{db, server};
use redis_cache::Cache;
use service_basics::{environment, runtime, shutdown};
use std::process::ExitCode;
use std::sync::Arc;
use telemetry::logging::Logger;
use telemetry::metrics::Metrics;
use telemetry::tracing::{SdkTracer, Tracing};
use tokio::sync::watch;

fn main() -> ExitCode {
    let config = match config::from_env(&environment::variables()) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{SERVICE}: {error}");
            return ExitCode::FAILURE;
        }
    };
    // Built outside the async runtime: the OTLP exporter's blocking HTTP
    // client must not be created (or dropped) on a runtime thread.
    let tracing = match Tracing::new(
        &config.service_name,
        config.tracing.otlp_endpoint.as_deref(),
        config.tracing.sampling_ratio,
    ) {
        Ok(tracing) => tracing,
        Err(error) => {
            eprintln!("{SERVICE}: {error}");
            return ExitCode::FAILURE;
        }
    };
    let runtime = match runtime::multi_thread() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("{SERVICE}: can't start the async runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    let code = runtime.block_on(serve(&config, tracing.tracer()));
    drop(runtime);
    tracing.shutdown();
    code
}

/// The service's name in its stderr lines and SIGTERM warning.
const SERVICE: &str = "order-service";

async fn serve(config: &Config, tracer: SdkTracer) -> ExitCode {
    let pool = match db::connect(&config.postgres).await {
        Ok(pool) => pool,
        Err(error) => {
            eprintln!(
                "{SERVICE}: database {}:{}/{} unreachable: {error}",
                config.postgres.host, config.postgres.port, config.postgres.database
            );
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = db::migrate(&pool).await {
        eprintln!("{SERVICE}: database migration failed: {error}");
        return ExitCode::FAILURE;
    }
    let logger = Logger::stdout(&config.service_name);
    logger.info(
        &[("port", &config.port.to_string())],
        "order-service starting",
    );
    let cache = match Cache::new(&config.redis_uri, cache::TIMEOUT) {
        Ok(cache) => Arc::new(cache),
        Err(error) => {
            eprintln!("{SERVICE}: {error}");
            return ExitCode::FAILURE;
        }
    };
    let metrics = Metrics::new(&config.service_name);
    let client = |base_url: &str| {
        clients::resilient(tracer.clone(), &metrics).map(|client| (base_url.to_owned(), client))
    };
    let (inventory, payment) = match (client(&config.inventory_url), client(&config.payment_url)) {
        (Ok((inventory_url, inventory)), Ok((payment_url, payment))) => (
            Inventory::new(&inventory_url, inventory),
            Payment::new(&payment_url, payment),
        ),
        (Err(error), _) | (_, Err(error)) => {
            eprintln!("{SERVICE}: can't build the HTTP client: {error}");
            return ExitCode::FAILURE;
        }
    };
    let store = PgOrders::new(pool);
    let state = AppState {
        store: store.clone(),
        history: History::new(cache, config.history_ttl, logger.clone()),
        inventory: inventory.clone(),
        payment,
        auth: OperatorAuth::new(&config.operator_token, &config.operator_id),
        logger: logger.clone(),
    };
    let telemetry = ConsumerTelemetry {
        tracer,
        logger: logger.clone(),
        metrics,
    };
    let (stop, stopped) = watch::channel(false);
    let server = {
        let (port, mut stopped) = (config.port, stopped.clone());
        async move {
            let until_stopped = async move {
                let _ = stopped.wait_for(|stop| *stop).await;
            };
            server::run(port, state, until_stopped).await
        }
    };
    let consumers = {
        let bootstrap = config.kafka_bootstrap.clone();
        async move { consumers::run(&bootstrap, store, inventory, telemetry, stopped).await }
    };
    let signal = shutdown::signal(SERVICE);
    if lifecycle::run_until_stopped(server, consumers, signal, stop, &logger).await {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
