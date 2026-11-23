// SPDX-License-Identifier: MIT

//! The Kafka consumers (Scala `KafkaConsumers.supervised`): `payment-events`
//! and `fulfilment-events`, each assembled and supervised by gluon-rs
//! `kafka-client`'s `run_supervised` — its own DLQ guard, restarted with
//! backoff when its stream fails — with `application.conf`'s `kafka.*`
//! settings (`Resilience::default`). Consuming needs a broker, so
//! tests/contract's consumer groups test it, not this crate's tests.

use std::time::Duration;

use kafka_client::KafkaError;
use kafka_client::settings::{ConsumerSettings, SettingsError};
use kafka_client::supervised::{ConsumerTelemetry, Resilience, run_supervised};
use tokio::sync::watch;

use crate::clients::Inventory;
use crate::events::{
    FULFILMENT_EVENTS, FULFILMENT_GROUP, FulfilmentEvents, PAYMENT_EVENTS, PAYMENT_GROUP,
    PaymentEvents,
};
use crate::store::Orders;

/// How often librdkafka reports the statistics the lag gauge reads.
pub const LAG_INTERVAL: Duration = Duration::from_secs(5);

/// One consumer's settings.
///
/// # Errors
/// A topic or group name Kafka doesn't allow.
pub fn settings(
    bootstrap: &str,
    topic: &str,
    group: &str,
) -> Result<ConsumerSettings, SettingsError> {
    Ok(ConsumerSettings {
        bootstrap: bootstrap.to_owned(),
        group_id: group.parse()?,
        topic: topic.parse()?,
        lag_interval: LAG_INTERVAL,
    })
}

/// Runs both consumers over `store` until `stop` is set.
///
/// # Errors
/// When a consumer can't be built (its settings, or its DLQ producer); a
/// failed stream is restarted, not returned.
pub async fn run<S: Orders>(
    bootstrap: &str,
    store: S,
    inventory: Inventory,
    telemetry: ConsumerTelemetry,
    stop: watch::Receiver<bool>,
) -> Result<(), KafkaError> {
    let created = |error: SettingsError| KafkaError::ClientCreation(error.to_string());
    let payments = settings(bootstrap, PAYMENT_EVENTS, PAYMENT_GROUP).map_err(created)?;
    let fulfilment = settings(bootstrap, FULFILMENT_EVENTS, FULFILMENT_GROUP).map_err(created)?;
    let resilience = Resilience::default();
    let logger = telemetry.logger.clone();
    tokio::try_join!(
        run_supervised(
            payments,
            &resilience,
            PaymentEvents::new(store.clone(), logger.clone()),
            telemetry.clone(),
            stop.clone(),
        ),
        run_supervised(
            fulfilment,
            &resilience,
            FulfilmentEvents::new(store, inventory, logger),
            telemetry,
            stop,
        ),
    )?;
    Ok(())
}
