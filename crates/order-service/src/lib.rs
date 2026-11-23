// SPDX-License-Identifier: MIT

//! The order-service binary: the Rust port of gluon's Scala
//! order-service (ADR 0029).
pub mod cache;
pub mod clients;
pub mod config;
pub mod consumers;
pub mod db;
pub mod docs;
pub mod domain;
pub mod events;
pub mod lifecycle;
pub mod published;
pub mod routes;
pub mod server;
pub mod store;

/// The package version, as built.
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
