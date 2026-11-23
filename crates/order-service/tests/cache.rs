// SPDX-License-Identifier: MIT

//! The history cache over a throwaway Redis (gluon-rs `service-testing`) and
//! the Postgres store: a history is cached for the TTL and a hit skips
//! Postgres; a malformed entry is discarded; Redis down is only slower.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::assert_is_empty,
    clippy::unused_async_trait_impl
)]

mod support;

use std::sync::Arc;

use order_service::cache::{TIMEOUT, key};
use order_service::store::PgOrders;
use redis_cache::Cache;
use service_testing::Redis;
use support::{Callee, Postgres, call, messages, placed, pool, state};

#[tokio::test]
async fn a_history_is_cached_and_a_hit_skips_postgres() {
    let redis = Redis::start();
    let postgres = Postgres::start();
    let pool = pool(&postgres).await;
    let callee = Callee::start().await;
    let (app, _) = state(PgOrders::new(pool.clone()), &callee, &redis.url());
    let order = placed(&app, "h1").await;
    let first = call(&app, "GET", "/orders?customerId=h1", None, &[]).await;
    assert_eq!(first.json()[0]["id"], order["id"]);
    let cache = Cache::new(&redis.url(), TIMEOUT).unwrap();
    assert!(cache.get(&key("h1")).await.unwrap().is_some());

    sqlx::query("delete from order_items")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("delete from \"order\"")
        .execute(&pool)
        .await
        .unwrap();
    let cached = call(&app, "GET", "/orders?customerId=h1", None, &[]).await;
    assert_eq!(cached.json(), first.json(), "served from Redis");
}

#[tokio::test]
async fn a_malformed_entry_is_discarded_and_redis_down_falls_back() {
    let redis = Redis::start();
    let postgres = Postgres::start();
    let callee = Callee::start().await;
    let (app, log) = state(PgOrders::new(pool(&postgres).await), &callee, &redis.url());
    let order = placed(&app, "h2").await;
    let cache = Arc::new(Cache::new(&redis.url(), TIMEOUT).unwrap());
    cache
        .set(&key("h2"), "not json", std::time::Duration::from_secs(60))
        .await
        .unwrap();
    let read = call(&app, "GET", "/orders?customerId=h2", None, &[]).await;
    assert_eq!(read.json()[0]["id"], order["id"]);
    assert!(messages(&log).contains(&"Discarding malformed order-history cache entry".to_owned()));

    redis.stop();
    let down = call(&app, "GET", "/orders?customerId=h2", None, &[]).await;
    assert_eq!(
        (down.status, down.json()[0]["id"].clone()),
        (200, order["id"].clone())
    );
    let logged = messages(&log);
    assert!(
        logged.contains(&"Order-history cache read failed - falling back to Postgres".to_owned())
    );
    assert!(logged.contains(&"Order-history cache write failed - response unaffected".to_owned()));
}
