// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Prometheus metrics for the bridge relay.

use std::sync::LazyLock;

use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use prometheus::{Gauge, IntCounter, IntGauge, Opts, TextEncoder};
use tower_http::cors::CorsLayer;

static DEPOSITS_DETECTED: LazyLock<IntCounter> = LazyLock::new(|| {
    let opts = Opts::new(
        "bridge_deposits_detected",
        "Total deposits found by EVM scanner",
    )
    .namespace("linera");
    prometheus::register_int_counter!(opts).unwrap()
});

static DEPOSITS_COMPLETED: LazyLock<IntCounter> = LazyLock::new(|| {
    let opts =
        Opts::new("bridge_deposits_completed", "Deposits confirmed on Linera").namespace("linera");
    prometheus::register_int_counter!(opts).unwrap()
});

static DEPOSITS_PENDING: LazyLock<IntGauge> = LazyLock::new(|| {
    let opts =
        Opts::new("bridge_deposits_pending", "Currently pending deposits").namespace("linera");
    prometheus::register_int_gauge!(opts).unwrap()
});

static DEPOSITS_FAILED: LazyLock<IntGauge> = LazyLock::new(|| {
    let opts =
        Opts::new("bridge_deposits_failed", "Permanently failed deposits").namespace("linera");
    prometheus::register_int_gauge!(opts).unwrap()
});

static BURNS_DETECTED: LazyLock<IntCounter> = LazyLock::new(|| {
    let opts = Opts::new(
        "bridge_burns_detected",
        "Total burns found by Linera scanner",
    )
    .namespace("linera");
    prometheus::register_int_counter!(opts).unwrap()
});

static BURNS_COMPLETED: LazyLock<IntCounter> = LazyLock::new(|| {
    let opts = Opts::new("bridge_burns_completed", "Burns forwarded to EVM").namespace("linera");
    prometheus::register_int_counter!(opts).unwrap()
});

static BURNS_PENDING: LazyLock<IntGauge> = LazyLock::new(|| {
    let opts = Opts::new("bridge_burns_pending", "Currently pending burns").namespace("linera");
    prometheus::register_int_gauge!(opts).unwrap()
});

static BURNS_FAILED: LazyLock<IntGauge> = LazyLock::new(|| {
    let opts = Opts::new("bridge_burns_failed", "Permanently failed burns").namespace("linera");
    prometheus::register_int_gauge!(opts).unwrap()
});

static LAST_SCANNED_EVM_BLOCK: LazyLock<IntGauge> = LazyLock::new(|| {
    let opts = Opts::new(
        "bridge_last_scanned_evm_block",
        "Last scanned EVM block number",
    )
    .namespace("linera");
    prometheus::register_int_gauge!(opts).unwrap()
});

static LAST_SCANNED_LINERA_HEIGHT: LazyLock<IntGauge> = LazyLock::new(|| {
    let opts = Opts::new(
        "bridge_last_scanned_linera_height",
        "Last scanned Linera block height",
    )
    .namespace("linera");
    prometheus::register_int_gauge!(opts).unwrap()
});

static RELAYER_EVM_BALANCE_WEI: LazyLock<Gauge> = LazyLock::new(|| {
    let opts = Opts::new(
        "bridge_evm_balance_wei",
        "Relayer EVM account balance in wei",
    )
    .namespace("linera");
    prometheus::register_gauge!(opts).unwrap()
});

static RELAYER_LINERA_BALANCE_ATTO: LazyLock<Gauge> = LazyLock::new(|| {
    let opts = Opts::new(
        "bridge_linera_balance_atto",
        "Relayer Linera chain balance in attos",
    )
    .namespace("linera");
    prometheus::register_gauge!(opts).unwrap()
});

pub(crate) fn deposit_detected() {
    DEPOSITS_DETECTED.inc();
    DEPOSITS_PENDING.inc();
}

pub(crate) fn deposit_completed() {
    DEPOSITS_COMPLETED.inc();
    DEPOSITS_PENDING.dec();
}

pub(crate) fn deposit_failed() {
    DEPOSITS_PENDING.dec();
    DEPOSITS_FAILED.inc();
}

pub(crate) fn burn_detected() {
    BURNS_DETECTED.inc();
    BURNS_PENDING.inc();
}

pub(crate) fn burn_completed() {
    BURNS_COMPLETED.inc();
    BURNS_PENDING.dec();
}

pub(crate) fn burn_failed() {
    BURNS_PENDING.dec();
    BURNS_FAILED.inc();
}

pub(crate) fn set_last_scanned_evm_block(block: u64) {
    LAST_SCANNED_EVM_BLOCK.set(block as i64);
}

pub(crate) fn set_last_scanned_linera_height(height: u64) {
    LAST_SCANNED_LINERA_HEIGHT.set(height as i64);
}

pub(crate) fn set_relayer_evm_balance(balance_wei: f64) {
    RELAYER_EVM_BALANCE_WEI.set(balance_wei);
}

pub(crate) fn set_relayer_linera_balance(balance_atto: f64) {
    RELAYER_LINERA_BALANCE_ATTO.set(balance_atto);
}

pub(crate) fn build_router() -> Router {
    Router::new()
        .route("/metrics", get(serve_metrics))
        .route("/health", get(|| async { StatusCode::OK }))
        .layer(CorsLayer::permissive())
}

async fn serve_metrics() -> impl IntoResponse {
    let metric_families = prometheus::gather();
    match TextEncoder::new().encode_to_string(&metric_families) {
        Ok(text) => (StatusCode::OK, text).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to encode metrics: {e}"),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn health_endpoint_returns_200() {
        let response = build_router()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
