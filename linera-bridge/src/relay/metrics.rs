// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Prometheus metrics for the bridge relay.

use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use linera_base::prometheus_util::{
    register_gauge_with_subsystem, register_int_counter_with_subsystem,
    register_int_gauge_with_subsystem,
};
use prometheus::{Gauge, IntCounter, IntGauge, TextEncoder};
use tower_http::cors::CorsLayer;

/// Every metric below is registered under this subsystem, so none of the names carries a
/// `bridge_` prefix of its own — the exported name is `linera_bridge_<name>`.
const SUBSYSTEM: &str = "bridge";

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

linera_base::declare_metrics! {
    static DEPOSITS_DETECTED: IntCounter = register_int_counter_with_subsystem(
        SUBSYSTEM, "deposits_detected", "Total deposits found by EVM scanner");

    static DEPOSITS_COMPLETED: IntCounter = register_int_counter_with_subsystem(
        SUBSYSTEM, "deposits_completed", "Deposits confirmed on Linera");

    static DEPOSITS_PENDING: IntGauge = register_int_gauge_with_subsystem(
        SUBSYSTEM, "deposits_pending", "Currently pending deposits");

    static DEPOSITS_FAILED: IntGauge = register_int_gauge_with_subsystem(
        SUBSYSTEM, "deposits_failed", "Permanently failed deposits");

    static BURNS_DETECTED: IntCounter = register_int_counter_with_subsystem(
        SUBSYSTEM, "burns_detected", "Total burns found by Linera scanner");

    static BURNS_COMPLETED: IntCounter = register_int_counter_with_subsystem(
        SUBSYSTEM, "burns_completed", "Burns forwarded to EVM");

    static BURNS_PENDING: IntGauge = register_int_gauge_with_subsystem(
        SUBSYSTEM, "burns_pending", "Currently pending burns");

    static BURNS_FAILED: IntGauge = register_int_gauge_with_subsystem(
        SUBSYSTEM, "burns_failed", "Permanently failed burns");

    static LAST_SCANNED_EVM_BLOCK: IntGauge = register_int_gauge_with_subsystem(
        SUBSYSTEM, "last_scanned_evm_block", "Last scanned EVM block number");

    static LAST_SCANNED_LINERA_HEIGHT: IntGauge = register_int_gauge_with_subsystem(
        SUBSYSTEM, "last_scanned_linera_height", "Last scanned Linera block height");

    static RELAYER_EVM_BALANCE_WEI: Gauge = register_gauge_with_subsystem(
        SUBSYSTEM, "evm_balance_wei", "Relayer EVM account balance in wei");

    static RELAYER_LINERA_BALANCE_ATTO: Gauge = register_gauge_with_subsystem(
        SUBSYSTEM, "linera_balance_atto", "Relayer Linera chain balance in attos");
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

    /// The `bridge_` prefix moved out    /// the name strings cannot show the exported names are unchanged. Pin them instead.
    #[test]
    fn exported_names_are_unchanged_by_the_subsystem() {
        crate::init_metrics();

        let exported = prometheus::gather()
            .iter()
            .map(|family| family.get_name().to_owned())
            .collect::<std::collections::BTreeSet<_>>();

        for expected in [
            "linera_bridge_deposits_detected",
            "linera_bridge_deposits_completed",
            "linera_bridge_deposits_pending",
            "linera_bridge_deposits_failed",
            "linera_bridge_burns_detected",
            "linera_bridge_burns_completed",
            "linera_bridge_burns_pending",
            "linera_bridge_burns_failed",
            "linera_bridge_last_scanned_evm_block",
            "linera_bridge_last_scanned_linera_height",
            "linera_bridge_evm_balance_wei",
            "linera_bridge_linera_balance_atto",
        ] {
            assert!(exported.contains(expected), "{expected} is not exported");
        }
    }
}
