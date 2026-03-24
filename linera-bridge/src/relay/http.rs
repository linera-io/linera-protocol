// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! HTTP handlers for the relay: monitor API.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;

use crate::monitor::MonitorState;

/// Builds the axum Router with monitor HTTP routes.
pub(crate) fn build_router(monitor: Arc<RwLock<MonitorState>>) -> Router {
    Router::new()
        .route("/monitor/status", get(monitor_status_handler))
        .route("/monitor/deposits", get(monitor_deposits_handler))
        .route("/monitor/burns", get(monitor_burns_handler))
        .layer(CorsLayer::permissive())
        .with_state(monitor)
}

async fn monitor_status_handler(
    State(monitor): State<Arc<RwLock<MonitorState>>>,
) -> impl IntoResponse {
    let summary = monitor.read().await.status_summary();
    Json(summary)
}

#[derive(serde::Deserialize)]
struct MonitorFilterParams {
    status: Option<String>,
}

async fn monitor_deposits_handler(
    State(monitor): State<Arc<RwLock<MonitorState>>>,
    Query(params): Query<MonitorFilterParams>,
) -> impl IntoResponse {
    let monitor = monitor.read().await;
    let deposits: Vec<_> = match params.status.as_deref() {
        Some("pending") => monitor.pending_deposits().into_iter().cloned().collect(),
        Some("completed") => monitor.completed_deposits().into_iter().cloned().collect(),
        _ => monitor.all_deposits().into_iter().cloned().collect(),
    };
    Json(deposits)
}

async fn monitor_burns_handler(
    State(monitor): State<Arc<RwLock<MonitorState>>>,
    Query(params): Query<MonitorFilterParams>,
) -> impl IntoResponse {
    let monitor = monitor.read().await;
    let burns: Vec<_> = match params.status.as_deref() {
        Some("pending") => monitor.pending_burns().into_iter().cloned().collect(),
        Some("completed") => monitor.completed_burns().into_iter().cloned().collect(),
        _ => monitor.all_burns().into_iter().cloned().collect(),
    };
    Json(burns)
}
