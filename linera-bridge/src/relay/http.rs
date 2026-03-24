// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! HTTP handlers for the relay: deposit endpoint and monitor API.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use tokio::sync::{mpsc, oneshot, RwLock};
use tower_http::cors::CorsLayer;

use super::DepositRequest;
use crate::{
    monitor::MonitorState,
    proof::gen::{DepositProofClient, HttpDepositProofClient, ProofError},
};

/// Builds the axum Router with all relay HTTP routes.
pub(crate) fn build_router(
    proof_client: HttpDepositProofClient,
    deposit_tx: mpsc::Sender<DepositRequest>,
    monitor: Arc<RwLock<MonitorState>>,
) -> Router {
    let app_state = Arc::new(AppState {
        proof_client,
        deposit_tx,
        monitor,
    });

    Router::new()
        .route("/deposit", post(deposit_handler))
        .route("/monitor/status", get(monitor_status_handler))
        .route("/monitor/deposits", get(monitor_deposits_handler))
        .route("/monitor/burns", get(monitor_burns_handler))
        .layer(CorsLayer::permissive())
        .with_state(app_state)
}

struct AppState {
    proof_client: HttpDepositProofClient,
    deposit_tx: mpsc::Sender<DepositRequest>,
    monitor: Arc<RwLock<MonitorState>>,
}

#[derive(serde::Deserialize)]
struct DepositHttpRequest {
    tx_hash: String,
}

async fn deposit_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DepositHttpRequest>,
) -> impl IntoResponse {
    tracing::info!(tx_hash = %req.tx_hash, "Received deposit request");

    let tx_hash = match req.tx_hash.parse() {
        Ok(h) => h,
        Err(_) => {
            tracing::error!(tx_hash = %req.tx_hash, "Invalid tx_hash format");
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid tx_hash"})),
            );
        }
    };

    // Retry proof generation — on public testnets the RPC may not have
    // indexed the receipt yet when the frontend sends the tx hash.
    // Permanent errors (invalid tx, missing deposit event) fail immediately.
    tracing::info!(%tx_hash, "Generating deposit proof...");
    let mut proof = None;
    for attempt in 0..5 {
        match state.proof_client.generate_deposit_proof(tx_hash).await {
            Ok(p) => {
                tracing::info!(
                    %tx_hash,
                    tx_index = p.tx_index,
                    log_count = p.log_indices.len(),
                    "Deposit proof generated"
                );
                proof = Some(p);
                break;
            }
            Err(ProofError::Permanent(e)) => {
                tracing::error!(%tx_hash, "Deposit proof generation failed permanently: {e:#}");
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("{e:#}")})),
                );
            }
            Err(ProofError::Transient(e)) => {
                if attempt < 4 {
                    tracing::warn!(
                        %tx_hash, attempt, "Deposit proof generation failed, retrying: {e:#}"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(2 * (attempt + 1))).await;
                } else {
                    tracing::error!(%tx_hash, "Deposit proof generation failed after 5 attempts: {e:#}");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": format!("{e:#}")})),
                    );
                }
            }
        }
    }
    let proof = proof.unwrap();

    tracing::info!(%tx_hash, "Sending deposit to processing channel...");
    let (resp_tx, resp_rx) = oneshot::channel();
    if state
        .deposit_tx
        .send(DepositRequest {
            proof,
            response: resp_tx,
        })
        .await
        .is_err()
    {
        tracing::error!(%tx_hash, "Relay deposit channel closed");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "relay channel closed"})),
        );
    }

    match resp_rx.await {
        Ok(Ok(())) => {
            tracing::info!(%tx_hash, "Deposit processed successfully");
            (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
        }
        Ok(Err(e)) => {
            tracing::error!(%tx_hash, "Deposit processing failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e})),
            )
        }
        Err(_) => {
            tracing::error!(%tx_hash, "Deposit response channel closed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "channel closed"})),
            )
        }
    }
}

async fn monitor_status_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let summary = state.monitor.read().await.status_summary();
    Json(summary)
}

#[derive(serde::Deserialize)]
struct MonitorFilterParams {
    status: Option<String>,
}

async fn monitor_deposits_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<MonitorFilterParams>,
) -> impl IntoResponse {
    let monitor = state.monitor.read().await;
    let deposits: Vec<_> = match params.status.as_deref() {
        Some("pending") => monitor.pending_deposits().into_iter().cloned().collect(),
        Some("completed") => monitor.completed_deposits().into_iter().cloned().collect(),
        _ => monitor.all_deposits().into_iter().cloned().collect(),
    };
    Json(deposits)
}

async fn monitor_burns_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<MonitorFilterParams>,
) -> impl IntoResponse {
    let monitor = state.monitor.read().await;
    let burns: Vec<_> = match params.status.as_deref() {
        Some("pending") => monitor.pending_burns().into_iter().cloned().collect(),
        Some("completed") => monitor.completed_burns().into_iter().cloned().collect(),
        _ => monitor.all_burns().into_iter().cloned().collect(),
    };
    Json(burns)
}
