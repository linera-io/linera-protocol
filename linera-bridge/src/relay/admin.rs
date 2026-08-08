// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Localhost-only admin HTTP endpoints for operator recovery actions.
//!
//! Re-queues `failed` burns/deposits back to pending so the normal retry
//! machinery picks them up without a relayer restart. Bound to 127.0.0.1
//! only — the localhost bind is the access control, since these routes
//! re-trigger fund-moving submissions (idempotent on-chain, operator-only).

use std::sync::Arc;

use alloy::primitives::B256;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use linera_base::data_types::BlockHeight;
use tokio::sync::{Notify, RwLock};

use crate::monitor::MonitorState;

#[derive(Clone)]
pub(crate) struct AdminState {
    pub monitor: Arc<RwLock<MonitorState>>,
    pub deposit_notify: Arc<Notify>,
    pub burn_notify: Arc<Notify>,
}

pub(crate) fn build_admin_router(state: AdminState) -> Router {
    Router::new()
        .route("/admin/retry/burns/{height}", post(retry_burns))
        .route(
            "/admin/retry/deposits/{source_chain_id}/{tx_hash}",
            post(retry_deposits),
        )
        .with_state(state)
}

async fn retry_burns(
    State(state): State<AdminState>,
    Path(height): Path<u64>,
) -> impl IntoResponse {
    let reset = state
        .monitor
        .write()
        .await
        .retry_failed_burns(BlockHeight(height))
        .await;
    if reset.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "status": "no_failed_items" })),
        )
            .into_response();
    }
    state.burn_notify.notify_one();
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "requeued", "count": reset.len(), "items": reset })),
    )
        .into_response()
}

async fn retry_deposits(
    State(state): State<AdminState>,
    Path((source_chain_id, tx_hash)): Path<(u64, String)>,
) -> impl IntoResponse {
    let tx_hash: B256 = match tx_hash.parse() {
        Ok(hash) => hash,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "status": "invalid_tx_hash" })),
            )
                .into_response();
        }
    };
    let reset = state
        .monitor
        .write()
        .await
        .retry_failed_deposits(source_chain_id, tx_hash)
        .await;
    if reset.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "status": "no_failed_items" })),
        )
            .into_response();
    }
    state.deposit_notify.notify_one();
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "requeued", "count": reset.len(), "items": reset })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use axum::{body::Body, http::Request};
    use linera_base::{crypto::CryptoHash, data_types::U128};
    use tower::ServiceExt;

    use super::*;
    use crate::monitor::PendingBurn;

    async fn state_with_failed_burn() -> AdminState {
        let mut ms = MonitorState::new(0);
        // A failed burn is removed from memory and lives in `finished_burns`,
        // so the retry path needs a DB to rebuild it from.
        ms.set_db(
            crate::monitor::db::BridgeDb::open_in_memory()
                .await
                .unwrap(),
        );
        ms.track_burn(PendingBurn {
            height: BlockHeight(5),
            block_hash: CryptoHash::from([0u8; 32]),
            tx_index: 0,
            event_pos_in_tx: 0,
            event_index: 10,
            evm_recipient: Address::ZERO,
            amount: U128(0),
        })
        .await;
        ms.mark_burn_failed(BlockHeight(5), 10).await;
        AdminState {
            monitor: Arc::new(RwLock::new(ms)),
            deposit_notify: Arc::new(Notify::new()),
            burn_notify: Arc::new(Notify::new()),
        }
    }

    #[tokio::test]
    async fn retry_burns_requeues_and_returns_200() {
        let state = state_with_failed_burn().await;
        let monitor = Arc::clone(&state.monitor);
        let response = build_admin_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/retry/burns/5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            monitor
                .write()
                .await
                .pending_burns_by_height_and_tx(10)
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn retry_burns_unknown_height_returns_404() {
        let state = state_with_failed_burn().await;
        let response = build_admin_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/retry/burns/999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn retry_burns_non_numeric_height_returns_400() {
        let state = state_with_failed_burn().await;
        let response = build_admin_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/retry/burns/notanumber")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn retry_deposits_bad_tx_hash_returns_400() {
        let state = state_with_failed_burn().await;
        let response = build_admin_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/retry/deposits/8453/notahash")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
