// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use sqlx::postgres::PgPool;

use crate::db;
use crate::models::*;

/// Type alias for route handler results.
type RouteResult<T> = Result<Json<T>, (StatusCode, Json<ErrorResponse>)>;

#[derive(Deserialize)]
pub struct PaginationParams {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "OK".to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}

pub async fn get_stats(State(pool): State<PgPool>) -> RouteResult<Stats> {
    let total_blocks = db::get_total_block_count(&pool)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    let chains = db::get_chains(&pool, None, 0)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    let total_chains = chains.len() as i64;
    let top_chains: Vec<ChainStats> = chains.into_iter().take(5).collect();

    Ok(Json(Stats {
        total_blocks,
        total_chains,
        chains: top_chains,
    }))
}

pub async fn get_blocks(
    State(pool): State<PgPool>,
    Query(params): Query<PaginationParams>,
) -> RouteResult<Vec<BlockSummary>> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);
    db::get_blocks(&pool, limit, offset).await.to_json()
}

pub async fn get_block_by_hash(
    State(pool): State<PgPool>,
    Path(hash): Path<String>,
) -> RouteResult<Block> {
    db::get_block_by_hash(&pool, &hash)
        .await
        .to_json_or_not_found("Block not found")
}

pub async fn get_block_bundles(
    State(pool): State<PgPool>,
    Path(hash): Path<String>,
) -> RouteResult<Vec<IncomingBundle>> {
    db::get_incoming_bundles(&pool, &hash).await.to_json()
}

pub async fn get_block_bundles_with_messages(
    State(pool): State<PgPool>,
    Path(hash): Path<String>,
) -> RouteResult<Vec<BundleWithMessages>> {
    db::get_block_with_bundles_and_messages(&pool, &hash)
        .await
        .to_json()
}

pub async fn get_bundle_messages(
    State(pool): State<PgPool>,
    Path(id): Path<i64>,
) -> RouteResult<Vec<PostedMessage>> {
    db::get_posted_messages(&pool, id).await.to_json()
}

pub async fn get_block_operations(
    State(pool): State<PgPool>,
    Path(hash): Path<String>,
) -> RouteResult<Vec<Operation>> {
    db::get_operations(&pool, &hash).await.to_json()
}

pub async fn get_block_messages(
    State(pool): State<PgPool>,
    Path(hash): Path<String>,
) -> RouteResult<Vec<OutgoingMessage>> {
    db::get_messages(&pool, &hash).await.to_json()
}

pub async fn get_block_events(
    State(pool): State<PgPool>,
    Path(hash): Path<String>,
) -> RouteResult<Vec<Event>> {
    db::get_events(&pool, &hash).await.to_json()
}

pub async fn get_block_oracle_responses(
    State(pool): State<PgPool>,
    Path(hash): Path<String>,
) -> RouteResult<Vec<OracleResponse>> {
    db::get_oracle_responses(&pool, &hash).await.to_json()
}

pub async fn get_chains(
    State(pool): State<PgPool>,
    Query(params): Query<PaginationParams>,
) -> RouteResult<Vec<ChainStats>> {
    let offset = params.offset.unwrap_or(0);
    db::get_chains(&pool, params.limit, offset).await.to_json()
}

pub async fn get_chains_count(State(pool): State<PgPool>) -> RouteResult<CountResponse> {
    let count = db::get_chains_count(&pool)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;
    Ok(Json(CountResponse { count }))
}

pub async fn get_chain_by_id(
    State(pool): State<PgPool>,
    Path(chain_id): Path<String>,
) -> RouteResult<ChainStats> {
    if !is_valid_chain_id(&chain_id) {
        return Err(bad_request("Chain ID must be a 64-character hex string"));
    }
    db::get_chain_by_id(&pool, &chain_id)
        .await
        .to_json_or_not_found("Chain not found")
}

pub async fn get_chain_blocks(
    State(pool): State<PgPool>,
    Path(chain_id): Path<String>,
    Query(params): Query<PaginationParams>,
) -> RouteResult<Vec<BlockSummary>> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);
    db::get_blocks_by_chain(&pool, &chain_id, limit, offset)
        .await
        .to_json()
}

pub async fn get_chain_block_count(
    State(pool): State<PgPool>,
    Path(chain_id): Path<String>,
) -> RouteResult<CountResponse> {
    let count = db::get_chain_block_count(&pool, &chain_id)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;
    Ok(Json(CountResponse { count }))
}

fn is_valid_chain_id(chain_id: &str) -> bool {
    chain_id.len() == 64 && chain_id.chars().all(|c| c.is_ascii_hexdigit())
}

fn internal_error(msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    tracing::error!("{}", msg);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: "Internal server error".to_string(),
        }),
    )
}

/// Extension trait for converting database results to route responses.
trait DbResultExt<T> {
    fn to_json(self) -> RouteResult<T>;
}

impl<T> DbResultExt<T> for Result<T, sqlx::Error> {
    fn to_json(self) -> RouteResult<T> {
        self.map(Json).map_err(|e| internal_error(&e.to_string()))
    }
}

/// Extension trait for converting Option database results to route responses.
trait DbOptionResultExt<T> {
    fn to_json_or_not_found(self, msg: &str) -> RouteResult<T>;
}

impl<T> DbOptionResultExt<T> for Result<Option<T>, sqlx::Error> {
    fn to_json_or_not_found(self, msg: &str) -> RouteResult<T> {
        let opt = self.map_err(|e| internal_error(&e.to_string()))?;
        opt.map(Json).ok_or_else(|| not_found(msg))
    }
}

fn not_found(msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
}

fn bad_request(msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
}
