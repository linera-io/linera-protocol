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

pub async fn get_stats(State(pool): State<PgPool>) -> Result<Json<Stats>, (StatusCode, Json<ErrorResponse>)> {
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
) -> Result<Json<Vec<BlockSummary>>, (StatusCode, Json<ErrorResponse>)> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    let blocks = db::get_blocks(&pool, limit, offset)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    Ok(Json(blocks))
}

pub async fn get_block_by_hash(
    State(pool): State<PgPool>,
    Path(hash): Path<String>,
) -> Result<Json<Block>, (StatusCode, Json<ErrorResponse>)> {
    let block = db::get_block_by_hash(&pool, &hash)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    match block {
        Some(b) => Ok(Json(b)),
        None => Err(not_found("Block not found")),
    }
}

pub async fn get_block_bundles(
    State(pool): State<PgPool>,
    Path(hash): Path<String>,
) -> Result<Json<Vec<IncomingBundle>>, (StatusCode, Json<ErrorResponse>)> {
    let bundles = db::get_incoming_bundles(&pool, &hash)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    Ok(Json(bundles))
}

pub async fn get_block_bundles_with_messages(
    State(pool): State<PgPool>,
    Path(hash): Path<String>,
) -> Result<Json<Vec<BundleWithMessages>>, (StatusCode, Json<ErrorResponse>)> {
    let bundles = db::get_block_with_bundles_and_messages(&pool, &hash)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    Ok(Json(bundles))
}

pub async fn get_bundle_messages(
    State(pool): State<PgPool>,
    Path(id): Path<i64>,
) -> Result<Json<Vec<PostedMessage>>, (StatusCode, Json<ErrorResponse>)> {
    let messages = db::get_posted_messages(&pool, id)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    Ok(Json(messages))
}

pub async fn get_block_operations(
    State(pool): State<PgPool>,
    Path(hash): Path<String>,
) -> Result<Json<Vec<Operation>>, (StatusCode, Json<ErrorResponse>)> {
    let operations = db::get_operations(&pool, &hash)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    Ok(Json(operations))
}

pub async fn get_block_messages(
    State(pool): State<PgPool>,
    Path(hash): Path<String>,
) -> Result<Json<Vec<OutgoingMessage>>, (StatusCode, Json<ErrorResponse>)> {
    let messages = db::get_messages(&pool, &hash)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    Ok(Json(messages))
}

pub async fn get_block_events(
    State(pool): State<PgPool>,
    Path(hash): Path<String>,
) -> Result<Json<Vec<Event>>, (StatusCode, Json<ErrorResponse>)> {
    let events = db::get_events(&pool, &hash)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    Ok(Json(events))
}

pub async fn get_block_oracle_responses(
    State(pool): State<PgPool>,
    Path(hash): Path<String>,
) -> Result<Json<Vec<OracleResponse>>, (StatusCode, Json<ErrorResponse>)> {
    let responses = db::get_oracle_responses(&pool, &hash)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    Ok(Json(responses))
}

pub async fn get_chains(
    State(pool): State<PgPool>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Vec<ChainStats>>, (StatusCode, Json<ErrorResponse>)> {
    let offset = params.offset.unwrap_or(0);

    let chains = db::get_chains(&pool, params.limit, offset)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    Ok(Json(chains))
}

pub async fn get_chains_count(
    State(pool): State<PgPool>,
) -> Result<Json<CountResponse>, (StatusCode, Json<ErrorResponse>)> {
    let count = db::get_chains_count(&pool)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    Ok(Json(CountResponse { count }))
}

pub async fn get_chain_by_id(
    State(pool): State<PgPool>,
    Path(chain_id): Path<String>,
) -> Result<Json<ChainStats>, (StatusCode, Json<ErrorResponse>)> {
    if !is_valid_chain_id(&chain_id) {
        return Err(bad_request("Chain ID must be a 64-character hex string"));
    }

    let chain = db::get_chain_by_id(&pool, &chain_id)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    match chain {
        Some(c) => Ok(Json(c)),
        None => Err(not_found("Chain not found")),
    }
}

pub async fn get_chain_blocks(
    State(pool): State<PgPool>,
    Path(chain_id): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Vec<BlockSummary>>, (StatusCode, Json<ErrorResponse>)> {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);

    let blocks = db::get_blocks_by_chain(&pool, &chain_id, limit, offset)
        .await
        .map_err(|e| internal_error(&e.to_string()))?;

    Ok(Json(blocks))
}

pub async fn get_chain_block_count(
    State(pool): State<PgPool>,
    Path(chain_id): Path<String>,
) -> Result<Json<CountResponse>, (StatusCode, Json<ErrorResponse>)> {
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
