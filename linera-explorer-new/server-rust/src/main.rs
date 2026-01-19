// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod db;
mod metrics;
mod models;
mod routes;

use axum::{middleware, routing::get, Router};
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::net::SocketAddr;
use tower_http::cors::{Any, CorsLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let port: u16 = env::var("EXPLORER_API_PORT")
        .unwrap_or_else(|_| "3002".to_string())
        .parse()
        .expect("EXPLORER_API_PORT must be a valid port number");

    tracing::info!("Connecting to PostgreSQL database...");

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(std::time::Duration::from_secs(2))
        .idle_timeout(std::time::Duration::from_secs(30))
        .connect(&database_url)
        .await
        .expect("Failed to connect to database");

    tracing::info!("Connected to PostgreSQL database");

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/api/health", get(routes::health))
        .route("/api/stats", get(routes::get_stats))
        .route("/api/blocks", get(routes::get_blocks))
        .route("/api/blocks/{hash}", get(routes::get_block_by_hash))
        .route("/api/blocks/{hash}/bundles", get(routes::get_block_bundles))
        .route("/api/blocks/{hash}/bundles-with-messages", get(routes::get_block_bundles_with_messages))
        .route("/api/blocks/{hash}/operations", get(routes::get_block_operations))
        .route("/api/blocks/{hash}/messages", get(routes::get_block_messages))
        .route("/api/blocks/{hash}/events", get(routes::get_block_events))
        .route("/api/blocks/{hash}/oracle-responses", get(routes::get_block_oracle_responses))
        .route("/api/bundles/{id}/messages", get(routes::get_bundle_messages))
        .route("/api/chains", get(routes::get_chains))
        .route("/api/chains/count", get(routes::get_chains_count))
        .route("/api/chains/{chainId}", get(routes::get_chain_by_id))
        .route("/api/chains/{chainId}/blocks", get(routes::get_chain_blocks))
        .route("/api/chains/{chainId}/blocks/count", get(routes::get_chain_block_count))
        .route("/metrics", get(metrics::serve_metrics))
        .layer(middleware::from_fn(metrics::track_metrics))
        .layer(cors)
        .with_state(pool);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Blockchain Explorer API server running on port {}", port);
    tracing::info!("Health check: http://localhost:{}/api/health", port);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install CTRL+C signal handler");
    tracing::info!("Shutting down server...");
}
