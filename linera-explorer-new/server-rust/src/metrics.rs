// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::LazyLock;

use axum::{
    body::Body,
    extract::{MatchedPath, Request},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use prometheus::{exponential_buckets, histogram_opts, register_histogram_vec, HistogramVec, TextEncoder};

const LINERA_NAMESPACE: &str = "linera";

/// Histogram for tracking request latency per endpoint.
pub static REQUEST_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    let buckets = exponential_buckets(1.0, 2.0, 12).expect("Buckets creation should not fail");
    let histogram_opts =
        histogram_opts!("explorer_request_latency_ms", "Request latency in milliseconds", buckets)
            .namespace(LINERA_NAMESPACE);
    register_histogram_vec!(histogram_opts, &["endpoint", "method"])
        .expect("Histogram can be created")
});

/// Middleware that measures request latency and records it in Prometheus metrics.
pub async fn track_metrics(request: Request<Body>, next: Next) -> Response {
    let method = request.method().to_string();
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let start = std::time::Instant::now();
    let response = next.run(request).await;
    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

    REQUEST_LATENCY
        .with_label_values(&[&path, &method])
        .observe(latency_ms);

    response
}

/// Handler for the /metrics endpoint that serves Prometheus metrics.
pub async fn serve_metrics() -> Result<String, (StatusCode, String)> {
    let metric_families = prometheus::gather();
    TextEncoder::new()
        .encode_to_string(&metric_families)
        .map_err(|e| {
            tracing::error!("Failed to encode metrics: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to encode metrics".to_string(),
            )
        })
}
