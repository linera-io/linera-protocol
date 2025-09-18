// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;

use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use tokio::net::ToSocketAddrs;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[cfg(feature = "memory-profiling")]
use crate::memory_profiler::MemoryProfiler;

pub fn start_metrics(
    address: impl ToSocketAddrs + Debug + Send + 'static,
    shutdown_signal: CancellationToken,
) {
    #[cfg(feature = "memory-profiling")]
    let app = {
        // Try to add memory profiling endpoint
        match MemoryProfiler::check_prof_ctl() {
            Ok(()) => {
                info!("Memory profiling available, enabling /debug/pprof and /debug/flamegraph endpoints");
                Router::new()
                    .route("/metrics", get(serve_metrics))
                    .route("/debug/pprof", get(MemoryProfiler::heap_profile))
                    .route("/debug/flamegraph", get(MemoryProfiler::heap_flamegraph))
            }
            Err(e) => {
                tracing::warn!(
                    "Memory profiling not available: {}, serving metrics-only",
                    e
                );
                Router::new().route("/metrics", get(serve_metrics))
            }
        }
    };

    #[cfg(not(feature = "memory-profiling"))]
    let app = Router::new().route("/metrics", get(serve_metrics));

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .expect("Failed to bind to address");
        let address = listener.local_addr().expect("Failed to get local address");

        info!("Starting to serve metrics on {:?}", address);
        if let Err(e) = axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal.cancelled_owned())
            .await
        {
            panic!("Error serving metrics: {}", e);
        }
    });
}

async fn serve_metrics() -> Result<String, AxumError> {
    let metric_families = prometheus::gather();
    Ok(prometheus::TextEncoder::new()
        .encode_to_string(&metric_families)
        .map_err(anyhow::Error::from)?)
}

struct AxumError(anyhow::Error);

impl IntoResponse for AxumError {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for AxumError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
