// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;

use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use tokio::net::ToSocketAddrs;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[cfg(feature = "jemalloc")]
use crate::memory_profiler::MemoryProfiler;

/// Whether memory profiling endpoints should be enabled on the metrics server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryProfiling {
    Enabled,
    Disabled,
}

impl From<bool> for MemoryProfiling {
    fn from(enabled: bool) -> Self {
        if enabled {
            MemoryProfiling::Enabled
        } else {
            MemoryProfiling::Disabled
        }
    }
}

impl MemoryProfiling {
    /// Attempts to activate jemalloc memory profiling if requested.
    ///
    /// Returns `MemoryProfiling::Enabled` only if activation succeeds. If the flag is
    /// false, or jemalloc is not compiled in, or activation fails (e.g. `MALLOC_CONF` not
    /// set), returns `MemoryProfiling::Disabled` with an appropriate warning.
    #[cfg(feature = "jemalloc")]
    pub async fn try_activate(requested: bool) -> Self {
        if !requested {
            return MemoryProfiling::Disabled;
        }
        match MemoryProfiler::activate().await {
            Ok(()) => MemoryProfiling::Enabled,
            Err(e) => {
                tracing::warn!(
                    "--enable-memory-profiling was passed but profiling could not be activated: {}",
                    e
                );
                MemoryProfiling::Disabled
            }
        }
    }

    /// Non-jemalloc fallback: always returns `Disabled`, warns if profiling was requested.
    #[cfg(not(feature = "jemalloc"))]
    pub async fn try_activate(requested: bool) -> Self {
        if requested {
            tracing::warn!(
                "--enable-memory-profiling was passed but the jemalloc feature is not compiled in"
            );
        }
        MemoryProfiling::Disabled
    }
}

/// Activates memory profiling if requested and starts the metrics server.
///
/// This is the single entry point that handles both activation and server startup,
/// avoiding duplication across binaries.
pub async fn start_metrics_with_profiling(
    address: impl ToSocketAddrs + Debug + Send + 'static,
    shutdown_signal: CancellationToken,
    enable_memory_profiling: bool,
) {
    let memory_profiling = MemoryProfiling::try_activate(enable_memory_profiling).await;
    start_metrics(address, shutdown_signal, memory_profiling);
}

pub fn start_metrics(
    address: impl ToSocketAddrs + Debug + Send + 'static,
    shutdown_signal: CancellationToken,
    memory_profiling: MemoryProfiling,
) {
    start_metrics_with_extras(address, shutdown_signal, memory_profiling, None);
}

pub fn start_metrics_with_extras(
    address: impl ToSocketAddrs + Debug + Send + 'static,
    shutdown_signal: CancellationToken,
    memory_profiling: MemoryProfiling,
    extra_routes: Option<Router>,
) {
    let mut app = metrics_router(memory_profiling);

    if let Some(extra) = extra_routes {
        app = app.merge(extra);
    }

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

fn metrics_router(memory_profiling: MemoryProfiling) -> Router {
    #[cfg(feature = "jemalloc")]
    if memory_profiling == MemoryProfiling::Enabled {
        match MemoryProfiler::check_prof_ctl() {
            Ok(()) => {
                info!("Memory profiling enabled, registering /debug/pprof and /debug/flamegraph endpoints");
                return Router::new()
                    .route("/metrics", get(serve_metrics))
                    .route("/debug/pprof", get(MemoryProfiler::heap_profile))
                    .route("/debug/flamegraph", get(MemoryProfiler::heap_flamegraph));
            }
            Err(e) => {
                tracing::warn!(
                    "Memory profiling requested but not available: {}, serving metrics-only",
                    e
                );
            }
        }
    }

    #[cfg(not(feature = "jemalloc"))]
    if memory_profiling == MemoryProfiling::Enabled {
        tracing::warn!(
            "Memory profiling requested but jemalloc feature is not compiled in, serving metrics-only"
        );
    }

    Router::new().route("/metrics", get(serve_metrics))
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
