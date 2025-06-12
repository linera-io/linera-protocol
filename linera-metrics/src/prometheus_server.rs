// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;

use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use tokio::net::ToSocketAddrs;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub fn start_metrics(
    address: impl ToSocketAddrs + Debug + Send + 'static,
    shutdown_signal: CancellationToken,
) {
    info!("Starting to serve metrics on {:?}", address);
    let prometheus_router = Router::new().route("/metrics", get(serve_metrics));

    tokio::spawn(async move {
        if let Err(e) = axum::serve(
            tokio::net::TcpListener::bind(address).await.unwrap(),
            prometheus_router,
        )
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
