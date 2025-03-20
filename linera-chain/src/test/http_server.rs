// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A simple HTTP server to use for testing.

use std::{future::IntoFuture, net::Ipv4Addr};

use axum::Router;
use futures::FutureExt as _;
use tokio::{net::TcpListener, sync::oneshot};

/// A handle to a running HTTP server.
///
/// The server is gracefully shutdown when this handle is dropped.
pub struct HttpServer {
    port: u16,
    _shutdown_sender: oneshot::Sender<()>,
}

impl HttpServer {
    /// Spawns a task with an HTTP server serving the routes defined by the [`Router`].
    ///
    /// Returns an [`HttpServer`] handle to keep the server running in the background.
    pub async fn start(router: Router) -> anyhow::Result<Self> {
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let shutdown_signal = shutdown_receiver.map(|_| ());

        let listener = TcpListener::bind((Ipv4Addr::from([127, 0, 0, 1]), 0)).await?;
        let port = listener.local_addr()?.port();

        tokio::spawn(
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown_signal)
                .into_future(),
        );

        Ok(HttpServer {
            port,
            _shutdown_sender: shutdown_sender,
        })
    }

    /// Returns the URL string this HTTP server is listening on.
    pub fn url(&self) -> String {
        format!("http://{}:{}", self.hostname(), self.port())
    }

    /// Returns the hostname of this HTTP server.
    pub fn hostname(&self) -> String {
        "localhost".to_owned()
    }

    /// Returns the port this HTTP server is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }
}
