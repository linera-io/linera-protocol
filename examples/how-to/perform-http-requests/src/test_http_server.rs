// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A simple HTTP server binary to use in the README test.

#![cfg_attr(target_arch = "wasm32", no_main)]
#![cfg(not(target_arch = "wasm32"))]

use std::{env, future::IntoFuture as _, net::Ipv4Addr};

use anyhow::anyhow;
use axum::{routing::get, Router};
use tokio::net::TcpListener;

/// The HTTP response expected by the contract.
const HTTP_RESPONSE_BODY: &str = "Hello, world!";

/// Runs an HTTP server that simple responds with the request expected by the contract.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let port = env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("Missing listen port argument"))?
        .parse()?;

    let listener = TcpListener::bind((Ipv4Addr::from([127, 0, 0, 1]), port)).await?;

    let router = Router::new().route("/", get(|| async { HTTP_RESPONSE_BODY }));

    axum::serve(listener, router).into_future().await?;

    Ok(())
}
