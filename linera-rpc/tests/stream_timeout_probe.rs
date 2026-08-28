// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Does a channel's per-request timeout bound a long-lived stream, or only the wait for headers?
//!
//! Every channel this crate builds sets `Endpoint::timeout` (`grpc::transport::create_channel`),
//! and block export sets it to 4 s. Whether that bounds the whole call decides whether export
//! could ever push over a stream, and tonic documents it only as "apply a timeout to each
//! request". This answers it against a real server rather than from the docs.
//!
//! `tonic_health`'s `Watch` is used purely because it is a server-streaming method that already
//! exists here; nothing about the question is specific to it.

#![cfg(not(target_arch = "wasm32"))]

use std::time::Duration;

use tokio_stream::wrappers::TcpListenerStream;
use tonic_health::{
    pb::{health_client::HealthClient, HealthCheckRequest},
    ServingStatus,
};

/// Well short of how long the stream is kept alive below, so a whole-call timeout must abort it.
const CLIENT_TIMEOUT: Duration = Duration::from_millis(300);
const STEP: Duration = Duration::from_millis(150);
const STEPS: usize = 8;

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn a_channel_timeout_does_not_bound_a_stream_body() -> anyhow::Result<()> {
    let (reporter, health) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("", ServingStatus::Serving)
        .await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(health)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                shutdown_rx.await.ok();
            }),
    );

    // Flip the status back and forth so the stream keeps producing messages for well past the
    // client's timeout.
    tokio::spawn(async move {
        for step in 0..STEPS {
            tokio::time::sleep(STEP).await;
            let status = if step % 2 == 0 {
                ServingStatus::NotServing
            } else {
                ServingStatus::Serving
            };
            reporter.set_service_status("", status).await;
        }
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))?
        .timeout(CLIENT_TIMEOUT)
        .connect()
        .await?;

    let mut stream = HealthClient::new(channel)
        .watch(HealthCheckRequest {
            service: String::new(),
        })
        .await?
        .into_inner();

    let started = std::time::Instant::now();
    let mut received = 0usize;
    let mut aborted = None;
    while started.elapsed() < STEP * STEPS as u32 {
        match stream.message().await {
            Ok(Some(_)) => received += 1,
            Ok(None) => break,
            Err(status) => {
                aborted = Some(status);
                break;
            }
        }
    }

    println!(
        "timeout={CLIENT_TIMEOUT:?} ran_for={:?} received={received} aborted={aborted:?}",
        started.elapsed(),
    );
    assert!(
        aborted.is_none(),
        "the channel timeout aborted the stream after {:?}: {aborted:?}",
        started.elapsed(),
    );
    assert!(
        started.elapsed() > CLIENT_TIMEOUT * 2,
        "the stream did not outlive the timeout, so this proved nothing",
    );

    shutdown.send(()).ok();
    server.await??;
    Ok(())
}
