// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! What a validator running an older binary answers when asked for a method it does not serve.
//!
//! The batch push falls back to one certificate per request on `Code::Unimplemented`, and that
//! is the only thing standing between a committee mid-upgrade and an old validator receiving no
//! blocks at all. Nothing else in this codebase depends on that code yet, so it is asserted
//! against a real tonic server rather than taken from the documentation.

#![cfg(not(target_arch = "wasm32"))]

use linera_rpc::grpc::api::{
    notifier_service_client::NotifierServiceClient,
    notifier_service_server::{NotifierService, NotifierServiceServer},
    validator_node_client::ValidatorNodeClient,
    validator_relay_client::ValidatorRelayClient,
    HandleConfirmedCertificatesRequest, NotificationBatch, RelayConfirmedCertificatesRequest,
};
use tonic::{transport::Server, Code, Request, Response, Status};

/// A server that serves one unrelated service, standing in for a binary that predates the batch
/// push: the methods below are absent from its router exactly as they are from an old validator's.
struct OlderValidator;

#[tonic::async_trait]
impl NotifierService for OlderValidator {
    /// Deliberately not `Unimplemented`: this is the positive control, and a code that a served
    /// method can also produce would make the assertions below pass against a broken server.
    async fn notify_batch(&self, _: Request<NotificationBatch>) -> Result<Response<()>, Status> {
        Err(Status::internal(SERVED))
    }
}

/// What the one served method answers, so "not routed" is distinguishable from "not reached".
const SERVED: &str = "this method is served";

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn an_absent_method_answers_unimplemented() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .add_service(NotifierServiceServer::new(OlderValidator))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    shutdown_rx.await.ok();
                },
            ),
    );

    let endpoint = format!("http://{address}");

    // The control: a method this server does serve comes back with the server's own answer, so
    // an `Unimplemented` below is the router refusing an absent method and not a dead server.
    let status = NotifierServiceClient::connect(endpoint.clone())
        .await?
        .notify_batch(NotificationBatch::default())
        .await
        .expect_err("the served method answers with an error of its own");
    assert_eq!(status.code(), Code::Internal);
    assert_eq!(status.message(), SERVED);

    // The destination-facing method, which is what a validator with an older binary is missing.
    let status = ValidatorNodeClient::connect(endpoint.clone())
        .await?
        .handle_confirmed_certificates(HandleConfirmedCertificatesRequest::default())
        .await
        .expect_err("a server that does not serve the method must refuse the call");
    assert_eq!(status.code(), Code::Unimplemented);

    // And the relay method, which is what our own proxy is missing while it is still rolling.
    let status = ValidatorRelayClient::connect(endpoint)
        .await?
        .relay_confirmed_certificates(RelayConfirmedCertificatesRequest::default())
        .await
        .expect_err("a server that does not serve the method must refuse the call");
    assert_eq!(status.code(), Code::Unimplemented);

    shutdown.send(()).ok();
    server.await??;
    Ok(())
}
