// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Carrying certificate push streams through the proxy, in both directions.
//!
//! A sender opens one stream per destination, but a validator's chains are spread across its
//! shards, so the receiving proxy cannot forward a stream whole: it reads each certificate,
//! opens a stream to that chain's shard the first time it sees one for it, and merges every
//! shard's answers back onto the single stream the sender is reading. [`demultiplex`] does that.
//!
//! On the sending side the proxy is a pipe: the shard names a destination in the first message
//! and the proxy opens the peer stream and forwards both directions verbatim, because shards hold
//! the validator's key and must not dial anyone. [`relay`] does that.
//!
//! Both directions are bounded by channels rather than by buffering: when a downstream stops
//! reading, the pump stops writing, which closes the HTTP/2 window back to the original sender.

use futures::StreamExt as _;
use linera_rpc::grpc::api::{
    self, validator_node_client::ValidatorNodeClient,
    validator_worker_client::ValidatorWorkerClient,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Channel, Status, Streaming};
use tracing::{debug, warn};

/// Messages buffered in either direction before the pump stops and lets the window close.
const PUMP_QUEUE: usize = 256;

/// The merged answers flowing back to whoever opened the stream.
pub type ResponseStream = ReceiverStream<Result<api::PushCertificateResponse, Status>>;

/// Fans a sender's single push stream out across the shards owning the chains it carries, and
/// merges their answers back onto one stream.
pub fn demultiplex<F>(
    mut certificates: Streaming<api::PushCertificateRequest>,
    shard_client: F,
) -> ResponseStream
where
    F: Fn(&api::PushCertificateRequest) -> Result<(String, ValidatorWorkerClient<Channel>), Status>
        + Send
        + 'static,
{
    let (responses, response_receiver) = mpsc::channel(PUMP_QUEUE);
    tokio::spawn(async move {
        // One outbound stream per shard, opened the first time a chain of that shard appears.
        let mut shards: std::collections::HashMap<
            String,
            mpsc::Sender<api::PushCertificateRequest>,
        > = std::collections::HashMap::new();
        while let Some(message) = certificates.next().await {
            let request = match message {
                Ok(request) => request,
                Err(error) => {
                    debug!(%error, "Inbound push stream ended");
                    break;
                }
            };
            // Keyed by the shard's address, which is what makes every chain living on one shard
            // share a single outbound stream rather than opening one stream per chain.
            let (key, mut client) = match shard_client(&request) {
                Ok(shard) => shard,
                Err(status) => {
                    let _ = responses.send(Err(status)).await;
                    break;
                }
            };
            let sender = match shards.get(&key) {
                Some(sender) if !sender.is_closed() => sender.clone(),
                _ => {
                    let (sender, receiver) = mpsc::channel(PUMP_QUEUE);
                    let outbound = match client
                        .push_confirmed_certificates(ReceiverStream::new(receiver))
                        .await
                    {
                        Ok(outbound) => outbound.into_inner(),
                        Err(status) => {
                            let _ = responses.send(Err(status)).await;
                            break;
                        }
                    };
                    tokio::spawn(pump_responses(outbound, responses.clone()));
                    shards.insert(key.clone(), sender.clone());
                    sender
                }
            };
            if sender.send(request).await.is_err() {
                warn!("A shard's push stream closed; dropping it");
                shards.remove(&key);
            }
        }
    });
    ReceiverStream::new(response_receiver)
}

/// Opens a stream to the named peer and forwards both directions verbatim.
pub fn relay<F>(mut inbound: Streaming<api::RelayPushRequest>, peer_client: F) -> ResponseStream
where
    F: FnOnce(
            &str,
        )
            -> futures::future::BoxFuture<'static, Result<ValidatorNodeClient<Channel>, Status>>
        + Send
        + 'static,
{
    let (responses, response_receiver) = mpsc::channel(PUMP_QUEUE);
    tokio::spawn(async move {
        // The destination has to arrive before anything can be forwarded, and naming it twice
        // would mean two different peers on one stream.
        let destination = match inbound.next().await {
            Some(Ok(api::RelayPushRequest {
                inner: Some(api::relay_push_request::Inner::Destination(destination)),
            })) => destination,
            Some(Ok(_)) => {
                let _ = responses
                    .send(Err(Status::invalid_argument(
                        "the first message of a relayed push must name the destination",
                    )))
                    .await;
                return;
            }
            Some(Err(error)) => {
                debug!(%error, "Relayed push stream ended before naming a destination");
                return;
            }
            None => return,
        };
        let mut client = match peer_client(&destination).await {
            Ok(client) => client,
            Err(status) => {
                let _ = responses.send(Err(status)).await;
                return;
            }
        };
        let (certificates, certificate_receiver) = mpsc::channel(PUMP_QUEUE);
        let outbound = match client
            .push_confirmed_certificates(ReceiverStream::new(certificate_receiver))
            .await
        {
            Ok(outbound) => outbound.into_inner(),
            Err(status) => {
                let _ = responses.send(Err(status)).await;
                return;
            }
        };
        tokio::spawn(pump_responses(outbound, responses));
        while let Some(message) = inbound.next().await {
            match message {
                Ok(api::RelayPushRequest {
                    inner: Some(api::relay_push_request::Inner::Certificate(certificate)),
                }) => {
                    if certificates.send(certificate).await.is_err() {
                        break;
                    }
                }
                // A second destination would silently retarget the stream mid-flight.
                Ok(_) => break,
                Err(error) => {
                    debug!(%error, "Relayed push stream ended");
                    break;
                }
            }
        }
    });
    ReceiverStream::new(response_receiver)
}

/// Forwards one peer's or shard's answers onto the merged stream.
async fn pump_responses(
    mut outbound: Streaming<api::PushCertificateResponse>,
    responses: mpsc::Sender<Result<api::PushCertificateResponse, Status>>,
) {
    while let Some(message) = outbound.next().await {
        if responses.send(message).await.is_err() {
            return;
        }
    }
}
