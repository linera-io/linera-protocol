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

use std::sync::{Arc, Mutex};

use futures::StreamExt as _;
use linera_core::node::NodeError;
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

/// Answers one certificate with an error, so a shard's failure costs that certificate and not
/// every other chain the stream is carrying.
///
/// Needs no decoding: the sender names the chain and height on the request for exactly this.
///
/// `None` when the request names neither, because an answer the sender cannot attribute is one it
/// discards — leaving whoever pushed it waiting out the whole stream timeout for nothing.
fn refuse(
    request: &api::PushCertificateRequest,
    error: &str,
) -> Option<api::PushCertificateResponse> {
    let (chain_id, height) = (request.chain_id.clone()?, request.height?);
    Some(api::PushCertificateResponse {
        chain_id: Some(chain_id),
        height: Some(height),
        attempt: request.attempt,
        result: Some(api::ChainInfoResult {
            inner: Some(api::chain_info_result::Inner::Error(
                bincode::serialize(&NodeError::PushRefused {
                    error: error.to_string(),
                })
                .expect("a `NodeError` always serializes"),
            )),
        }),
    })
}

/// What one shard still owes an answer for, so its teardown can be answered per certificate.
///
/// A shard's stream dying must not reach the sender as a stream error: one stream carries every
/// chain going to this validator, and the sender ends the whole thing on a transport failure. So
/// the proxy answers that shard's outstanding certificates itself and leaves the stream up.
type Outstanding = Arc<Mutex<Vec<api::PushCertificateRequest>>>;

/// Fans a sender's single push stream out across the shards owning the chains it carries, and
/// merges their answers back onto one stream.
pub fn demultiplex<K, C, S>(
    mut certificates: Streaming<api::PushCertificateRequest>,
    shard_of: K,
    connect: C,
) -> ResponseStream
where
    // Split so the per-certificate cost is only the routing key: building a client is what the
    // second closure does, and it runs once per shard rather than once per certificate.
    K: Fn(&api::PushCertificateRequest) -> Result<(String, S), Status> + Send + 'static,
    C: Fn(&S) -> Result<ValidatorWorkerClient<Channel>, Status> + Send + 'static,
    S: Send + 'static,
{
    let (responses, response_receiver) = mpsc::channel(PUMP_QUEUE);
    tokio::spawn(async move {
        // One outbound stream per shard, opened the first time a chain of that shard appears.
        let mut shards: std::collections::HashMap<
            String,
            (mpsc::Sender<api::PushCertificateRequest>, Outstanding),
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
            let (key, shard) = match shard_of(&request) {
                Ok(shard) => shard,
                // One unroutable certificate, not the whole stream: this carries every chain
                // going to this validator, and the shard side states the same rule.
                Err(status) => {
                    // Unanswerable requests end the stream: the sender named no chain or height, so
                    // nothing we send back can be matched to what it is waiting for.
                    let Some(answer) = refuse(&request, status.message()) else {
                        responses
                            .send(Err(Status::invalid_argument(
                                "a pushed certificate must name its chain and height",
                            )))
                            .await
                            .ok();
                        break;
                    };
                    if responses.send(Ok(answer)).await.is_err() {
                        break;
                    }
                    continue;
                }
            };
            let (sender, outstanding) = match shards.get(&key) {
                Some((sender, outstanding)) if !sender.is_closed() => {
                    (sender.clone(), outstanding.clone())
                }
                _ => {
                    let mut client = match connect(&shard) {
                        Ok(client) => client,
                        Err(status) => {
                            // Unanswerable requests end the stream: the sender named no chain or height, so
                            // nothing we send back can be matched to what it is waiting for.
                            let Some(answer) = refuse(&request, status.message()) else {
                                responses
                                    .send(Err(Status::invalid_argument(
                                        "a pushed certificate must name its chain and height",
                                    )))
                                    .await
                                    .ok();
                                break;
                            };
                            if responses.send(Ok(answer)).await.is_err() {
                                break;
                            }
                            continue;
                        }
                    };
                    let (sender, receiver) = mpsc::channel(PUMP_QUEUE);
                    let outbound = match client
                        .push_confirmed_certificates(ReceiverStream::new(receiver))
                        .await
                    {
                        Ok(outbound) => outbound.into_inner(),
                        // A shard we cannot reach fails its own certificates; the shards that
                        // are up keep serving theirs.
                        Err(status) => {
                            // Unanswerable requests end the stream: the sender named no chain or height, so
                            // nothing we send back can be matched to what it is waiting for.
                            let Some(answer) = refuse(&request, status.message()) else {
                                responses
                                    .send(Err(Status::invalid_argument(
                                        "a pushed certificate must name its chain and height",
                                    )))
                                    .await
                                    .ok();
                                break;
                            };
                            if responses.send(Ok(answer)).await.is_err() {
                                break;
                            }
                            continue;
                        }
                    };
                    let outstanding: Outstanding = Arc::new(Mutex::new(Vec::new()));
                    tokio::spawn(pump_responses(
                        outbound,
                        responses.clone(),
                        outstanding.clone(),
                    ));
                    shards.insert(key.clone(), (sender.clone(), outstanding.clone()));
                    (sender, outstanding)
                }
            };
            // `try_send`, not `send`: waiting for one shard's queue to drain would park this
            // loop and stop every chain on every *other* shard with it — the head-of-line stall
            // that carrying many chains on one stream exists to avoid.
            // Recorded before the send, so a shard that dies mid-flight can still be told which
            // certificates it never answered.
            outstanding
                .lock()
                .expect("the outstanding table is never held across a panic")
                .push(request.clone());
            match sender.try_send(request) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(request)) => {
                    warn!(%key, "A shard is not keeping up with the push stream");
                    // Unanswerable requests end the stream: the sender named no chain or height, so
                    // nothing we send back can be matched to what it is waiting for.
                    let Some(answer) =
                        refuse(&request, "the shard is not keeping up with the stream")
                    else {
                        responses
                            .send(Err(Status::invalid_argument(
                                "a pushed certificate must name its chain and height",
                            )))
                            .await
                            .ok();
                        break;
                    };
                    if responses.send(Ok(answer)).await.is_err() {
                        break;
                    }
                }
                // The pump answers whatever this shard had outstanding, but not this one — it
                // never reached the queue — so it is refused here and the entry dropped, which
                // makes the next certificate for that shard reconnect.
                Err(mpsc::error::TrySendError::Closed(request)) => {
                    warn!(%key, "A shard's push stream closed; dropping it");
                    shards.remove(&key);
                    outstanding
                        .lock()
                        .expect("the outstanding table is never held across a panic")
                        .retain(|queued| {
                            (queued.chain_id.as_ref(), queued.height, queued.attempt)
                                != (request.chain_id.as_ref(), request.height, request.attempt)
                        });
                    let Some(answer) = refuse(&request, "the shard's push stream closed") else {
                        continue;
                    };
                    if responses.send(Ok(answer)).await.is_err() {
                        break;
                    }
                }
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
                responses
                    .send(Err(Status::invalid_argument(
                        "the first message of a relayed push must name the destination",
                    )))
                    .await
                    .ok();
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
                responses.send(Err(status)).await.ok();
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
                responses.send(Err(status)).await.ok();
                return;
            }
        };
        tokio::spawn(forward_responses(outbound, responses));
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
/// Forwards one peer's answers verbatim, errors included.
///
/// For [`relay`] only, where the proxy is a pipe to a single destination: an error there means the
/// one stream the shard is using has ended, and the shard has to be told. [`demultiplex`] must not
/// do this — it merges many shards onto one stream, so see [`pump_responses`].
async fn forward_responses(
    mut outbound: Streaming<api::PushCertificateResponse>,
    responses: mpsc::Sender<Result<api::PushCertificateResponse, Status>>,
) {
    while let Some(message) = outbound.next().await {
        if responses.send(message).await.is_err() {
            return;
        }
    }
}

async fn pump_responses(
    mut outbound: Streaming<api::PushCertificateResponse>,
    responses: mpsc::Sender<Result<api::PushCertificateResponse, Status>>,
    outstanding: Outstanding,
) {
    let mut ended = None;
    while let Some(message) = outbound.next().await {
        let answer = match message {
            Ok(answer) => answer,
            // NOT forwarded: an `Err` reaching the sender ends the merged stream, and this shard's
            // failure is not the other shards' problem. Its own certificates are refused below.
            Err(status) => {
                ended = Some(status);
                break;
            }
        };
        outstanding
            .lock()
            .expect("the outstanding table is never held across a panic")
            .retain(|request| {
                (request.chain_id.as_ref(), request.height, request.attempt)
                    != (answer.chain_id.as_ref(), answer.height, answer.attempt)
            });
        if responses.send(Ok(answer)).await.is_err() {
            return;
        }
    }
    // Whatever this shard never answered is refused here, so its senders retry instead of waiting
    // out the stream timeout for an answer that is no longer coming.
    let reason = ended.map_or_else(
        || "the shard closed its push stream".to_string(),
        |status| status.message().to_string(),
    );
    let stranded = std::mem::take(
        &mut *outstanding
            .lock()
            .expect("the outstanding table is never held across a panic"),
    );
    for request in stranded {
        let Some(answer) = refuse(&request, &reason) else {
            continue;
        };
        if responses.send(Ok(answer)).await.is_err() {
            return;
        }
    }
}
