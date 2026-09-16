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

use futures::{Stream, StreamExt as _};
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
fn refuse(pending: &Pending, error: &str) -> Option<api::PushCertificateResponse> {
    let (chain_id, height) = (pending.chain_id.clone()?, pending.height?);
    Some(api::PushCertificateResponse {
        chain_id: Some(chain_id),
        height: Some(height),
        attempt: pending.attempt,
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

/// The identity an answer needs, which is all the proxy keeps while a certificate is in flight.
///
/// Deliberately not the request: that owns the certificate, and holding ~1280 of them per shard —
/// the outbound channel plus the shard's own window — at a 1 MB block limit is gigabytes for
/// something only three fields are ever read from.
#[derive(Clone, PartialEq)]
struct Pending {
    chain_id: Option<api::ChainId>,
    height: Option<api::BlockHeight>,
    attempt: u64,
}

impl Pending {
    fn of(request: &api::PushCertificateRequest) -> Self {
        Self {
            chain_id: request.chain_id.clone(),
            height: request.height,
            attempt: request.attempt,
        }
    }

    fn answers(&self, response: &api::PushCertificateResponse) -> bool {
        (&self.chain_id, self.height, self.attempt)
            == (&response.chain_id, response.height, response.attempt)
    }
}

/// Drops one entry from a shard's outstanding list, for a certificate that will never be answered.
fn forget(outstanding: &Outstanding, pending: &Pending) {
    outstanding
        .lock()
        .expect("the outstanding table is never held across a panic")
        .retain(|queued| queued != pending);
}

/// What one shard still owes an answer for, so its teardown can be answered per certificate.
///
/// A shard's stream dying must not reach the sender as a stream error: one stream carries every
/// chain going to this validator, and the sender ends the whole thing on a transport failure. So
/// the proxy answers that shard's outstanding certificates itself and leaves the stream up.
type Outstanding = Arc<Mutex<Vec<Pending>>>;

/// Fans a sender's single push stream out across the shards owning the chains it carries, and
/// merges their answers back onto one stream.
pub fn demultiplex<K, C, S, I>(mut certificates: I, shard_of: K, connect: C) -> ResponseStream
where
    // Generic over the inbound stream, not `Streaming`, so the loop can be driven by a channel in
    // a test: the arms that matter here are the ones that must NOT end the stream, and proving
    // that needs to observe what comes back after a failure.
    I: Stream<Item = Result<api::PushCertificateRequest, Status>> + Unpin + Send + 'static,
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
                    let Some(answer) = refuse(&Pending::of(&request), status.message()) else {
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
                    // A dead entry is drained before it is replaced. The pump refuses the snapshot
                    // it took and returns, but the read loop can keep writing until `is_closed`
                    // flips — and it is this arm, not the `Closed` one, that the guard above sends
                    // those to. Replacing without draining drops the only handle to them.
                    if let Some((_, stale)) = shards.remove(&key) {
                        strand(&responses, &stale, "the shard's push stream closed").await;
                    }
                    let client = match connect(&shard) {
                        Ok(client) => client,
                        Err(status) => {
                            let Some(answer) = refuse(&Pending::of(&request), status.message())
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
                            continue;
                        }
                    };
                    let (sender, receiver) = mpsc::channel(PUMP_QUEUE);
                    let outstanding: Outstanding = Arc::new(Mutex::new(Vec::new()));
                    // The dial happens on its own task, never here. Awaiting it in this loop
                    // parks every chain on every OTHER shard for as long as one unreachable
                    // shard takes to time out — the same head-of-line stall the `try_send`
                    // below exists to avoid, and the reason the sender dials outside its mutex.
                    tokio::spawn(open_and_pump(
                        client,
                        receiver,
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
            // Recorded BEFORE the send: an answer can come back before this line would otherwise
            // run, and an unrecorded answer would leave the entry behind forever. Every arm that
            // does not reach the shard removes it again.
            let pending = Pending::of(&request);
            outstanding
                .lock()
                .expect("the outstanding table is never held across a panic")
                .push(pending.clone());
            match sender.try_send(request) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!(%key, "A shard is not keeping up with the push stream");
                    // Never reached the shard, so nothing will ever answer it.
                    forget(&outstanding, &pending);
                    // Unanswerable requests end the stream: the sender named no chain or height, so
                    // nothing we send back can be matched to what it is waiting for.
                    let Some(answer) =
                        refuse(&pending, "the shard is not keeping up with the stream")
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
                // Refuses everything this shard still owed, not just this certificate: anything
                // written after the pump took its snapshot has nobody left to answer it. `pending`
                // is in the table too, having been recorded before the send.
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    warn!(%key, "A shard's push stream closed; dropping it");
                    shards.remove(&key);
                    strand(&responses, &outstanding, "the shard's push stream closed").await;
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
            .retain(|queued| !queued.answers(&answer));
        if responses.send(Ok(answer)).await.is_err() {
            return;
        }
    }
    let reason = ended.map_or_else(
        || "the shard closed its push stream".to_string(),
        |status| status.message().to_string(),
    );
    strand(&responses, &outstanding, &reason).await;
}

/// Opens a shard's stream and pumps its answers, refusing what it never answered if it fails.
///
/// Separate task on purpose: awaiting the dial in the read loop parks every other shard's chains.
async fn open_and_pump(
    mut client: ValidatorWorkerClient<Channel>,
    receiver: mpsc::Receiver<api::PushCertificateRequest>,
    responses: mpsc::Sender<Result<api::PushCertificateResponse, Status>>,
    outstanding: Outstanding,
) {
    match client
        .push_confirmed_certificates(ReceiverStream::new(receiver))
        .await
    {
        Ok(outbound) => pump_responses(outbound.into_inner(), responses, outstanding).await,
        // Dropping `receiver` here is what makes the next certificate for this shard see `Closed`
        // and reconnect; the ones already written are refused rather than left unanswered.
        Err(status) => strand(&responses, &outstanding, status.message()).await,
    }
}

/// Refuses everything a shard still owed an answer for, so its senders retry rather than wait out
/// the stream timeout for an answer that is no longer coming.
async fn strand(
    responses: &mpsc::Sender<Result<api::PushCertificateResponse, Status>>,
    outstanding: &Outstanding,
    reason: &str,
) {
    let stranded = std::mem::take(
        &mut *outstanding
            .lock()
            .expect("the outstanding table is never held across a panic"),
    );
    for pending in stranded {
        let Some(answer) = refuse(&pending, reason) else {
            continue;
        };
        if responses.send(Ok(answer)).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt as _;
    use linera_base::{crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId};

    use super::*;

    fn chain(seed: u8) -> api::ChainId {
        ChainId(CryptoHash::test_hash(format!("chain {seed}"))).into()
    }

    fn request(seed: u8, height: u64, attempt: u64) -> api::PushCertificateRequest {
        api::PushCertificateRequest {
            chain_id: Some(chain(seed)),
            certificate: None,
            supports_aggregated_missing: true,
            height: Some(BlockHeight(height).into()),
            attempt,
        }
    }

    /// The error a refusal carries, or `None` if it is not an in-band refusal at all.
    fn refusal_of(answer: &api::PushCertificateResponse) -> Option<NodeError> {
        match answer.result.as_ref()?.inner.as_ref()? {
            api::chain_info_result::Inner::Error(error) => bincode::deserialize(error).ok(),
            api::chain_info_result::Inner::ChainInfoResponse(_) => None,
        }
    }

    /// Drives `demultiplex` with a shard that cannot be reached, which is the failure ma2bd's
    /// first finding was about: it must cost those certificates and nothing else.
    fn against_unreachable_shards(requests: Vec<api::PushCertificateRequest>) -> ResponseStream {
        let inbound = futures::stream::iter(requests.into_iter().map(Ok));
        demultiplex(
            inbound,
            // Routes by chain, so different chains land on different shards.
            |request: &api::PushCertificateRequest| {
                let chain_id = request
                    .chain_id
                    .clone()
                    .ok_or_else(|| Status::invalid_argument("missing chain id"))?;
                Ok((format!("{chain_id:?}"), ()))
            },
            |_: &()| Err(Status::unavailable("the shard is down")),
        )
    }

    /// One shard's failure must not stop delivery for the chains on every other shard.
    ///
    /// This is the invariant the whole design exists for, and the one that broke in five different
    /// places: a `break` in this loop, an error variant the sender read as a dead transport, a
    /// status forwarded onto the shared channel, a dial awaited here, and a stale entry replaced
    /// without draining. Each certificate must come back refused, and the stream must stay open
    /// through all of them.
    #[test_log::test(tokio::test)]
    async fn a_dead_shard_costs_its_own_certificates_and_no_others() {
        let answers: Vec<_> = against_unreachable_shards(vec![
            request(1, 10, 0),
            request(2, 20, 0),
            request(3, 30, 0),
        ])
        .collect()
        .await;

        assert_eq!(
            answers.len(),
            3,
            "every certificate must be answered, not just the first: {answers:?}",
        );
        for answer in &answers {
            let answer = answer
                .as_ref()
                .expect("a shard failure is an answer, not a status");
            assert!(
                matches!(refusal_of(answer), Some(NodeError::PushRefused { .. })),
                "a refusal must be `PushRefused` — `GrpcError` makes the sender discard the whole \
                 stream, which is the same blast radius by another route: {answer:?}",
            );
        }
        let heights: Vec<_> = answers
            .iter()
            .filter_map(|answer| answer.as_ref().ok()?.height)
            .map(|height| height.height)
            .collect();
        assert_eq!(
            heights,
            vec![10, 20, 30],
            "each answer must name the certificate it refuses, or the sender cannot match it",
        );
    }

    /// An answer has to carry back the attempt it belongs to.
    ///
    /// Heights repeat across retries, so without the attempt a refusal from a run the sender has
    /// abandoned resolves the run that replaced it.
    #[test_log::test(tokio::test)]
    async fn a_refusal_names_the_attempt_it_answers() {
        let answers: Vec<_> = against_unreachable_shards(vec![request(1, 10, 7)])
            .collect()
            .await;
        let answer = answers[0].as_ref().expect("refused in band");
        assert_eq!(answer.attempt, 7, "the attempt must be echoed: {answer:?}");
    }

    /// A request naming no chain or height cannot be answered, so it ends the stream.
    ///
    /// The sender drops an answer it cannot attribute, so refusing such a request in band would
    /// leave whoever pushed it waiting out the whole stream timeout for nothing.
    #[test_log::test(tokio::test)]
    async fn an_unattributable_request_ends_the_stream() {
        let nameless = api::PushCertificateRequest {
            chain_id: None,
            ..request(1, 10, 0)
        };
        let answers: Vec<_> = against_unreachable_shards(vec![nameless, request(2, 20, 0)])
            .collect()
            .await;

        assert_eq!(
            answers.len(),
            1,
            "the stream ends at the bad request: {answers:?}"
        );
        let status = answers[0]
            .as_ref()
            .expect_err("it cannot be answered in band");
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
    }

    /// Losing a shard refuses everything it still owed, not only the certificate in hand.
    ///
    /// `pump_responses` takes a snapshot and returns, but the read loop can keep writing into a
    /// channel whose reader is already gone; those have nobody to answer them, and their senders
    /// would wait out the stream timeout.
    #[test_log::test(tokio::test)]
    async fn stranding_refuses_every_certificate_still_owed() {
        let (responses, receiver) = mpsc::channel(8);
        let outstanding: Outstanding = Arc::new(Mutex::new(vec![
            Pending::of(&request(1, 10, 0)),
            Pending::of(&request(1, 11, 0)),
            // Unanswerable, and must not stop the others being refused.
            Pending::of(&api::PushCertificateRequest {
                height: None,
                ..request(1, 12, 0)
            }),
        ]));

        strand(&responses, &outstanding, "the shard went away").await;
        drop(responses);

        assert!(
            outstanding.lock().expect("not poisoned").is_empty(),
            "stranding must empty the table, or the entries leak for the life of the stream",
        );
        let answers: Vec<_> = ReceiverStream::new(receiver).collect().await;
        assert_eq!(
            answers.len(),
            2,
            "both answerable certificates are refused; the third cannot be named: {answers:?}",
        );
    }

    /// The identity a refusal is matched on is the whole triple.
    ///
    /// The proxy records what it forwarded and clears it on the matching answer. A mismatch means
    /// the entry is never cleared — which is how an unvalidated wire height became an unbounded
    /// leak on a public endpoint.
    #[test]
    fn an_answer_clears_only_the_certificate_it_names() {
        let pending = Pending::of(&request(1, 10, 3));
        let answer = |height: u64, attempt: u64| api::PushCertificateResponse {
            chain_id: Some(chain(1)),
            height: Some(BlockHeight(height).into()),
            result: None,
            attempt,
        };
        assert!(pending.answers(&answer(10, 3)));
        assert!(
            !pending.answers(&answer(11, 3)),
            "a different height is a different certificate"
        );
        assert!(
            !pending.answers(&answer(10, 4)),
            "a different attempt is a different run"
        );
    }
}
