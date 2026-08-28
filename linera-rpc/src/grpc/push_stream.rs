// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The receiving half of a certificate push stream.
//!
//! One stream carries many chains at once, so the reader must never be the thing that waits. It
//! hands each certificate to a per-chain task and goes back to reading; the tasks apply at their
//! own pace and write answers back. A chain that is slow to apply therefore delays only itself.
//!
//! Two bounds keep that from turning into unbounded memory. Each chain's queue holds
//! [`CHAIN_QUEUE`] certificates, which is what a sender is allowed to have unacknowledged for one
//! chain — a well-behaved sender can never overflow it, and one that tries is refused rather than
//! served. Across all chains, [`STREAM_QUEUE`] bounds what is waiting to be applied; reaching it
//! stops the reader, which closes the HTTP/2 window and makes the sender wait, which is the
//! backpressure this design relies on instead of estimating the receiver's speed.

use std::{collections::HashMap, sync::Arc};

use linera_base::identifiers::ChainId;
use linera_chain::types::ConfirmedBlockCertificate;
use linera_core::{node::NodeError, ProcessConfirmedBlockMode};
use linera_storage::Storage;
use std::time::Duration;

use tokio::{
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
    time::timeout,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Status, Streaming};
use tracing::{debug, warn};

use super::{api, server::GrpcServer};

/// Certificates one chain may have queued, and so the most a sender may leave unacknowledged for
/// it. Sized to cover a round trip at a fast chain's apply rate, so the chain's task is not left
/// idle waiting for the next certificate to arrive.
pub const CHAIN_QUEUE: usize = 128;

/// Certificates the whole stream may have queued across every chain. This, not [`CHAIN_QUEUE`],
/// is what bounds memory: a count per chain multiplied by an unbounded number of chains is not a
/// bound at all.
pub const STREAM_QUEUE: usize = 1024;

/// How long a chain's task waits for another certificate before retiring.
///
/// Retiring is what makes [`CHAINS_PER_STREAM`] a bound on *concurrent* chains: while a task
/// lives, the map holds its sender, so nothing can prune it. A stream legitimately carries
/// thousands of chains over its life — every chain that ever falls behind this destination — and
/// only a handful at a time, so without this the cap would count chains ever seen and refuse the
/// stream partway through exactly the mass catch-up it exists to serve.
const CHAIN_IDLE: Duration = Duration::from_secs(30);

/// Chains one stream may have tasks for at once.
///
/// The id is attacker-chosen and the service is public, so without a cap a peer could name a
/// million chains and make us hold a task and a queue for each. A stream legitimately carries far
/// fewer at a time — the sender's own in-flight window bounds it — so reaching this is abuse or a
/// bug rather than load.
const CHAINS_PER_STREAM: usize = 256;

/// Answers flowing back to the sender. Bounded so a sender that stops reading cannot make us
/// accumulate answers without limit; the tasks producing them block instead.
const RESPONSE_QUEUE: usize = 256;

/// The stream of answers this server sends back.
pub type ResponseStream = ReceiverStream<Result<api::PushCertificateResponse, Status>>;

/// Serves one push stream: reads certificates, routes them per chain, and answers each one.
pub fn serve<S>(
    server: GrpcServer<S>,
    mut certificates: Streaming<api::PushCertificateRequest>,
) -> ResponseStream
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let (responses, response_receiver) = mpsc::channel(RESPONSE_QUEUE);
    // Held for as long as a certificate is queued or being applied, so the reader below stops
    // once `STREAM_QUEUE` of them are outstanding and the sender feels it as backpressure.
    let inflight = Arc::new(Semaphore::new(STREAM_QUEUE));

    tokio::spawn(async move {
        let mut chains: HashMap<ChainId, mpsc::Sender<Queued>> = HashMap::new();
        loop {
            // Taken before reading, not after: a permit acquired afterwards would mean the
            // certificate is already in hand and has to be held somewhere unbounded.
            let Ok(permit) = inflight.clone().acquire_owned().await else {
                break;
            };
            let request = match certificates.message().await {
                Ok(Some(request)) => request,
                Ok(None) => break,
                Err(error) => {
                    debug!(%error, "Certificate push stream ended");
                    break;
                }
            };
            let certificate = match ConfirmedBlockCertificate::try_from(request) {
                Ok(certificate) => certificate,
                // A certificate we cannot even read is the sender's bug, and answering it would
                // need a chain and height we do not have. Ending the stream makes it visible.
                Err(error) => {
                    let _ = responses
                        .send(Err(Status::invalid_argument(error.to_string())))
                        .await;
                    break;
                }
            };
            let chain_id = certificate.inner().chain_id();
            // Chains whose task has finished are forgotten here rather than accumulating for the
            // life of the stream.
            chains.retain(|_, queue| !queue.is_closed());
            if !chains.contains_key(&chain_id) && chains.len() >= CHAINS_PER_STREAM {
                warn!(%chain_id, "Push stream naming more than {CHAINS_PER_STREAM} chains at once");
                let _ = responses
                    .send(Err(Status::resource_exhausted(format!(
                        "a push stream may carry at most {CHAINS_PER_STREAM} chains at once"
                    ))))
                    .await;
                break;
            }
            let queue = chains.entry(chain_id).or_insert_with(|| {
                let (sender, receiver) = mpsc::channel(CHAIN_QUEUE);
                tokio::spawn(apply_chain(
                    server.clone(),
                    chain_id,
                    receiver,
                    responses.clone(),
                ));
                sender
            });
            let queued = Queued {
                certificate,
                permit,
            };
            // `try_send`, not `send`: waiting here for one chain's queue to drain is exactly the
            // head-of-line stall that carrying many chains on one stream exists to avoid. A
            // sender that respects `CHAIN_QUEUE` never reaches this.
            match queue.try_send(queued) {
                Ok(()) => {}
                // Either the sender overran its window, or it stopped reading answers and the
                // backlog reached here. Both end the stream and are reported as one, because from
                // here they are indistinguishable — telling them apart needs the sender's side.
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!(
                        %chain_id,
                        "Chain queue full: the sender is past its window or not reading answers"
                    );
                    let _ = responses
                        .send(Err(Status::resource_exhausted(format!(
                            "more than {CHAIN_QUEUE} certificates outstanding for chain \
                             {chain_id}"
                        ))))
                        .await;
                    break;
                }
                // The chain's task retired between the lookup and the send. Dropping the
                // certificate here would leave its sender waiting for an answer that never
                // comes, so a fresh task takes it.
                Err(mpsc::error::TrySendError::Closed(queued)) => {
                    let (sender, receiver) = mpsc::channel(CHAIN_QUEUE);
                    tokio::spawn(apply_chain(
                        server.clone(),
                        chain_id,
                        receiver,
                        responses.clone(),
                    ));
                    if sender.try_send(queued).is_err() {
                        warn!(%chain_id, "Could not hand a certificate to a fresh chain task");
                        break;
                    }
                    chains.insert(chain_id, sender);
                }
            }
        }
    });

    ReceiverStream::new(response_receiver)
}

/// A certificate waiting for its chain, holding the stream slot it occupies.
struct Queued {
    certificate: ConfirmedBlockCertificate,
    /// Released when the certificate has been answered, which is what lets the reader take
    /// another one.
    permit: OwnedSemaphorePermit,
}

/// Applies one chain's certificates in the order they arrived, answering each.
async fn apply_chain<S>(
    server: GrpcServer<S>,
    chain_id: ChainId,
    mut queue: mpsc::Receiver<Queued>,
    responses: mpsc::Sender<Result<api::PushCertificateResponse, Status>>,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    // Retiring on idle is what lets the stream forget a chain it has finished with; see
    // `CHAIN_IDLE`. The sender re-creates the task on its next certificate.
    while let Ok(Some(queued)) = timeout(CHAIN_IDLE, queue.recv()).await {
        let height = queued.certificate.block().header.height;
        let result = server
            .worker()
            .handle_confirmed_certificate(queued.certificate, ProcessConfirmedBlockMode::Auto, None)
            .await;
        let result = match result {
            Ok((info, actions)) => {
                server.handle_network_actions(actions);
                info.try_into()
            }
            // An answer, not the end of the stream: certificates for every other chain on this
            // stream must keep flowing when one chain's fail. Logged here as well as answered,
            // because the sender's copy names the peer while this one names the chain and the
            // worker that actually failed.
            Err(error) => {
                warn!(%chain_id, %error, "Failed to apply a pushed certificate");
                NodeError::from(error).try_into()
            }
        };
        let response = match result {
            Ok(result) => api::PushCertificateResponse {
                chain_id: Some(chain_id.into()),
                height: Some(height.into()),
                result: Some(result),
            },
            Err(error) => {
                let _ = responses
                    .send(Err(Status::internal(error.to_string())))
                    .await;
                return;
            }
        };
        // Dropped only now: the slot is occupied until the sender has its answer, so the window
        // reflects work outstanding rather than work merely read.
        drop(queued.permit);
        if responses.send(Ok(response)).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A chain's task must retire when idle, or the cap counts chains ever seen.
    ///
    /// The map holds a chain's only sender while its task lives, so `is_closed()` cannot become
    /// true on its own. Without retirement the live set only grows and the stream is refused
    /// partway through a mass catch-up — the workload it exists to serve.
    #[test_log::test(tokio::test(start_paused = true))]
    async fn an_idle_chain_task_retires_so_its_entry_can_be_pruned() {
        let (sender, mut receiver) = mpsc::channel::<u8>(CHAIN_QUEUE);
        let task = tokio::spawn(async move {
            while let Ok(Some(_)) = timeout(CHAIN_IDLE, receiver.recv()).await {}
        });

        assert!(
            !sender.is_closed(),
            "a live task must keep its entry, or a busy chain would be pruned mid-run",
        );
        tokio::time::sleep(CHAIN_IDLE * 2).await;
        task.await.expect("the task retires rather than panicking");
        assert!(
            sender.is_closed(),
            "an idle task must retire so `retain` can prune its entry and free a slot",
        );
    }

    /// The per-chain window a sender is held to has to fit in the queue that enforces it, or a
    /// sender behaving exactly as told would be cut off for it.
    #[test]
    fn the_window_a_sender_is_given_fits_the_queue_that_enforces_it() {
        assert!(
            linera_core::node::PUSH_WINDOW <= CHAIN_QUEUE,
            "a sender allowed {} in flight cannot be refused by a queue of {CHAIN_QUEUE}",
            linera_core::node::PUSH_WINDOW,
        );
        assert!(
            CHAIN_QUEUE <= STREAM_QUEUE,
            "one chain must not be able to fill the whole stream's budget by itself",
        );
    }
}
