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

use std::{collections::HashMap, sync::Arc, time::Duration};

use linera_base::identifiers::ChainId;
use linera_chain::types::ConfirmedBlockCertificate;
use linera_core::{node::NodeError, ProcessConfirmedBlockMode};
use linera_storage::Storage;
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
/// Retiring is what lets [`CHAINS_PER_STREAM`] bound the chains in flight rather than the chains
/// ever seen: the map holds a chain's only sender while its task lives, so nothing can prune it.
/// Only a round trip has to be survived — a sender writes a whole run without waiting — and every
/// second of slack here is another second of finished chains counting against the cap.
const CHAIN_IDLE: Duration = Duration::from_secs(1);

/// Chains one stream may have tasks for at once.
///
/// The id is attacker-chosen and the service is public, so without a cap a peer could name a
/// million chains and make us hold a task and a queue for each. The live set is roughly the
/// sender's in-flight window times how many chains it retires through in [`CHAIN_IDLE`], so this
/// sits well above that: reaching it is abuse rather than load.
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
        let mut refused_a_chain = false;
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
                    responses
                        .send(Err(Status::invalid_argument(error.to_string())))
                        .await
                        .ok();
                    break;
                }
            };
            let chain_id = certificate.inner().chain_id();
            // Chains whose task has finished are forgotten here rather than accumulating for the
            // life of the stream.
            chains.retain(|_, queue| !queue.is_closed());
            // Refusing the one certificate, not the stream: ending it here would punish every
            // other chain the stream carries for one peer naming too many at once.
            if !chains.contains_key(&chain_id) && chains.len() >= CHAINS_PER_STREAM {
                // Once per stream: this arm `continue`s, so logging every refusal would let one
                // peer drive unbounded output from a public endpoint.
                if !refused_a_chain {
                    refused_a_chain = true;
                    warn!(%chain_id, "Push stream naming more than {CHAINS_PER_STREAM} chains");
                }
                let refusal = NodeError::GrpcError {
                    error: format!(
                        "a push stream may carry at most {CHAINS_PER_STREAM} chains at once"
                    ),
                };
                if let Ok(result) = refusal.try_into() {
                    let answer = api::PushCertificateResponse {
                        chain_id: Some(chain_id.into()),
                        height: Some(certificate.block().header.height.into()),
                        result: Some(result),
                    };
                    if responses.send(Ok(answer)).await.is_err() {
                        break;
                    }
                }
                continue;
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
                    responses
                        .send(Err(Status::resource_exhausted(format!(
                            "more than {CHAIN_QUEUE} certificates outstanding for chain \
                             {chain_id}"
                        ))))
                        .await
                        .ok();
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

/// The next item, or `None` once the queue has been idle for `idle` and is finished.
///
/// Retiring is what lets the stream forget a chain it has finished with; the sender re-creates
/// the task on that chain's next certificate. The queue is **closed before** the last drain, so a
/// certificate the reader accepted in the instant before the deadline is still returned rather
/// than dropped — dropping it would leave its sender waiting out the whole push timeout for an
/// answer that is never coming.
async fn next_or_retire<T>(queue: &mut mpsc::Receiver<T>, idle: Duration) -> Option<T> {
    match timeout(idle, queue.recv()).await {
        Ok(item) => item,
        Err(_) => {
            queue.close();
            queue.recv().await
        }
    }
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
    while let Some(queued) = next_or_retire(&mut queue, CHAIN_IDLE).await {
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
            // stream must keep flowing when one chain's fail. Logged through the server's own
            // policy rather than at a flat level: a remote-caused failure like `BlobsNotFound`
            // is the designed blob-recovery handshake and belongs at debug, while a local one
            // like a poisoned worker must stay at error so it still trips alerting.
            Err(error) => {
                server.log_error(&error, "Failed to apply a pushed certificate");
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
                responses
                    .send(Err(Status::internal(error.to_string())))
                    .await
                    .ok();
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

    /// Retiring must close the queue, and must not drop what the reader already handed over.
    ///
    /// Drives `next_or_retire` itself rather than a copy of its loop: the previous two versions
    /// of this test asserted a property of a hand-written duplicate and stayed green under the
    /// regression they named. Deleting the `timeout` in `next_or_retire` makes the second
    /// assertion here hang, and deleting the `queue.close()` makes the third fail.
    #[test_log::test(tokio::test(start_paused = true))]
    async fn retiring_closes_the_queue_and_keeps_what_it_was_given() {
        let (sender, mut queue) = mpsc::channel::<u8>(CHAIN_QUEUE);

        sender.try_send(1).expect("the queue is empty");
        assert_eq!(
            next_or_retire(&mut queue, CHAIN_IDLE).await,
            Some(1),
            "a queued item must be returned without waiting for the deadline",
        );

        // Nothing more arrives, so the deadline retires the queue.
        assert_eq!(
            next_or_retire(&mut queue, CHAIN_IDLE).await,
            None,
            "an idle queue must retire so its entry can be pruned and its slot freed",
        );
        assert!(
            sender.is_closed(),
            "retiring must close the queue, or `retain` can never prune the entry and the cap \
             counts chains ever seen",
        );
    }

    /// Everything the reader handed over must survive retirement, not just the first item.
    ///
    /// Staged with reserved permits rather than a timer race. Under `start_paused` the clock
    /// jumps to the *earliest* pending deadline, so a writer sleeping longer than the receiver's
    /// idle can never win — the previous rewrite of this test deadlocked itself that way and was
    /// deterministically red. A reserved permit deposits into an already-closed queue, which is
    /// exactly the state the close-and-drain arm has to cope with.
    ///
    /// Deleting `queue.close()` leaves the sender open and the last assertion fails; deleting the
    /// whole `Err` arm strands both items and the first assertion fails.
    #[test_log::test(tokio::test(start_paused = true))]
    async fn retirement_drains_what_arrived_at_the_deadline() {
        let (sender, mut queue) = mpsc::channel::<u8>(CHAIN_QUEUE);
        let first = sender
            .clone()
            .reserve_owned()
            .await
            .expect("the queue is open");
        let second = sender
            .clone()
            .reserve_owned()
            .await
            .expect("the queue has room");

        // Deposited only once the receiver has given up waiting, so the drain is what returns it.
        let handing_over = tokio::spawn(async move {
            tokio::time::sleep(CHAIN_IDLE * 2).await;
            first.send(1);
            second.send(2);
        });

        assert_eq!(
            next_or_retire(&mut queue, CHAIN_IDLE).await,
            None,
            "an idle queue retires rather than waiting for a writer that has not arrived",
        );
        handing_over.await.expect("the writer does not panic");
        assert_eq!(
            next_or_retire(&mut queue, CHAIN_IDLE).await,
            Some(1),
            "an item handed over as the queue retired must still be returned",
        );
        assert_eq!(
            next_or_retire(&mut queue, CHAIN_IDLE).await,
            Some(2),
            "and so must every other item already reserved behind it",
        );
        assert!(
            sender.is_closed(),
            "retiring must close the queue so its entry can be pruned",
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
