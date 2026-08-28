// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The sending half of a certificate push stream.
//!
//! One stream carries every chain going to a destination, so answers come back interleaved and
//! out of step with what was written. Each response names the chain and height it answers, which
//! is what lets a caller waiting on one chain be woken by its own answer and no one else's.
//!
//! A caller waits only on the *last* certificate of the run it pushed: the destination applies a
//! chain's certificates in order and answers them in order, so that answer implies the ones
//! before it. That is what removes the round trip per certificate — the whole run is written
//! before anything is awaited.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures::{Stream, StreamExt as _};
use linera_base::{data_types::BlockHeight, identifiers::ChainId};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_core::{
    data_types::ChainInfoResponse,
    node::{CertificatePushStream, NodeError, PUSH_WINDOW},
};
use linera_storage::Arc as CacheArc;
use tokio::sync::{mpsc, oneshot};
use tonic::Status;
use tracing::debug;

use super::{api, conversions::push_certificate_request};

/// Certificates buffered before writing blocks. Only a bound on the local queue — the destination
/// enforces its own window, and HTTP/2 flow control is what actually paces the wire.
const WRITE_QUEUE: usize = PUSH_WINDOW;

/// What a caller is waiting for: the answer to one chain's certificate at one height.
type Waiter = oneshot::Sender<Result<ChainInfoResponse, NodeError>>;

/// A live push stream to one validator.
pub struct PushStream {
    certificates: mpsc::Sender<api::PushCertificateRequest>,
    waiters: Arc<Mutex<HashMap<ChainId, Vec<(BlockHeight, Waiter)>>>>,
}

impl PushStream {
    /// Starts routing a destination's answers to whoever is waiting for them.
    ///
    /// `certificates` is the writing half the caller pushes into; `responses` is what the
    /// destination sends back.
    pub(crate) fn new(
        certificates: mpsc::Sender<api::PushCertificateRequest>,
        responses: impl Stream<Item = Result<api::PushCertificateResponse, Status>>
            + Send
            + Unpin
            + 'static,
    ) -> Self {
        let waiters = Arc::new(Mutex::new(HashMap::new()));
        tokio::spawn(route_responses(responses, waiters.clone()));
        Self {
            certificates,
            waiters,
        }
    }

    /// Registers interest in one chain's answer at `height`, before anything is written.
    ///
    /// Registered first on purpose: an answer can arrive while the run is still being written,
    /// and a waiter added afterwards would have missed it and hung until the stream closed.
    fn expect(
        &self,
        chain_id: ChainId,
        height: BlockHeight,
    ) -> oneshot::Receiver<Result<ChainInfoResponse, NodeError>> {
        let (sender, receiver) = oneshot::channel();
        self.waiters
            .lock()
            .expect("the waiter table is never held across a panic")
            .entry(chain_id)
            .or_default()
            .push((height, sender));
        receiver
    }

    /// Drops a chain's waiter at `height`, so a run that failed to write does not leak one.
    fn forget(&self, chain_id: ChainId, height: BlockHeight) {
        let mut waiters = self
            .waiters
            .lock()
            .expect("the waiter table is never held across a panic");
        if let Some(chain) = waiters.get_mut(&chain_id) {
            chain.retain(|(waiting, _)| *waiting != height);
            if chain.is_empty() {
                waiters.remove(&chain_id);
            }
        }
    }
}

impl CertificatePushStream for PushStream {
    async fn push(
        &self,
        certificates: Vec<CacheArc<ConfirmedBlockCertificate>>,
    ) -> Result<ChainInfoResponse, NodeError> {
        let last = certificates
            .last()
            .ok_or(NodeError::EmptyCertificateRun)?
            .block();
        let (chain_id, height) = (last.header.chain_id, last.header.height);
        if certificates.len() > PUSH_WINDOW {
            return Err(NodeError::PushRunTooLong {
                chain_id,
                length: certificates.len(),
            });
        }
        let answer = self.expect(chain_id, height);
        for certificate in &certificates {
            let request = push_certificate_request(certificate)?;
            if self.certificates.send(request).await.is_err() {
                self.forget(chain_id, height);
                return Err(NodeError::PushStreamClosed);
            }
        }
        answer.await.map_err(|_| NodeError::PushStreamClosed)?
    }
}

/// Wakes each waiter with the answer that names it, until the destination stops answering.
///
/// A chain's answers arrive in height order, so every waiter at or below the height answered is
/// satisfied by it — a caller that pushed a run is woken by the run's last certificate, and one
/// whose certificates the destination had already applied is woken by the next answer past them
/// rather than waiting for one that will never come.
async fn route_responses(
    mut responses: impl Stream<Item = Result<api::PushCertificateResponse, Status>> + Unpin,
    waiters: Arc<Mutex<HashMap<ChainId, Vec<(BlockHeight, Waiter)>>>>,
) {
    while let Some(message) = responses.next().await {
        let response = match message {
            Ok(response) => response,
            Err(status) => {
                debug!(%status, "Certificate push stream ended");
                break;
            }
        };
        let Some(answered) = read_answer(response) else {
            continue;
        };
        let (chain_id, height, result) = answered;
        let woken = {
            let mut table = waiters
                .lock()
                .expect("the waiter table is never held across a panic");
            let Some(chain) = table.get_mut(&chain_id) else {
                continue;
            };
            let (woken, waiting): (Vec<_>, Vec<_>) =
                chain.drain(..).partition(|(at, _)| *at <= height);
            *chain = waiting;
            if chain.is_empty() {
                table.remove(&chain_id);
            }
            woken
        };
        for (_, waiter) in woken {
            let _ = waiter.send(result.clone());
        }
    }
    // Nothing more is coming, so anyone still waiting must be told rather than left hanging.
    let table = std::mem::take(
        &mut *waiters
            .lock()
            .expect("the waiter table is never held across a panic"),
    );
    for (_, chain) in table {
        for (_, waiter) in chain {
            let _ = waiter.send(Err(NodeError::PushStreamClosed));
        }
    }
}

/// Reads one answer, or `None` if it is too malformed to attribute to a waiter.
fn read_answer(
    response: api::PushCertificateResponse,
) -> Option<(ChainId, BlockHeight, Result<ChainInfoResponse, NodeError>)> {
    let chain_id = ChainId::try_from(response.chain_id?).ok()?;
    let height = BlockHeight::from(response.height?);
    let result = match response.result?.inner? {
        api::chain_info_result::Inner::ChainInfoResponse(info) => {
            info.try_into().map_err(|error| NodeError::GrpcError {
                error: format!("failed to unmarshal response: {error}"),
            })
        }
        // bincode, matching what the validator used. Load-bearing: the recovery paths dispatch on
        // the *variant*, so a mangled error silently disables them.
        api::chain_info_result::Inner::Error(error) => Err(bincode::deserialize(&error)
            .unwrap_or_else(|error| NodeError::GrpcError {
                error: format!("failed to unmarshal error message: {error}"),
            })),
    };
    Some((chain_id, height, result))
}

/// The write half handed to a transport, plus the queue it drains.
pub(crate) fn write_half() -> (
    mpsc::Sender<api::PushCertificateRequest>,
    mpsc::Receiver<api::PushCertificateRequest>,
) {
    mpsc::channel(WRITE_QUEUE)
}

/// Turns a transport error into the status a caller sees when the stream cannot be opened.
pub(crate) fn open_error(status: Status) -> NodeError {
    if status.code() == tonic::Code::Unimplemented {
        NodeError::PushStreamUnsupported
    } else {
        NodeError::GrpcError {
            error: format!("cannot open a certificate push stream: {status:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::channel::mpsc as futures_mpsc;

    use super::*;

    fn chain(seed: u8) -> ChainId {
        ChainId(linera_base::crypto::CryptoHash::test_hash(format!(
            "chain {seed}"
        )))
    }

    fn answer(chain_id: ChainId, height: u64) -> api::PushCertificateResponse {
        api::PushCertificateResponse {
            chain_id: Some(chain_id.into()),
            height: Some(BlockHeight(height).into()),
            // An absent result reads as malformed and is skipped, which is what the "ignores what
            // it cannot attribute" case below relies on; the wake-up cases set one.
            result: None,
        }
    }

    /// The whole correctness of a multi-chain stream reduces to this: an answer must wake the
    /// waiter that asked for it and no other, or a push resolves with a different chain's outcome
    /// and its cursor advances over blocks the destination never took.
    #[test_log::test(tokio::test)]
    async fn an_answer_wakes_only_its_own_chain() {
        let (certificates, _queue) = mpsc::channel(4);
        let (mut responses, response_stream) = futures_mpsc::unbounded();
        let stream = PushStream::new(certificates, response_stream.map(Ok));

        let first = stream.expect(chain(1), BlockHeight(7));
        let mut second = stream.expect(chain(2), BlockHeight(7));

        responses
            .start_send(answer(chain(1), 7))
            .expect("the channel is open");
        // Only chain 1 was answered, so chain 2's caller must still be waiting rather than woken
        // with someone else's outcome.
        assert!(first.await.is_ok(), "chain 1 must be woken by its answer");
        assert!(
            second.try_recv().is_err(),
            "chain 2 must not be woken by chain 1's answer",
        );
    }

    /// A run is written whole and awaited once, so the answer to its last certificate is what
    /// resolves it — and it must also satisfy anything queued below, or a caller whose
    /// certificates the destination had already applied waits for an answer never coming.
    #[test_log::test(tokio::test)]
    async fn an_answer_satisfies_every_waiter_at_or_below_it() {
        let (certificates, _queue) = mpsc::channel(4);
        let (mut responses, response_stream) = futures_mpsc::unbounded();
        let stream = PushStream::new(certificates, response_stream.map(Ok));

        let below = stream.expect(chain(1), BlockHeight(3));
        let at = stream.expect(chain(1), BlockHeight(9));
        let mut above = stream.expect(chain(1), BlockHeight(11));

        responses
            .start_send(answer(chain(1), 9))
            .expect("the channel is open");
        assert!(
            below.await.is_ok(),
            "a waiter below the answer is satisfied"
        );
        assert!(at.await.is_ok(), "the waiter at the answer is satisfied");
        assert!(
            above.try_recv().is_err(),
            "a waiter above the answer is still outstanding",
        );
    }

    /// A destination that stops answering must not leave callers hanging: they are the export
    /// queue's jobs, and a job that never completes holds its in-flight slot forever.
    #[test_log::test(tokio::test)]
    async fn closing_the_stream_wakes_everyone_left() {
        let (certificates, _queue) = mpsc::channel(4);
        let (responses, response_stream) = futures_mpsc::unbounded();
        let stream = PushStream::new(certificates, response_stream.map(Ok));

        let waiting = stream.expect(chain(1), BlockHeight(1));
        drop(responses);

        assert!(
            matches!(waiting.await, Ok(Err(NodeError::PushStreamClosed))),
            "a caller must learn the stream died rather than wait forever",
        );
    }
}
