// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Delegated formation of confirmation certificates.
//!
//! A client normally forms a confirmation certificate itself: it fans its signed proposal out
//! to every validator, aggregates the votes, and pushes the result back out so that the
//! block's cross-chain messages are delivered. For a client far from the validators each of
//! those steps costs a wide-area round trip, and so does every repair the fan-out needs on the
//! way -- uploading a blob a validator is missing, or bringing a lagging validator up to the
//! proposal's height.
//!
//! A delegate does that work on the client's behalf, close to the validators. It holds no
//! signing authority of its own: the client signs the proposal, the validators sign the votes,
//! and everything the delegate returns is a quorum-signed artifact that the client checks
//! against its own committee. A faulty delegate can withhold or delay, but it cannot confirm a
//! block that the owner did not sign or that the validators did not accept.
//!
//! The division of labour follows from how far the owner's signature reaches. The round is part
//! of [`ProposalContent`], so only the owner can open one. Finalization carries a quorum-signed
//! [`ValidatedBlockCertificate`] and no owner signature at all, so anyone can close one:
//!
//! > A delegate can always finish a round. It can never start one.
//!
//! Within the round its proposal was signed for, the delegate runs to completion -- collecting
//! votes, finalizing a validated certificate, and delivering the resulting cross-chain messages
//! -- discharging from its own storage whatever the validators turn out to be missing. As soon
//! as it meets something that needs a fresh owner signature it stops, hands back the chain state
//! it observed, and leaves the next signature to the owner.
//!
//! A delegate joins a round wherever someone else has already supplied the authority to be there,
//! so there are two ways in. [`ProposerDelegate::submit_and_confirm`] enters on the owner's
//! signature over a proposal. [`ProposerDelegate::finalize`] enters on a quorum's signatures over
//! a validated block, which is what a caller resuming an interrupted attempt has to hand.
//!
//! [`ProposalContent`]: linera_chain::data_types::ProposalContent
//! [`ValidatedBlockCertificate`]: linera_chain::types::ValidatedBlockCertificate

use std::sync::Arc;

#[cfg(not(web))]
use futures::future::BoxFuture;
#[cfg(web)]
use futures::future::LocalBoxFuture as BoxFuture;
use linera_base::{
    data_types::Blob,
    identifiers::ChainId,
    task::{MaybeSend, MaybeSync},
};
use linera_chain::{
    data_types::BlockProposal,
    types::{
        Block, ConfirmedBlock, ConfirmedBlockCertificate, ValidatedBlock, ValidatedBlockCertificate,
    },
};
use linera_execution::committee::Committee;
use linera_storage::Storage as _;
use tracing::{debug, warn};

use crate::{
    client::{chain_client, Client, ListeningMode},
    data_types::ChainInfo,
    environment::Environment,
    local_node::LocalNodeError,
    node::{CrossChainMessageDelivery, NodeError},
};

/// What a delegate made of a proposal.
#[derive(Debug)]
pub enum DelegatedOutcome {
    /// The delegate carried the proposal through the rest of its round.
    ///
    /// The certificate is quorum-signed, so the caller validates it against its own committee
    /// instead of trusting the delegate that delivered it.
    Confirmed(Box<ConfirmedBlockCertificate>),

    /// The delegate stopped at something only the owner can resolve -- the round moved on, a
    /// different block locked or committed at this height, or a blob it does not hold -- and
    /// reports the chain state it observed so that the owner can decide what to sign next.
    ///
    /// Only the certificate-bearing fields of the [`ChainInfo`] carry their own proof:
    /// `manager.timeout` and `manager.requested_locking`. The rest are hints from an
    /// unauthenticated aggregator, so a caller should take the round it proposes in next from
    /// the certificates it can check rather than from `manager.current_round`.
    NeedsOwner(Box<ChainInfo>),
}

/// A node that forms confirmation certificates on a client's behalf.
///
/// The trait is deliberately narrow: one call carries one signed proposal as far as it can go
/// without another owner signature. It is object-safe so that a delegate can be plugged into a
/// [`Client`](crate::Client) without every [`Environment`](crate::environment::Environment)
/// having to name a delegate type.
pub trait ProposerDelegate: std::fmt::Debug + MaybeSend + MaybeSync {
    /// Returns the address of this delegate.
    fn address(&self) -> String;

    /// Carries a signed proposal through the rest of the round it was signed for.
    ///
    /// `block` is the proposed block paired with the execution outcome the caller has already
    /// computed, so that the delegate can build the value that a certificate needs without
    /// executing the block itself. Supplying the wrong outcome gains a caller nothing: the
    /// resulting value hash will not match the votes, and the attempt fails.
    ///
    /// `blobs` are the blobs the block publishes. The delegate may have to upload them to
    /// validators that have not seen them, and for a newly published blob it has no other
    /// source.
    ///
    /// Returning [`DelegatedOutcome::NeedsOwner`] is an ordinary outcome rather than a failure.
    /// An `Err` means the delegate itself could not be reached or misbehaved, and the caller
    /// should fall back to forming the certificate itself.
    fn submit_and_confirm<'a>(
        &'a self,
        proposal: BlockProposal,
        block: Block,
        blobs: Vec<Blob>,
        delivery: CrossChainMessageDelivery,
    ) -> BoxFuture<'a, Result<DelegatedOutcome, NodeError>>;

    /// Carries an already validated block to a confirmation certificate.
    ///
    /// This is the same work as the tail of [`submit_and_confirm`](Self::submit_and_confirm), for
    /// a caller that holds a lock rather than a fresh proposal -- one resuming an attempt that
    /// was interrupted, or that found the lock on the validators. Finalization carries no owner
    /// signature at all: the certificate authorizes itself, so a delegate accepts one whatever
    /// the caller's relation to the chain.
    fn finalize<'a>(
        &'a self,
        certificate: ValidatedBlockCertificate,
        delivery: CrossChainMessageDelivery,
    ) -> BoxFuture<'a, Result<DelegatedOutcome, NodeError>>;
}

#[cfg(test)]
#[path = "unit_tests/delegate_tests.rs"]
mod delegate_tests;

/// Reports a failure of the delegate's own machinery, as opposed to one of the chain's rounds.
fn internal_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::WorkerError {
        error: error.to_string(),
    }
}

/// A [`ProposerDelegate`] that does the work in this process, against a local [`Client`].
///
/// This is the delegate proper, not a handle to a remote one: a node that offers delegation over
/// the network serves it from one of these, and a client reaching that node holds an
/// implementation that forwards to it instead.
///
/// The [`Client`] it drives needs no key for the chains it serves, since nothing the delegate
/// does is signed by an owner. It does need to be close to the validators and to hold, or be able
/// to fetch, the blocks and blobs they turn out to be missing, because that is the work being
/// moved off the caller.
pub struct LocalProposerDelegate<Env: Environment> {
    client: Arc<Client<Env>>,
    address: String,
}

impl<Env: Environment> std::fmt::Debug for LocalProposerDelegate<Env> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalProposerDelegate")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl<Env: Environment> LocalProposerDelegate<Env> {
    /// Creates a delegate that serves proposals from the given client, reporting `address` as
    /// its own.
    pub fn new(client: Arc<Client<Env>>, address: impl Into<String>) -> Self {
        Self {
            client,
            address: address.into(),
        }
    }

    /// Returns the committee of the chain's current epoch, as our local node sees it.
    async fn committee(&self, chain_id: ChainId) -> Result<Arc<Committee>, chain_client::Error> {
        let info = self.client.local_node.chain_info(chain_id).await?;
        let hash = info
            .committee_hash
            .ok_or(LocalNodeError::InactiveChain(chain_id))?;
        Ok(self
            .client
            .storage_client()
            .get_or_load_committee_by_hash(hash)
            .await
            .map_err(LocalNodeError::from)?)
    }

    /// Starts following the chain and brings our view of it level with the validators.
    ///
    /// We serve chains we were never configured for, so the mode is extended on demand. The
    /// chain has to be followed in full: repairing a validator that is behind can mean pushing
    /// blocks from the chains that sent it messages, not just from this one.
    async fn prepare(&self, chain_id: ChainId) -> Result<(), chain_client::Error> {
        self.client
            .extend_chain_mode(chain_id, ListeningMode::FullChain);
        self.client.synchronize_chain_state(chain_id).await?;
        Ok(())
    }

    /// Reports the chain state we can see, for a caller that has to decide what to sign next.
    ///
    /// We resynchronize first, so that a timeout certificate or a locking block that appeared
    /// while we were working is in what we hand back: those carry their own proof, and are the
    /// part of this the caller can actually use.
    async fn hand_back(&self, chain_id: ChainId) -> Result<DelegatedOutcome, NodeError> {
        let info = match self.client.synchronize_chain_state(chain_id).await {
            Ok(info) => info,
            Err(error) => {
                debug!(%chain_id, %error, "Could not resynchronize before handing the block back");
                self.client
                    .local_node
                    .chain_info(chain_id)
                    .await
                    .map_err(internal_error)?
            }
        };
        Ok(DelegatedOutcome::NeedsOwner(info))
    }

    /// Makes the validators aware of a block we just got confirmed.
    ///
    /// A delegate that formed a certificate and left the validators ignorant of it would have
    /// done half the job, so this always runs; `delivery` only decides whether we also wait for
    /// the block's outgoing messages to reach their inboxes. A caller that broadcasts for itself
    /// as well loses nothing, since the operation is idempotent.
    async fn broadcast(
        &self,
        chain_id: ChainId,
        committee: &Committee,
        certificate: &ConfirmedBlockCertificate,
        delivery: CrossChainMessageDelivery,
    ) {
        let height = certificate.block().header.height;
        let cached = self
            .client
            .storage_client()
            .cache_certificate(certificate.clone());
        if let Err(error) = self
            .client
            .communicate_chain_updates(committee, chain_id, height, delivery, Some(cached))
            .await
        {
            // The caller's certificate is good regardless, and the validators that missed this
            // will catch up through the usual synchronization, so this is not worth failing over.
            warn!(%chain_id, %height, %error, "Could not broadcast the confirmed block");
        }
    }

    /// Carries a signed proposal through the rest of its round. See
    /// [`ProposerDelegate::submit_and_confirm`].
    async fn run_proposal(
        &self,
        proposal: BlockProposal,
        block: Block,
        blobs: Vec<Blob>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<DelegatedOutcome, NodeError> {
        let chain_id = proposal.content.block.chain_id;
        let round = proposal.content.round;
        self.prepare(chain_id).await.map_err(internal_error)?;
        // Hold the proposal ourselves before offering it around. That is what lets us answer a
        // validator that reports the blobs missing, since they are then ours to send.
        self.client
            .local_node
            .handle_pending_blobs(chain_id, blobs)
            .await
            .map_err(internal_error)?;
        if let Err(error) = self
            .client
            .local_node
            .handle_block_proposal(proposal.clone())
            .await
        {
            debug!(%chain_id, %round, %error, "Rejected the proposal locally");
            return self.hand_back(chain_id).await;
        }
        let committee = match self.committee(chain_id).await {
            Ok(committee) => committee,
            Err(error) => return Err(internal_error(error)),
        };
        let proposal = Box::new(proposal);
        let certificate = if round.is_fast() {
            self.client
                .submit_block_proposal(committee.clone(), proposal, ConfirmedBlock::new(block))
                .await
        } else {
            // Nothing in finalization is signed by an owner, so we close the round ourselves
            // rather than handing the validated certificate back for a second round trip.
            match self
                .client
                .submit_block_proposal(committee.clone(), proposal, ValidatedBlock::new(block))
                .await
            {
                Ok(validated) => self.client.finalize_block(&committee, validated).await,
                Err(error) => Err(error),
            }
        };
        match certificate {
            Ok(certificate) => {
                self.broadcast(chain_id, &committee, &certificate, delivery)
                    .await;
                Ok(DelegatedOutcome::Confirmed(Box::new(certificate)))
            }
            Err(error) => {
                debug!(%chain_id, %round, %error, "Could not carry the proposal to a certificate");
                self.hand_back(chain_id).await
            }
        }
    }

    /// Carries an already validated block to a confirmation certificate. See
    /// [`ProposerDelegate::finalize`].
    async fn run_finalization(
        &self,
        validated: ValidatedBlockCertificate,
        delivery: CrossChainMessageDelivery,
    ) -> Result<DelegatedOutcome, NodeError> {
        let chain_id = validated.block().header.chain_id;
        self.prepare(chain_id).await.map_err(internal_error)?;
        let committee = match self.committee(chain_id).await {
            Ok(committee) => committee,
            Err(error) => return Err(internal_error(error)),
        };
        match self.client.finalize_block(&committee, validated).await {
            Ok(certificate) => {
                self.broadcast(chain_id, &committee, &certificate, delivery)
                    .await;
                Ok(DelegatedOutcome::Confirmed(Box::new(certificate)))
            }
            Err(error) => {
                debug!(%chain_id, %error, "Could not finalize the validated block");
                self.hand_back(chain_id).await
            }
        }
    }
}

impl<Env: Environment> ProposerDelegate for LocalProposerDelegate<Env> {
    fn address(&self) -> String {
        self.address.clone()
    }

    fn submit_and_confirm<'a>(
        &'a self,
        proposal: BlockProposal,
        block: Block,
        blobs: Vec<Blob>,
        delivery: CrossChainMessageDelivery,
    ) -> BoxFuture<'a, Result<DelegatedOutcome, NodeError>> {
        Box::pin(self.run_proposal(proposal, block, blobs, delivery))
    }

    fn finalize<'a>(
        &'a self,
        certificate: ValidatedBlockCertificate,
        delivery: CrossChainMessageDelivery,
    ) -> BoxFuture<'a, Result<DelegatedOutcome, NodeError>> {
        Box::pin(self.run_finalization(certificate, delivery))
    }
}
