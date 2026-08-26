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
//! [`ProposalContent`]: linera_chain::data_types::ProposalContent
//! [`ValidatedBlockCertificate`]: linera_chain::types::ValidatedBlockCertificate

#[cfg(not(web))]
use futures::future::BoxFuture;
#[cfg(web)]
use futures::future::LocalBoxFuture as BoxFuture;
use linera_base::{
    data_types::Blob,
    task::{MaybeSend, MaybeSync},
};
use linera_chain::{
    data_types::BlockProposal,
    types::{Block, ConfirmedBlockCertificate},
};

use crate::{
    data_types::ChainInfo,
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
pub trait ConfirmationDelegate: std::fmt::Debug + MaybeSend + MaybeSync {
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
}
