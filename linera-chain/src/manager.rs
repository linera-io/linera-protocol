// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! # Chain manager
//!
//! This module contains the consensus mechanism for all microchains. Whenever a block is
//! confirmed, a new chain manager is created for the next block height. It manages the consensus
//! state until a new block is confirmed. As long as less than a third of the validators are faulty,
//! it guarantees that at most one `ConfirmedBlock` certificate will be created for this height.
//!
//! The protocol proceeds in rounds, until it reaches a round where a block gets confirmed.
//!
//! There are four kinds of rounds:
//!
//! * In `Round::Fast`, only super owners can propose blocks, and validators vote to confirm a
//!   block immediately. Super owners must be careful to make only one block proposal, or else they
//!   can permanently block the microchain. If there are no super owners, `Round::Fast` is skipped.
//! * In cooperative mode (`Round::MultiLeader`), all chain owners can propose blocks at any time.
//!   The protocol is guaranteed to eventually confirm a block as long as no chain owner
//!   continuously actively prevents progress.
//! * In leader rotation mode (`Round::SingleLeader`), chain owners take turns at proposing blocks.
//!   It can make progress as long as at least one owner is honest, even if other owners try to
//!   prevent it.
//! * In fallback/public mode (`Round::Validator`), validators take turns at proposing blocks.
//!   It can always make progress under the standard assumption that there is a quorum of honest
//!   validators.
//!
//! ## Safety, i.e. at most one block will be confirmed
//!
//! In all modes this is guaranteed as follows:
//!
//! * Validators (honest ones) never cast a vote if they have already cast any vote in a later
//!   round.
//! * Validators never vote for a `ValidatedBlock` **A** in round **r** if they have voted for a
//!   _different_ `ConfirmedBlock` **B** in an earlier round **s** ≤ **r**, unless there is a
//!   `ValidatedBlock` certificate (with a quorum of validator signatures) for **A** in some round
//!   between **s** and **r** included in the block proposal.
//! * Validators only vote for a `ConfirmedBlock` if there is a `ValidatedBlock` certificate for the
//!   same block in the same round.
//!
//! This guarantees that once a quorum votes for some `ConfirmedBlock`, there can never be a
//! `ValidatedBlock` certificate (and thus also no `ConfirmedBlock` certificate) for a different
//! block in a later round. So if there are two different `ConfirmedBlock` certificates, they may
//! be from different rounds, but they are guaranteed to contain the same block.
//!
//! ## Liveness, i.e. some block will eventually be confirmed
//!
//! In `Round::Fast`, liveness depends on the super owners coordinating, and proposing at most one
//! block.
//!
//! If they propose none, and there are other owners, `Round::Fast` will eventually time out.
//!
//! In cooperative mode, if there is contention, the owners need to agree on a single owner as the
//! next proposer. That owner should then download all highest-round certificates and block
//! proposals known to the honest validators. They can then make a proposal in a round higher than
//! all previous proposals. If there is any `ValidatedBlock` certificate they must include the
//! highest one in their proposal, and propose that block. Otherwise they can propose a new block.
//! Now all honest validators are allowed to vote for that proposal, and eventually confirm it.
//!
//! If the owners fail to cooperate, any honest owner can initiate the last multi-leader round by
//! making a proposal there, then wait for it to time out, which starts the leader-based mode:
//!
//! In leader-based and fallback/public mode, an honest participant should subscribe to
//! notifications from all validators, and follow the chain. Whenever another leader's round takes
//! too long, they should request timeout votes from the validators to make the next round begin.
//! Once the honest participant becomes the round leader, they should update all validators, so
//! that they all agree on the current round. Then they download the highest `ValidatedBlock`
//! certificate known to any honest validator and include that in their block proposal, just like
//! in the cooperative case.

use std::collections::BTreeMap;

use custom_debug_derive::Debug;
use futures::future::Either;
use linera_base::{
    crypto::{KeyPair, PublicKey},
    data_types::{ArithmeticError, Blob, BlockHeight, Round, Timestamp},
    doc_scalar, ensure,
    hashed::Hashed,
    identifiers::{BlobId, ChainId, Owner},
    ownership::ChainOwnership,
};
use linera_execution::committee::Epoch;
use rand_chacha::{rand_core::SeedableRng, ChaCha8Rng};
use rand_distr::{Distribution, WeightedAliasIndex};
use serde::{Deserialize, Serialize};

use crate::{
    block::{ConfirmedBlock, Timeout, ValidatedBlock},
    data_types::{BlockProposal, ExecutedBlock, LiteVote, Proposal, Vote},
    types::{TimeoutCertificate, ValidatedBlockCertificate},
    ChainError,
};

/// The result of verifying a (valid) query.
#[derive(Eq, PartialEq)]
pub enum Outcome {
    Accept,
    Skip,
}

/// The state of the certification process for a chain's next block.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct ChainManager {
    /// The public keys, weights and types of the chain's owners.
    pub ownership: ChainOwnership,
    /// The seed for the pseudo-random number generator that determines the round leaders.
    pub seed: u64,
    /// The probability distribution for choosing a round leader.
    #[debug(skip_if = Option::is_none)]
    pub distribution: Option<WeightedAliasIndex<u64>>,
    /// The probability distribution for choosing a fallback round leader.
    #[debug(skip_if = Option::is_none)]
    pub fallback_distribution: Option<WeightedAliasIndex<u64>>,
    /// Highest-round authenticated block that we have received and checked. If there are multiple
    /// proposals in the same round, this contains only the first one.
    #[debug(skip_if = Option::is_none)]
    pub proposed: Option<BlockProposal>,
    /// Latest validated proposal that we have voted to confirm (or would have, if we are not a
    /// validator).
    #[debug(skip_if = Option::is_none)]
    pub locked: Option<ValidatedBlockCertificate>,
    /// These are blobs published or read by the locked block.
    #[debug(skip_if = BTreeMap::is_empty)]
    pub locked_blobs: BTreeMap<BlobId, Blob>,
    /// Latest leader timeout certificate we have received.
    #[debug(skip_if = Option::is_none)]
    pub timeout: Option<TimeoutCertificate>,
    /// Latest vote we cast to confirm a block.
    #[debug(skip_if = Option::is_none)]
    pub confirmed_vote: Option<Vote<ConfirmedBlock>>,
    /// Latest vote we cast to validate a block.
    #[debug(skip_if = Option::is_none)]
    pub validated_vote: Option<Vote<ValidatedBlock>>,
    /// Latest timeout vote we cast.
    #[debug(skip_if = Option::is_none)]
    pub timeout_vote: Option<Vote<Timeout>>,
    /// Fallback vote we cast.
    #[debug(skip_if = Option::is_none)]
    pub fallback_vote: Option<Vote<Timeout>>,
    /// The time after which we are ready to sign a timeout certificate for the current round.
    #[debug(skip_if = Option::is_none)]
    pub round_timeout: Option<Timestamp>,
    /// The lowest round where we can still vote to validate or confirm a block. This is
    /// the round to which the timeout applies.
    ///
    /// Having a leader timeout certificate in any given round causes the next one to become
    /// current. Seeing a validated block certificate or a valid proposal in any round causes that
    /// round to become current, unless a higher one already is.
    pub current_round: Round,
    /// The owners that take over in fallback mode.
    #[debug(skip_if = BTreeMap::is_empty)]
    pub fallback_owners: BTreeMap<Owner, (PublicKey, u64)>,
}

doc_scalar!(
    ChainManager,
    "The state of the certification process for a chain's next block"
);

impl ChainManager {
    /// Replaces `self` with a new chain manager.
    pub fn reset<'a>(
        &mut self,
        ownership: &ChainOwnership,
        height: BlockHeight,
        local_time: Timestamp,
        fallback_owners: impl Iterator<Item = (PublicKey, u64)> + 'a,
    ) -> Result<(), ChainError> {
        *self = ChainManager::new(ownership.clone(), height.0, local_time, fallback_owners)?;
        Ok(())
    }

    /// Creates a new `ChainManager`, and starts the first round.
    fn new<'a>(
        ownership: ChainOwnership,
        seed: u64,
        local_time: Timestamp,
        fallback_owners: impl Iterator<Item = (PublicKey, u64)> + 'a,
    ) -> Result<Self, ChainError> {
        let distribution = if !ownership.owners.is_empty() {
            let weights = ownership
                .owners
                .values()
                .map(|(_, weight)| *weight)
                .collect();
            Some(WeightedAliasIndex::new(weights)?)
        } else {
            None
        };
        let fallback_owners = fallback_owners
            .map(|(pub_key, weight)| (Owner::from(pub_key), (pub_key, weight)))
            .collect::<BTreeMap<_, _>>();
        let fallback_distribution = if !fallback_owners.is_empty() {
            let weights = fallback_owners
                .values()
                .map(|(_, weight)| *weight)
                .collect();
            Some(WeightedAliasIndex::new(weights)?)
        } else {
            None
        };

        let current_round = ownership.first_round();
        let round_duration = ownership.round_timeout(current_round);
        let round_timeout = round_duration.map(|rd| local_time.saturating_add(rd));

        Ok(ChainManager {
            ownership,
            seed,
            distribution,
            fallback_distribution,
            proposed: None,
            locked: None,
            timeout: None,
            confirmed_vote: None,
            validated_vote: None,
            timeout_vote: None,
            fallback_vote: None,
            round_timeout,
            current_round,
            fallback_owners,
            locked_blobs: BTreeMap::new(),
        })
    }

    /// Returns the most recent confirmed vote we cast.
    pub fn confirmed_vote(&self) -> Option<&Vote<ConfirmedBlock>> {
        self.confirmed_vote.as_ref()
    }

    /// Returns the most recent validated vote we cast.
    pub fn validated_vote(&self) -> Option<&Vote<ValidatedBlock>> {
        self.validated_vote.as_ref()
    }

    /// Returns the most recent timeout vote we cast.
    pub fn timeout_vote(&self) -> Option<&Vote<Timeout>> {
        self.timeout_vote.as_ref()
    }

    /// Returns the most recent fallback vote we cast.
    pub fn fallback_vote(&self) -> Option<&Vote<Timeout>> {
        self.fallback_vote.as_ref()
    }

    /// Verifies the safety of a proposed block with respect to voting rules.
    pub fn check_proposed_block(&self, proposal: &BlockProposal) -> Result<Outcome, ChainError> {
        let new_round = proposal.content.round;
        let new_block = &proposal.content.proposal;
        let owner = &proposal.owner;

        // When a block is certified, incrementing its height must succeed.
        ensure!(
            new_block.height < BlockHeight::MAX,
            ChainError::InvalidBlockHeight
        );
        let expected_round = match &proposal.validated_block_certificate {
            None => self.current_round,
            Some(cert) => self
                .ownership
                .next_round(cert.round)
                .ok_or_else(|| ChainError::ArithmeticError(ArithmeticError::Overflow))?
                .max(self.current_round),
        };
        // In leader rotation mode, the round must equal the expected one exactly.
        // Only the first single-leader round can be entered at any time.
        if self.is_super(owner)
            || (new_round <= Round::SingleLeader(0) && !expected_round.is_fast())
        {
            ensure!(
                expected_round <= new_round,
                ChainError::InsufficientRound(expected_round)
            );
        } else {
            ensure!(
                expected_round == new_round,
                ChainError::WrongRound(expected_round)
            );
        }
        if let Some(old_proposal) = &self.proposed {
            if old_proposal.content == proposal.content {
                return Ok(Outcome::Skip); // We already voted for this proposal; nothing to do.
            }
            ensure!(
                new_round > old_proposal.content.round,
                // We already accepted a proposal in this round or in a higher round.
                ChainError::InsufficientRoundStrict(old_proposal.content.round)
            );
            // Any proposal in the fast round is considered locked, because validators vote to
            // confirm it immediately.
            if old_proposal.content.round.is_fast()
                && proposal.validated_block_certificate.is_none()
            {
                ensure!(
                    old_proposal.content.proposal == *new_block,
                    ChainError::HasLockedBlock(new_block.height, Round::Fast)
                )
            }
        }
        // If we have a locked block, the proposal must contain a certificate that validates the
        // proposed block and is no older than the locked block.
        if let Some(locked) = &self.locked {
            ensure!(
                proposal
                    .validated_block_certificate
                    .as_ref()
                    .is_some_and(|cert| locked.round <= cert.round),
                ChainError::HasLockedBlock(locked.block().header.height, locked.round)
            )
        }
        Ok(Outcome::Accept)
    }

    /// Checks if the current round has timed out, and signs a `Timeout`.
    pub fn vote_timeout(
        &mut self,
        chain_id: ChainId,
        height: BlockHeight,
        epoch: Epoch,
        key_pair: Option<&KeyPair>,
        local_time: Timestamp,
    ) -> bool {
        let Some(key_pair) = key_pair else {
            return false; // We are not a validator.
        };
        let Some(round_timeout) = self.round_timeout else {
            return false; // The current round does not time out.
        };
        if local_time < round_timeout || self.ownership.owners.is_empty() {
            return false; // Round has not timed out yet, or there are no regular owners.
        }
        let current_round = self.current_round;
        if let Some(vote) = &self.timeout_vote {
            if vote.round == current_round {
                return false; // We already signed this timeout.
            }
        }
        let value = Hashed::new(Timeout::new(chain_id, height, epoch));
        self.timeout_vote = Some(Vote::new(value, current_round, key_pair));
        true
    }

    /// Signs a `Timeout` certificate to switch to fallback mode.
    ///
    /// This must only be called after verifying that the condition for fallback mode is
    /// satisfied locally.
    pub fn vote_fallback(
        &mut self,
        chain_id: ChainId,
        height: BlockHeight,
        epoch: Epoch,
        key_pair: Option<&KeyPair>,
    ) -> bool {
        let Some(key_pair) = key_pair else {
            return false; // We are not a validator.
        };
        if self.fallback_vote.is_some() || self.current_round >= Round::Validator(0) {
            return false; // We already signed this or are already in fallback mode.
        }
        let value = Hashed::new(Timeout::new(chain_id, height, epoch));
        let last_regular_round = Round::SingleLeader(u32::MAX);
        self.fallback_vote = Some(Vote::new(value, last_regular_round, key_pair));
        true
    }

    /// Verifies that we can vote to confirm a validated block.
    pub fn check_validated_block(
        &self,
        certificate: &ValidatedBlockCertificate,
    ) -> Result<Outcome, ChainError> {
        let new_block = certificate.block();
        let new_round = certificate.round;
        if let Some(Vote { value, round, .. }) = &self.confirmed_vote {
            if value.inner().block() == new_block && *round == new_round {
                return Ok(Outcome::Skip); // We already voted to confirm this block.
            }
        }

        if let Some(Vote { round, .. }) = &self.validated_vote {
            ensure!(new_round >= *round, ChainError::InsufficientRound(*round))
        }

        // We don't compare to `current_round` here: Non-validators must update their locked block
        // even if it is older than the current round. Validators will only sign in the current
        // round, though. (See `create_final_vote` below.)
        if let Some(locked) = &self.locked {
            if locked.hash() == certificate.hash() && locked.round == certificate.round {
                return Ok(Outcome::Skip);
            }
            ensure!(
                new_round > locked.round,
                ChainError::InsufficientRoundStrict(locked.round)
            );
        }
        Ok(Outcome::Accept)
    }

    /// Signs a vote to validate the proposed block.
    pub fn create_vote(
        &mut self,
        proposal: BlockProposal,
        executed_block: ExecutedBlock,
        key_pair: Option<&KeyPair>,
        local_time: Timestamp,
        blobs: BTreeMap<BlobId, Blob>,
    ) -> Option<Either<&Vote<ValidatedBlock>, &Vote<ConfirmedBlock>>> {
        let round = proposal.content.round;

        // If the validated block certificate is more recent, update our locked block.
        if let Some(lite_cert) = &proposal.validated_block_certificate {
            if self
                .locked
                .as_ref()
                .map_or(true, |locked| locked.round < lite_cert.round)
            {
                let value = Hashed::new(ValidatedBlock::new(executed_block.clone()));
                if let Some(certificate) = lite_cert.clone().with_value(value) {
                    self.locked = Some(certificate);
                    self.locked_blobs = blobs;
                }
            }
        }

        // Record the proposed block, so it can be supplied to clients that request it.
        self.proposed = Some(proposal);
        self.update_current_round(local_time);

        if let Some(key_pair) = key_pair {
            // If this is a fast block, vote to confirm. Otherwise vote to validate.
            if round.is_fast() {
                self.validated_vote = None;
                Some(Either::Right(self.confirmed_vote.insert(Vote::new(
                    Hashed::new(ConfirmedBlock::new(executed_block)),
                    round,
                    key_pair,
                ))))
            } else {
                self.confirmed_vote = None;
                Some(Either::Left(&*self.validated_vote.insert(Vote::new(
                    Hashed::new(ValidatedBlock::new(executed_block)),
                    round,
                    key_pair,
                ))))
            }
        } else {
            None
        }
    }

    /// Signs a vote to confirm the validated block.
    pub fn create_final_vote(
        &mut self,
        validated: ValidatedBlockCertificate,
        key_pair: Option<&KeyPair>,
        local_time: Timestamp,
        blobs: BTreeMap<BlobId, Blob>,
    ) {
        let round = validated.round;
        // Validators only change their locked block if the new one is included in a proposal in the
        // current round, or it is itself in the current round.
        if key_pair.is_some() && round < self.current_round {
            return;
        }
        let confirmed_block = ConfirmedBlock::new(validated.inner().block().clone().into());
        self.locked = Some(validated);
        self.locked_blobs = blobs;
        self.update_current_round(local_time);
        if let Some(key_pair) = key_pair {
            // Vote to confirm.
            let vote = Vote::new(Hashed::new(confirmed_block), round, key_pair);
            // Ok to overwrite validation votes with confirmation votes at equal or higher round.
            self.confirmed_vote = Some(vote);
            self.validated_vote = None;
        }
    }

    /// Updates `current_round` and `round_timeout` if necessary.
    ///
    /// This must be after every change to `timeout`, `locked` or `proposed`.
    fn update_current_round(&mut self, local_time: Timestamp) {
        let current_round = self
            .timeout
            .iter()
            .map(|certificate| {
                self.ownership
                    .next_round(certificate.round)
                    .unwrap_or(Round::Validator(u32::MAX))
            })
            .chain(self.locked.iter().map(|certificate| certificate.round))
            .chain(self.proposed.iter().map(|proposal| proposal.content.round))
            .max()
            .unwrap_or_default()
            .max(self.ownership.first_round());
        if current_round <= self.current_round {
            return;
        }
        let round_duration = self.ownership.round_timeout(current_round);
        self.round_timeout = round_duration.map(|rd| local_time.saturating_add(rd));
        self.current_round = current_round;
    }

    /// Updates the round number and timer if the timeout certificate is from a higher round than
    /// any known certificate.
    pub fn handle_timeout_certificate(
        &mut self,
        certificate: TimeoutCertificate,
        local_time: Timestamp,
    ) {
        let round = certificate.round;
        if let Some(known_certificate) = &self.timeout {
            if known_certificate.round >= round {
                return;
            }
        }
        self.timeout = Some(certificate);
        self.update_current_round(local_time);
    }

    /// Returns the public key of the block proposal's signer, if they are a valid owner and allowed
    /// to propose a block in the proposal's round.
    pub fn verify_owner(&self, proposal: &BlockProposal) -> Option<PublicKey> {
        if let Some(public_key) = self.ownership.super_owners.get(&proposal.owner) {
            return Some(*public_key);
        }
        match proposal.content.round {
            Round::Fast => {
                None // Only super owners can propose in the first round.
            }
            Round::MultiLeader(_) => {
                // Not in leader rotation mode; any owner is allowed to propose.
                self.ownership
                    .owners
                    .get(&proposal.owner)
                    .map(|(public_key, _)| *public_key)
            }
            Round::SingleLeader(r) => {
                let index = self.round_leader_index(r)?;
                let (leader, (public_key, _)) = self.ownership.owners.iter().nth(index)?;
                (*leader == proposal.owner).then_some(*public_key)
            }
            Round::Validator(r) => {
                let index = self.fallback_round_leader_index(r)?;
                let (leader, (public_key, _)) = self.fallback_owners.iter().nth(index)?;
                (*leader == proposal.owner).then_some(*public_key)
            }
        }
    }

    /// Returns the leader who is allowed to propose a block in the given round, or `None` if every
    /// owner is allowed to propose. Exception: In `Round::Fast`, only super owners can propose.
    fn round_leader(&self, round: Round) -> Option<&Owner> {
        match round {
            Round::SingleLeader(r) => {
                let index = self.round_leader_index(r)?;
                self.ownership.owners.keys().nth(index)
            }
            Round::Validator(r) => {
                let index = self.fallback_round_leader_index(r)?;
                self.fallback_owners.keys().nth(index)
            }
            Round::Fast | Round::MultiLeader(_) => None,
        }
    }

    /// Returns the index of the leader who is allowed to propose a block in the given round.
    fn round_leader_index(&self, round: u32) -> Option<usize> {
        let seed = u64::from(round).rotate_left(32).wrapping_add(self.seed);
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        Some(self.distribution.as_ref()?.sample(&mut rng))
    }

    /// Returns the index of the fallback leader who is allowed to propose a block in the given
    /// round.
    fn fallback_round_leader_index(&self, round: u32) -> Option<usize> {
        let seed = u64::from(round).rotate_left(32).wrapping_add(self.seed);
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        Some(self.fallback_distribution.as_ref()?.sample(&mut rng))
    }

    /// Returns whether the owner is a super owner.
    fn is_super(&self, owner: &Owner) -> bool {
        self.ownership.super_owners.contains_key(owner)
    }
}

/// Chain manager information that is included in `ChainInfo` sent to clients.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub struct ChainManagerInfo {
    /// The configuration of the chain's owners.
    pub ownership: ChainOwnership,
    /// Latest authenticated block that we have received, if requested.
    #[debug(skip_if = Option::is_none)]
    pub requested_proposed: Option<Box<BlockProposal>>,
    /// Latest validated proposal that we have voted to confirm (or would have, if we are not a
    /// validator).
    #[debug(skip_if = Option::is_none)]
    pub requested_locked: Option<Box<ValidatedBlockCertificate>>,
    /// Latest timeout certificate we have seen.
    #[debug(skip_if = Option::is_none)]
    pub timeout: Option<Box<TimeoutCertificate>>,
    /// Latest vote we cast (either to validate or to confirm a block).
    #[debug(skip_if = Option::is_none)]
    pub pending: Option<LiteVote>,
    /// Latest timeout vote we cast.
    #[debug(skip_if = Option::is_none)]
    pub timeout_vote: Option<LiteVote>,
    /// Fallback vote we cast.
    #[debug(skip_if = Option::is_none)]
    pub fallback_vote: Option<LiteVote>,
    /// The value we voted for, if requested.
    #[debug(skip_if = Option::is_none)]
    pub requested_confirmed: Option<Box<Hashed<ConfirmedBlock>>>,
    /// The value we voted for, if requested.
    #[debug(skip_if = Option::is_none)]
    pub requested_validated: Option<Box<Hashed<ValidatedBlock>>>,
    /// The current round, i.e. the lowest round where we can still vote to validate a block.
    pub current_round: Round,
    /// The current leader, who is allowed to propose the next block.
    /// `None` if everyone is allowed to propose.
    #[debug(skip_if = Option::is_none)]
    pub leader: Option<Owner>,
    /// The timestamp when the current round times out.
    #[debug(skip_if = Option::is_none)]
    pub round_timeout: Option<Timestamp>,
}

impl From<&ChainManager> for ChainManagerInfo {
    fn from(manager: &ChainManager) -> Self {
        let current_round = manager.current_round;
        let pending = manager
            .confirmed_vote
            .as_ref()
            .map(|vote| vote.lite())
            .or_else(move || manager.validated_vote.as_ref().map(|vote| vote.lite()));
        ChainManagerInfo {
            ownership: manager.ownership.clone(),
            requested_proposed: None,
            requested_locked: None,
            timeout: manager.timeout.clone().map(Box::new),
            pending,
            timeout_vote: manager.timeout_vote.as_ref().map(Vote::lite),
            fallback_vote: manager.fallback_vote.as_ref().map(Vote::lite),
            requested_confirmed: None,
            requested_validated: None,
            current_round,
            leader: manager.round_leader(current_round).cloned(),
            round_timeout: manager.round_timeout,
        }
    }
}

impl ChainManagerInfo {
    /// Adds requested certificate values and proposals to the `ChainManagerInfo`.
    pub fn add_values(&mut self, manager: &ChainManager) {
        self.requested_proposed = manager.proposed.clone().map(Box::new);
        self.requested_locked = manager.locked.clone().map(Box::new);
        self.requested_confirmed = manager
            .confirmed_vote
            .as_ref()
            .map(|vote| Box::new(vote.value.clone()));
        self.requested_validated = manager
            .validated_vote
            .as_ref()
            .map(|vote| Box::new(vote.value.clone()));
    }

    /// Returns whether the `identity` is allowed to propose a block in `round`.
    /// This is dependant on the type of round and whether `identity` is a validator or (super)owner.
    pub fn can_propose(&self, identity: &Owner, round: Round) -> bool {
        match round {
            Round::Fast => self.ownership.super_owners.contains_key(identity),
            Round::MultiLeader(_) => true,
            Round::SingleLeader(_) | Round::Validator(_) => self.leader.as_ref() == Some(identity),
        }
    }

    /// Returns whether a proposal with this content was already handled.
    pub fn already_handled_proposal(&self, round: Round, block: &Proposal) -> bool {
        self.requested_proposed.as_ref().is_some_and(|proposal| {
            proposal.content.round == round && proposal.content.proposal == *block
        })
    }

    /// Returns whether there is a locked block in the current round.
    pub fn has_locked_block_in_current_round(&self) -> bool {
        self.requested_locked
            .as_ref()
            .is_some_and(|certificate| certificate.round == self.current_round)
    }
}
