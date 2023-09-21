// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! # Manager for Multi-Owner Chains
//!
//! This module contains the consensus mechanism for multi-owner chains. Whenever a block is
//! confirmed, a new chain manager is created for the next block height. It manages the consensus
//! state until a new block is confirmed. As long as less than a third of the validators are faulty,
//! it guarantees that at most one `ConfirmedBlock` certificate will be created for this height.
//! There are two modes of operation:
//!
//! * In cooperative mode (multi-owner rounds), all chain owners can propose blocks at any time.
//!   The protocol is guaranteed to eventually confirm a block as long as no chain owner
//!   continuously actively prevents progress.
//! * In leader rotation mode, chain owners take turns at proposing blocks. It can make progress
//!   as long as at least one owner is honest, even if other owners try to prevent it.
//!
//! ## Safety, i.e. at most one block will be confirmed
//!
//! In both modes this is guaranteed as follows:
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
//!
//! ## Liveness, i.e. some block will eventually be confirmed
//!
//! In cooperative mode, if there is contention, the owners need to agree on a single owner as the
//! next proposer. That owner should then download all highest-round certificates and block
//! proposals known to the honest validators. They can then make a proposal in a round higher than
//! all previous proposals. If there is any `ValidatedBlock` certificate they must include the
//! highest one in their proposal, and propose that block. Otherwise they can propose a new block.
//! Now all honest validators are allowed to vote for that proposal, and eventually confirm it.
//!
//! In leader-based mode, an honest owner should subscribe to notifications from all validators,
//! and follow the chain. Whenever another leader's round takes too long, they should request
//! timeout votes from the validators to make the next round begin. Once the honest owner becomes
//! the round leader, they should update all validators, so that they all agree on the current
//! round. Then they download the highest `ValidatedBlock` certificate known to any honest validator
//! and include that in their block proposal, just like in the cooperative case.

use super::Outcome;
use crate::{
    data_types::{
        BlockAndRound, BlockProposal, Certificate, CertificateValue, ExecutedBlock, HashedValue,
        LiteVote, OutgoingMessage, Vote,
    },
    ChainError,
};
use linera_base::{
    crypto::{CryptoHash, KeyPair, PublicKey},
    data_types::{BlockHeight, RoundNumber, Timestamp},
    ensure,
    identifiers::{ChainId, Owner},
};
use linera_execution::committee::Epoch;
use rand_chacha::{rand_core::SeedableRng, ChaCha8Rng};
use rand_distr::{Distribution, WeightedAliasIndex};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};
use tracing::error;

const TIMEOUT: Duration = Duration::from_secs(10);

/// The consensus state of a chain with multiple owners some of which are potentially faulty.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MultiOwnerManager {
    /// The public keys of the chain's co-owners, with their weights.
    pub public_keys: BTreeMap<Owner, (PublicKey, u64)>,
    /// The number of initial rounds in which all owners are allowed to propose blocks,
    /// i.e. the first round with only a single leader.
    pub multi_leader_rounds: RoundNumber,
    /// The seed for the pseudo-random number generator that determines the round leaders.
    pub seed: u64,
    /// The probability distribution for choosing a round leader.
    pub distribution: WeightedAliasIndex<u64>,
    /// Latest authenticated block that we have received and checked.
    pub proposed: Option<BlockProposal>,
    /// Latest validated proposal that we have voted to confirm (or would have, if we are not a
    /// validator).
    pub locked: Option<Certificate>,
    /// Latest leader timeout certificate we have received.
    pub leader_timeout: Option<Certificate>,
    /// Latest vote we have cast, to validate or confirm.
    pub pending: Option<Vote>,
    /// Latest timeout vote we cast.
    pub timeout_vote: Option<Vote>,
    /// The time after which we are ready to sign a timeout certificate for the current round.
    pub round_timeout: Timestamp,
}

impl MultiOwnerManager {
    pub fn new(
        public_keys: impl IntoIterator<Item = (Owner, (PublicKey, u64))>,
        multi_leader_rounds: RoundNumber,
        seed: u64,
        now: Timestamp,
    ) -> Result<Self, ChainError> {
        let public_keys: BTreeMap<Owner, (PublicKey, u64)> = public_keys.into_iter().collect();
        let weights = public_keys.values().map(|(_, weight)| *weight).collect();
        let distribution = WeightedAliasIndex::new(weights)?;
        let round_timeout = now.saturating_add(TIMEOUT);

        Ok(MultiOwnerManager {
            public_keys,
            multi_leader_rounds,
            seed,
            distribution,
            proposed: None,
            locked: None,
            leader_timeout: None,
            pending: None,
            timeout_vote: None,
            round_timeout,
        })
    }

    /// Returns the lowest round where we can still vote to validate or confirm a block. This is
    /// the round to which the current timeout applies.
    ///
    /// Having a leader timeout certificate in any given round causes the next one to become
    /// current. Seeing a validated block certificate or a valid proposal in any round causes that
    /// round to become current, unless a higher one already is.
    pub fn current_round(&self) -> RoundNumber {
        self.leader_timeout
            .iter()
            .map(|certificate| certificate.round.try_add_one().unwrap_or(RoundNumber::MAX))
            .chain(self.locked.iter().map(|certificate| certificate.round))
            .chain(self.proposed.iter().map(|proposal| proposal.content.round))
            .max()
            .unwrap_or_default()
    }

    /// Returns the most recent vote we cast.
    pub fn pending(&self) -> Option<&Vote> {
        self.pending.as_ref()
    }

    /// Verifies the safety of a proposed block with respect to voting rules.
    pub fn check_proposed_block(&self, proposal: &BlockProposal) -> Result<Outcome, ChainError> {
        let new_round = proposal.content.round;
        let new_block = &proposal.content.block;
        let validated = proposal.validated.as_ref();
        if let Some(validated) = validated {
            ensure!(
                validated.value().is_validated(),
                ChainError::InvalidBlockProposal
            );
        }
        let expected_round = match validated {
            None => self.current_round(),
            Some(cert) => cert.round.try_add_one()?.max(self.current_round()),
        };
        // In leader rotation mode, the round must equal the expected one exactly.
        // Only the first single-leader round can be entered at any time.
        if new_round > self.multi_leader_rounds {
            ensure!(
                expected_round == new_round,
                ChainError::WrongRound(expected_round)
            );
        } else {
            ensure!(
                expected_round <= new_round,
                ChainError::InsufficientRound(expected_round)
            );
        }
        if let Some(old_proposal) = &self.proposed {
            if old_proposal.content == proposal.content {
                return Ok(Outcome::Skip); // We already voted for this proposal; nothing to do.
            }
            if new_round <= old_proposal.content.round {
                // We already accepted a proposal in this or a higher round.
                return Err(ChainError::InsufficientRound(old_proposal.content.round));
            }
        }
        // If we have a locked block, it must either match the proposal, or the proposal must
        // include a higher certificate that validates the proposed block.
        if let Some(locked) = validated
            .into_iter()
            .chain(&self.locked)
            .max_by_key(|cert| cert.round)
        {
            let block = locked.value().block().ok_or_else(|| {
                // Should be unreachable: We only put validated block certificates into the locked
                // field, and we checked that the proposal includes only validated blocks.
                ChainError::InternalError("locked certificate must contain block".to_string())
            })?;
            ensure!(
                locked.round < new_round && block == new_block,
                ChainError::HasLockedBlock(block.height, locked.round)
            );
        }
        Ok(Outcome::Accept)
    }

    /// Checks if the current round has timed out, and signs a `LeaderTimeout`.
    pub fn vote_leader_timeout(
        &mut self,
        chain_id: ChainId,
        height: BlockHeight,
        epoch: Epoch,
        key_pair: Option<&KeyPair>,
        now: Timestamp,
    ) -> bool {
        let Some(key_pair) = key_pair else {
            return false; // We are not a validator.
        };
        if now < self.round_timeout {
            return false; // Round has not timed out yet.
        }
        let current_round = self.current_round();
        if let Some(vote) = &self.timeout_vote {
            if vote.round == current_round {
                return false; // We already signed this timeout.
            }
        }
        let value = HashedValue::new_leader_timeout(chain_id, height, epoch);
        self.timeout_vote = Some(Vote::new(value, current_round, key_pair));
        true
    }

    /// Verifies that we can vote to confirm a validated block.
    pub fn check_validated_block(&self, certificate: &Certificate) -> Result<Outcome, ChainError> {
        let new_block = certificate.value().block();
        let new_round = certificate.round;
        if let Some(Vote { value, round, .. }) = &self.pending {
            match value.inner() {
                CertificateValue::ConfirmedBlock { executed_block } => {
                    if Some(&executed_block.block) == new_block && *round == new_round {
                        return Ok(Outcome::Skip); // We already voted to confirm this block.
                    }
                }
                CertificateValue::ValidatedBlock { .. } => {
                    ensure!(new_round >= *round, ChainError::InsufficientRound(*round))
                }
                CertificateValue::LeaderTimeout { .. } => {
                    // Unreachable: We only put validated or confirmed blocks in pending.
                    return Err(ChainError::InternalError(
                        "pending can only be validated or confirmed block".to_string(),
                    ));
                }
            }
        }
        if let Some(locked) = &self.locked {
            ensure!(
                new_round > locked.round,
                ChainError::InsufficientRound(locked.round)
            );
        }
        Ok(Outcome::Accept)
    }

    /// Signs a vote to validate the proposed block.
    pub fn create_vote(
        &mut self,
        proposal: BlockProposal,
        messages: Vec<OutgoingMessage>,
        state_hash: CryptoHash,
        key_pair: Option<&KeyPair>,
        now: Timestamp,
    ) {
        if let Ok(round) = proposal.content.round.try_sub_one() {
            self.update_timeout(round, now);
        }
        // Record the proposed block, so it can be supplied to clients that request it.
        self.proposed = Some(proposal.clone());
        // If the validated block certificate is more recent, update our locked block.
        if let Some(validated) = &proposal.validated {
            if self
                .locked
                .as_ref()
                .map_or(true, |locked| locked.round < validated.round)
            {
                self.locked = Some(validated.clone());
            }
        }
        if let Some(key_pair) = key_pair {
            // Vote to validate.
            let BlockAndRound { block, round } = proposal.content;
            let executed_block = ExecutedBlock {
                block,
                messages,
                state_hash,
            };
            let vote = Vote::new(HashedValue::new_validated(executed_block), round, key_pair);
            self.pending = Some(vote);
        }
    }

    /// Signs a vote to confirm the validated block.
    pub fn create_final_vote(
        &mut self,
        certificate: Certificate,
        key_pair: Option<&KeyPair>,
        now: Timestamp,
    ) {
        let round = certificate.round;
        // Validators only change their locked block if the new one is included in a proposal in the
        // current round, or it is itself in the current round.
        if key_pair.is_some() && round < self.current_round() {
            return;
        }
        let Some(value) = certificate.value.clone().into_confirmed() else {
            // Unreachable: This is only called with validated blocks.
            error!("Unexpected certificate; expected ValidatedBlock");
            return;
        };
        self.update_timeout(round, now);
        self.locked = Some(certificate);
        if let Some(key_pair) = key_pair {
            // Vote to confirm.
            let vote = Vote::new(value, round, key_pair);
            // Ok to overwrite validation votes with confirmation votes at equal or higher round.
            self.pending = Some(vote);
        }
    }

    /// Resets the timer if `round` has just ended.
    fn update_timeout(&mut self, round: RoundNumber, now: Timestamp) {
        if self.current_round() <= round {
            let factor = round.0.saturating_add(2);
            let timeout = TIMEOUT.saturating_mul(factor);
            self.round_timeout = now.saturating_add(timeout);
        }
    }

    /// Updates the round number and timer if the timeout certificate is from a higher round than
    /// any known certificate.
    pub fn handle_timeout_certificate(&mut self, certificate: Certificate, now: Timestamp) {
        if !certificate.value().is_timeout() {
            // Unreachable: This is only called with timeout certificates.
            error!("Unexpected certificate; expected leader timeout");
            return;
        }
        let round = certificate.round;
        if let Some(known_certificate) = &self.leader_timeout {
            if known_certificate.round >= round {
                return;
            }
        }
        self.update_timeout(round, now);
        self.leader_timeout = Some(certificate);
    }

    /// Returns the public key of the block proposal's signer, if they are a valid owner and allowed
    /// to propose a block in the proposal's round.
    pub fn verify_owner(&self, proposal: &BlockProposal) -> Option<PublicKey> {
        if proposal.content.round < self.multi_leader_rounds {
            let (key, _) = self.public_keys.get(&proposal.owner)?;
            return Some(*key); // Not in leader rotation mode; any owner is allowed to propose.
        };
        let index = self.round_leader_index(proposal.content.round)?;
        let (leader, (key, _)) = self.public_keys.iter().nth(index)?;
        (*leader == proposal.owner).then_some(*key)
    }

    /// Returns the leader who is allowed to propose a block in the given round, or `None` if every
    /// owner is allowed to propose.
    fn round_leader(&self, round: RoundNumber) -> Option<&Owner> {
        if round < self.multi_leader_rounds {
            return None;
        };
        let index = self.round_leader_index(round)?;
        self.public_keys.keys().nth(index)
    }

    /// Returns the index of the leader who is allowed to propose a block in the given round, or
    /// `None` if every owner is allowed to propose.
    fn round_leader_index(&self, round: RoundNumber) -> Option<usize> {
        let seed = u64::from(round.0).rotate_left(32).wrapping_add(self.seed);
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        Some(self.distribution.sample(&mut rng))
    }
}

/// Chain manager information that is included in `ChainInfo` sent to clients, about chains
/// with multiple owners.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct MultiOwnerManagerInfo {
    /// The public keys of the chain's co-owners.
    pub public_keys: HashMap<Owner, (PublicKey, u64)>,
    /// The number of initial rounds in which all owners are allowed to propose blocks,
    /// i.e. the first round with only a single leader.
    pub multi_leader_rounds: RoundNumber,
    /// Latest authenticated block that we have received, if requested.
    pub requested_proposed: Option<BlockProposal>,
    /// Latest validated proposal that we have voted to confirm (or would have, if we are not a
    /// validator).
    pub requested_locked: Option<Certificate>,
    /// Latest timeout certificate we have seen.
    pub leader_timeout: Option<Certificate>,
    /// Latest vote we cast (either to validate or to confirm a block).
    pub pending: Option<LiteVote>,
    /// Latest timeout vote we cast.
    pub timeout_vote: Option<LiteVote>,
    /// The value we voted for, if requested.
    pub requested_pending_value: Option<HashedValue>,
    /// The current round, i.e. the lowest round where we can still vote to validate a block.
    pub current_round: RoundNumber,
    /// The current leader, who is allowed to propose the next block.
    /// `None` if everyone is allowed to propose.
    pub leader: Option<Owner>,
    /// The timestamp when the current round times out.
    pub round_timeout: Timestamp,
}

impl From<&MultiOwnerManager> for MultiOwnerManagerInfo {
    fn from(manager: &MultiOwnerManager) -> Self {
        let current_round = manager.current_round();
        MultiOwnerManagerInfo {
            public_keys: manager.public_keys.clone().into_iter().collect(),
            multi_leader_rounds: manager.multi_leader_rounds,
            requested_proposed: None,
            requested_locked: None,
            leader_timeout: manager.leader_timeout.clone(),
            pending: manager.pending.as_ref().map(|vote| vote.lite()),
            timeout_vote: manager.timeout_vote.as_ref().map(Vote::lite),
            requested_pending_value: None,
            current_round,
            leader: manager.round_leader(current_round).cloned(),
            round_timeout: manager.round_timeout,
        }
    }
}

impl MultiOwnerManagerInfo {
    pub fn add_values(&mut self, manager: &MultiOwnerManager) {
        self.requested_proposed = manager.proposed.clone();
        self.requested_locked = manager.locked.clone();
        self.requested_pending_value = manager.pending.as_ref().map(|vote| vote.value.clone());
    }

    pub fn highest_validated(&self) -> Option<&Certificate> {
        self.requested_locked
            .iter()
            .chain(
                self.requested_proposed
                    .as_ref()
                    .and_then(|proposal| proposal.validated.as_ref()),
            )
            .max_by_key(|cert| cert.round)
    }

    pub fn next_round(&self) -> Option<RoundNumber> {
        match &self.requested_proposed {
            Some(proposal) if proposal.content.round == self.current_round => {
                if self.current_round >= self.multi_leader_rounds {
                    None // There's already a proposal in the current round.
                } else {
                    self.current_round.try_add_one().ok()
                }
            }
            _ => Some(self.current_round),
        }
    }
}
