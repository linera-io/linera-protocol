// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

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
    data_types::{BlockHeight, RoundNumber},
    ensure,
    identifiers::{ChainId, Owner},
};
use linera_execution::committee::Epoch;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::error;

/// The specific state of a chain managed by multiple owners.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MultiOwnerManager {
    /// The co-owners of the chain.
    pub owners: HashMap<Owner, PublicKey>,
    /// Latest authenticated block that we have received (and voted to validate).
    pub proposed: Option<BlockProposal>,
    /// Latest validated proposal that we have seen (and voted to confirm).
    pub locked: Option<Certificate>,
    /// Latest proposal that we have voted on (either to validate or to confirm it).
    pub pending: Option<Vote>,
}

impl MultiOwnerManager {
    pub fn new(owners: HashMap<Owner, PublicKey>) -> Self {
        MultiOwnerManager {
            owners,
            proposed: None,
            locked: None,
            pending: None,
        }
    }

    pub fn round(&self) -> RoundNumber {
        let mut current_round = RoundNumber::default();
        if let Some(proposal) = &self.proposed {
            if current_round < proposal.content.round {
                current_round = proposal.content.round;
            }
        }
        if let Some(Certificate { round, .. }) = &self.locked {
            if current_round < *round {
                current_round = *round;
            }
        }
        current_round
    }

    /// Returns the most recent vote we cast.
    pub fn pending(&self) -> Option<&Vote> {
        self.pending.as_ref()
    }

    /// Verifies the safety of the block w.r.t. voting rules.
    pub fn check_proposed_block(&self, proposal: &BlockProposal) -> Result<Outcome, ChainError> {
        let new_block = &proposal.content.block;
        let new_round = proposal.content.round;
        if let Some(old_proposal) = &self.proposed {
            if old_proposal.content.block == *new_block && old_proposal.content.round == new_round {
                return Ok(Outcome::Skip); // We already voted for this proposal; nothing to do.
            }
            if new_round <= old_proposal.content.round {
                // We already accepted a proposal in this or a higher round.
                return Err(ChainError::InsufficientRound(old_proposal.content.round));
            }
        }
        // If we have a locked block, it must either match the proposal, or the proposal must
        // include a higher certificate that validates the proposed block.
        if let Some(locked) = proposal
            .validated
            .iter()
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
        _chain_id: ChainId,
        _height: BlockHeight,
        _epoch: Epoch,
        _key_pair: Option<&KeyPair>,
    ) -> bool {
        false // TODO(#464)
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
                CertificateValue::ValidatedBlock { .. } => ensure!(
                    new_round >= *round,
                    ChainError::InsufficientRound(round.try_sub_one().unwrap())
                ),
                CertificateValue::LeaderTimeout { .. } => {
                    // Unreachable: We only put validated or confirmed blocks in pending.
                    return Err(ChainError::InternalError(
                        "pending can only be validated or confirmed block".to_string(),
                    ));
                }
            }
        }
        if let Some(Certificate { round, .. }) = &self.locked {
            ensure!(
                new_round >= *round,
                ChainError::InsufficientRound(round.try_sub_one().unwrap())
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
    ) {
        // Record the proposed block, so it can be supplied to clients that request it.
        self.proposed = Some(proposal.clone());
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
    pub fn create_final_vote(&mut self, certificate: Certificate, key_pair: Option<&KeyPair>) {
        let Some(value) = certificate.value.clone().into_confirmed() else {
            // Unreachable: This is only called with validated blocks.
            error!("Unexpected certificate; expected ValidatedBlock");
            return;
        };
        let round = certificate.round;
        self.locked = Some(certificate);
        if let Some(key_pair) = key_pair {
            // Vote to confirm.
            let vote = Vote::new(value, round, key_pair);
            // Ok to overwrite validation votes with confirmation votes at equal or higher round.
            self.pending = Some(vote);
        }
    }

    /// Updates the round number and timer if the timeout certificate is from a higher round than
    /// any known certificate.
    pub fn handle_timeout_certificate(&mut self, _certificate: Certificate) {
        // TODO(#464)
    }

    pub fn verify_owner(&self, proposal: &BlockProposal) -> Option<PublicKey> {
        self.owners.get(&proposal.owner).copied()
    }
}

/// Chain manager information that is included in `ChainInfo` sent to clients, about chains
/// with multiple owners.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct MultiOwnerManagerInfo {
    /// The co-owners of the chain.
    /// Using a map instead a hashset because Serde treats HashSet's as vectors.
    pub owners: HashMap<Owner, PublicKey>,
    /// Latest authenticated block that we have received, if requested.
    pub requested_proposed: Option<BlockProposal>,
    /// Latest validated proposal that we have seen (and voted to confirm), if requested.
    pub requested_locked: Option<Certificate>,
    /// Latest vote we cast (either to validate or to confirm a block).
    pub pending: Option<LiteVote>,
    /// Latest timeout vote we cast.
    pub timeout_vote: Option<LiteVote>,
    /// The value we voted for, if requested.
    pub requested_pending_value: Option<HashedValue>,
    /// The current round.
    pub round: RoundNumber,
}

impl From<&MultiOwnerManager> for MultiOwnerManagerInfo {
    fn from(manager: &MultiOwnerManager) -> Self {
        MultiOwnerManagerInfo {
            owners: manager.owners.clone(),
            requested_proposed: None,
            requested_locked: None,
            pending: manager.pending.as_ref().map(|vote| vote.lite()),
            timeout_vote: None,
            requested_pending_value: None,
            round: manager.round(),
        }
    }
}

impl MultiOwnerManagerInfo {
    pub fn add_values(&mut self, manager: &MultiOwnerManager) {
        self.requested_proposed = manager.proposed.clone();
        self.requested_locked = manager.locked.clone();
        self.requested_pending_value = manager.pending.as_ref().map(|vote| vote.value.clone());
    }

    pub fn next_round(&self) -> RoundNumber {
        self.round.try_add_one().unwrap_or(self.round)
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
}
