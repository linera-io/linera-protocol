// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::Outcome;
use crate::{
    data_types::{
        Block, BlockAndRound, BlockProposal, ExecutedBlock, HashedValue, LiteVote, OutgoingEffect,
        Value, Vote,
    },
    ChainError,
};
use linera_base::{
    crypto::{CryptoHash, KeyPair, PublicKey},
    data_types::RoundNumber,
    ensure,
    identifiers::Owner,
};
use serde::{Deserialize, Serialize};

/// The specific state of a chain managed by one owner.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SingleOwnerManager {
    /// The owner of the chain.
    pub owner: Owner,
    /// The corresponding public key.
    pub public_key: PublicKey,
    /// Latest proposal that we have voted on last (both to validate and confirm it).
    pub pending: Option<Vote>,
}

impl SingleOwnerManager {
    pub fn new(owner: Owner, public_key: PublicKey) -> Self {
        SingleOwnerManager {
            public_key,
            owner,
            pending: None,
        }
    }

    pub fn pending(&self) -> Option<&Vote> {
        self.pending.as_ref()
    }

    /// Verify the safety of the block w.r.t. voting rules.
    pub fn check_proposed_block(
        &self,
        new_block: &Block,
        new_round: RoundNumber,
    ) -> Result<Outcome, ChainError> {
        ensure!(
            new_round == RoundNumber::default(),
            ChainError::InvalidBlockProposal
        );
        if let Some(vote) = &self.pending {
            let block = match &vote.value.inner() {
                Value::ConfirmedBlock { executed } => &executed.block,
                Value::ValidatedBlock { .. } => return Err(ChainError::InvalidBlockProposal),
            };
            if block == new_block {
                return Ok(Outcome::Skip);
            } else {
                tracing::error!(
                    "Attempting to sign a different block at the same height:\n{:?}\n{:?}",
                    block,
                    new_block
                );
                return Err(ChainError::PreviousBlockMustBeConfirmedFirst);
            }
        }
        Ok(Outcome::Accept)
    }

    pub fn create_vote(
        &mut self,
        proposal: BlockProposal,
        effects: Vec<OutgoingEffect>,
        state_hash: CryptoHash,
        key_pair: Option<&KeyPair>,
    ) {
        if let Some(key_pair) = key_pair {
            // Vote to confirm.
            let BlockAndRound { block, .. } = proposal.content;
            let executed = ExecutedBlock {
                block,
                effects,
                state_hash,
            };
            let vote = Vote::new(HashedValue::new_confirmed(executed), key_pair);
            self.pending = Some(vote);
        }
    }
}

/// Chain manager information that is included in `ChainInfo` sent to clients, about chains
/// with one owner.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct SingleOwnerManagerInfo {
    /// The owner of the chain.
    pub owner: Owner,
    /// Latest vote we cast.
    pub pending: Option<LiteVote>,
    /// The value we voted for, if requested.
    pub requested_pending_value: Option<HashedValue>,
}

impl From<&SingleOwnerManager> for SingleOwnerManagerInfo {
    fn from(manager: &SingleOwnerManager) -> Self {
        SingleOwnerManagerInfo {
            owner: manager.owner,
            pending: manager.pending.as_ref().map(|vote| vote.lite()),
            requested_pending_value: None,
        }
    }
}

impl SingleOwnerManagerInfo {
    pub fn add_values(&mut self, manager: &SingleOwnerManager) {
        self.requested_pending_value = manager.pending.as_ref().map(|vote| vote.value.clone());
    }
}
