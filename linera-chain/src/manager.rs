// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{
        Block, BlockAndRound, BlockProposal, Certificate, HashedValue, LiteVote, OutgoingEffect,
        ValueKind, Vote,
    },
    ChainError,
};
use linera_base::{
    crypto::{CryptoHash, KeyPair},
    data_types::{BlockHeight, Owner, RoundNumber},
    ensure,
};
use linera_execution::ChainOwnership;
use log::error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// How to produce new blocks.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ChainManager {
    /// The chain is not active. (No blocks can be created)
    None,
    /// The chain is managed by a single owner.
    Single(Box<SingleOwnerManager>),
    /// The chain is managed by multiple owners.
    Multi(Box<MultiOwnerManager>),
}

/// The specific state of a chain managed by one owner.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SingleOwnerManager {
    /// The owner of the chain.
    pub owner: Owner,
    /// Latest proposal that we have voted on last (both to validate and confirm it).
    pub pending: Option<Vote>,
}

/// The specific state of a chain managed by multiple owners.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MultiOwnerManager {
    /// The co-owners of the chain.
    /// Using a map instead a hashset because Serde treats HashSet's as vectors.
    pub owners: HashMap<Owner, ()>,
    /// Latest authenticated block that we have received.
    pub proposed: Option<BlockProposal>,
    /// Latest validated proposal that we have seen (and voted to confirm).
    pub locked: Option<Certificate>,
    /// Latest proposal that we have voted on (either to validate or to confirm it).
    pub pending: Option<Vote>,
}

/// The result of verifying a (valid) query.
#[derive(Eq, PartialEq)]
pub enum Outcome {
    Accept,
    Skip,
}

impl Default for ChainManager {
    fn default() -> Self {
        ChainManager::None
    }
}

impl SingleOwnerManager {
    pub fn new(owner: Owner) -> Self {
        SingleOwnerManager {
            owner,
            pending: None,
        }
    }
}

impl MultiOwnerManager {
    pub fn new(owners: HashMap<Owner, ()>) -> Self {
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
        if let Some(cert) = &self.locked {
            if let ValueKind::ValidatedBlock { round } = &cert.value.kind() {
                if current_round < *round {
                    current_round = *round;
                }
            }
        }
        current_round
    }
}

impl ChainManager {
    pub fn reset(&mut self, ownership: &ChainOwnership) {
        match ownership {
            ChainOwnership::None => {
                *self = ChainManager::None;
            }
            ChainOwnership::Single { owner } => {
                *self = ChainManager::Single(Box::new(SingleOwnerManager::new(*owner)));
            }
            ChainOwnership::Multi { owners } => {
                *self = ChainManager::Multi(Box::new(MultiOwnerManager::new(owners.clone())));
            }
        }
    }

    pub fn is_active(&self) -> bool {
        !matches!(self, ChainManager::None)
    }

    pub fn has_owner(&self, owner: &Owner) -> bool {
        match self {
            ChainManager::Single(manager) => manager.owner == *owner,
            ChainManager::Multi(manager) => manager.owners.contains_key(owner),
            ChainManager::None => false,
        }
    }

    pub fn next_round(&self) -> RoundNumber {
        match self {
            ChainManager::Multi(m) => {
                let round = m.round();
                round.try_add_one().unwrap_or(round)
            }
            _ => RoundNumber::default(),
        }
    }

    pub fn pending(&self) -> Option<&Vote> {
        match self {
            ChainManager::Single(manager) => manager.pending.as_ref(),
            ChainManager::Multi(manager) => manager.pending.as_ref(),
            _ => None,
        }
    }

    /// Verify the safety of the block w.r.t. voting rules.
    pub fn check_proposed_block(
        &self,
        block_hash: Option<CryptoHash>,
        next_block_height: BlockHeight,
        new_block: &Block,
        new_round: RoundNumber,
    ) -> Result<Outcome, ChainError> {
        ensure!(
            new_block.height == next_block_height,
            ChainError::UnexpectedBlockHeight {
                expected_block_height: next_block_height,
                found_block_height: new_block.height
            }
        );
        ensure!(
            new_block.previous_block_hash == block_hash,
            ChainError::UnexpectedPreviousBlockHash
        );
        ensure!(
            new_block.height <= BlockHeight::max(),
            ChainError::InvalidBlockHeight
        );
        match self {
            ChainManager::Single(manager) => {
                ensure!(
                    new_round == RoundNumber::default(),
                    ChainError::InvalidBlockProposal
                );
                if let Some(vote) = &manager.pending {
                    ensure!(vote.value.is_confirmed(), ChainError::InvalidBlockProposal);
                    if vote.value.block() == new_block {
                        return Ok(Outcome::Skip);
                    } else {
                        log::error!(
                            "Attempting to sign a different block at the same height:\n{:?}\n{:?}",
                            vote.value.block(),
                            new_block
                        );
                        return Err(ChainError::PreviousBlockMustBeConfirmedFirst);
                    }
                }
                Ok(Outcome::Accept)
            }
            ChainManager::Multi(manager) => {
                if let Some(proposal) = &manager.proposed {
                    if proposal.content.block == *new_block && proposal.content.round == new_round {
                        return Ok(Outcome::Skip);
                    }
                    if new_round <= proposal.content.round {
                        return Err(ChainError::InsufficientRound(proposal.content.round));
                    }
                }
                if let Some(Certificate { value, .. }) = &manager.locked {
                    if let ValueKind::ValidatedBlock { round } = value.kind() {
                        ensure!(new_round > round, ChainError::InsufficientRound(round));
                        ensure!(
                            new_block == value.block(),
                            ChainError::HasLockedBlock(value.block().height, round)
                        );
                    }
                }
                Ok(Outcome::Accept)
            }
            _ => panic!("unexpected chain manager"),
        }
    }

    pub fn check_validated_block(
        &self,
        next_block_height: BlockHeight,
        new_block: &Block,
        new_round: RoundNumber,
    ) -> Result<Outcome, ChainError> {
        if next_block_height < new_block.height {
            return Err(ChainError::MissingEarlierBlocks {
                current_block_height: next_block_height,
            });
        }
        if next_block_height > new_block.height {
            // Block was already confirmed.
            return Ok(Outcome::Skip);
        }
        match self {
            ChainManager::Multi(manager) => {
                if let Some(Vote { value, .. }) = &manager.pending {
                    match value.kind() {
                        ValueKind::ConfirmedBlock => {
                            if value.block() == new_block {
                                return Ok(Outcome::Skip);
                            }
                        }
                        ValueKind::ValidatedBlock { round } => ensure!(
                            new_round >= round,
                            ChainError::InsufficientRound(round.try_sub_one().unwrap())
                        ),
                    }
                }
                if let Some(Certificate { value, .. }) = &manager.locked {
                    if let ValueKind::ValidatedBlock { round } = value.kind() {
                        ensure!(
                            new_round >= round,
                            ChainError::InsufficientRound(round.try_sub_one().unwrap())
                        );
                    }
                }
                Ok(Outcome::Accept)
            }
            _ => panic!("unexpected chain manager"),
        }
    }

    pub fn create_vote(
        &mut self,
        proposal: BlockProposal,
        effects: Vec<OutgoingEffect>,
        state_hash: CryptoHash,
        key_pair: Option<&KeyPair>,
    ) {
        match self {
            ChainManager::Single(manager) => {
                if let Some(key_pair) = key_pair {
                    // Vote to confirm.
                    let BlockAndRound { block, .. } = proposal.content;
                    let value = HashedValue::new_confirmed(block, effects, state_hash);
                    let vote = Vote::new(value, key_pair);
                    manager.pending = Some(vote);
                }
            }
            ChainManager::Multi(manager) => {
                // Record the proposed block. This is important to keep track of rounds
                // for non-voting nodes.
                manager.proposed = Some(proposal.clone());
                if let Some(key_pair) = key_pair {
                    // Vote to validate.
                    let BlockAndRound { block, round } = proposal.content;
                    let value = HashedValue::new_validated(block, effects, state_hash, round);
                    let vote = Vote::new(value, key_pair);
                    manager.pending = Some(vote);
                }
            }
            _ => panic!("unexpected chain manager"),
        }
    }

    pub fn create_final_vote(&mut self, certificate: Certificate, key_pair: Option<&KeyPair>) {
        match self {
            ChainManager::Multi(manager) => {
                // Record validity certificate. This is important to keep track of rounds
                // for non-voting nodes.
                let value = certificate.value.clone().into_confirmed();
                manager.locked = Some(certificate);
                if let Some(key_pair) = key_pair {
                    // Vote to confirm.
                    let vote = Vote::new(value, key_pair);
                    // Ok to overwrite validation votes with confirmation votes at equal or
                    // higher round.
                    manager.pending = Some(vote);
                }
            }
            _ => panic!("unexpected chain manager"),
        }
    }
}

/// Chain manager information that is included in `ChainInfo` sent to clients.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub enum ChainManagerInfo {
    /// The chain is not active. (No blocks can be created)
    None,
    /// The chain is managed by a single owner.
    Single(Box<SingleOwnerManagerInfo>),
    /// The chain is managed by multiple owners.
    Multi(Box<MultiOwnerManagerInfo>),
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

/// Chain manager information that is included in `ChainInfo` sent to clients, about chains
/// with multiple owners.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct MultiOwnerManagerInfo {
    /// The co-owners of the chain.
    /// Using a map instead a hashset because Serde treats HashSet's as vectors.
    pub owners: HashMap<Owner, ()>,
    /// Latest authenticated block that we have received, if requested.
    pub requested_proposed: Option<BlockProposal>,
    /// Latest validated proposal that we have seen (and voted to confirm), if requested.
    pub requested_locked: Option<Certificate>,
    /// Latest vote we cast (either to validate or to confirm a block).
    pub pending: Option<LiteVote>,
    /// The value we voted for, if requested.
    pub requested_pending_value: Option<HashedValue>,
    /// The current round.
    pub round: RoundNumber,
}

impl From<&ChainManager> for ChainManagerInfo {
    fn from(manager: &ChainManager) -> Self {
        match manager {
            ChainManager::Single(single) => {
                ChainManagerInfo::Single(Box::new(SingleOwnerManagerInfo {
                    owner: single.owner,
                    pending: single.pending.as_ref().map(|vote| vote.lite()),
                    requested_pending_value: None,
                }))
            }
            ChainManager::Multi(multi) => {
                ChainManagerInfo::Multi(Box::new(MultiOwnerManagerInfo {
                    owners: multi.owners.clone(),
                    requested_proposed: None,
                    requested_locked: None,
                    pending: multi.pending.as_ref().map(|vote| vote.lite()),
                    requested_pending_value: None,
                    round: multi.round(),
                }))
            }
            ChainManager::None => ChainManagerInfo::None,
        }
    }
}

impl Default for ChainManagerInfo {
    fn default() -> Self {
        ChainManagerInfo::None
    }
}

impl ChainManagerInfo {
    pub fn add_values(&mut self, manager: &ChainManager) {
        match (self, manager) {
            (ChainManagerInfo::None, ChainManager::None) => {}
            (ChainManagerInfo::Single(info), ChainManager::Single(single)) => {
                info.requested_pending_value =
                    single.pending.as_ref().map(|vote| vote.value.clone());
            }
            (ChainManagerInfo::Multi(info), ChainManager::Multi(multi)) => {
                info.requested_proposed = multi.proposed.clone();
                info.requested_locked = multi.locked.clone();
                info.requested_pending_value =
                    multi.pending.as_ref().map(|vote| vote.value.clone());
            }
            (_, _) => error!("cannot assign info from a chain manager of different type"),
        }
    }

    pub fn pending(&self) -> Option<&LiteVote> {
        match self {
            ChainManagerInfo::Single(single) => single.pending.as_ref(),
            ChainManagerInfo::Multi(multi) => multi.pending.as_ref(),
            _ => None,
        }
    }

    pub fn next_round(&self) -> RoundNumber {
        match self {
            ChainManagerInfo::Multi(multi) => multi.round.try_add_one().unwrap_or(multi.round),
            _ => RoundNumber::default(),
        }
    }
}
