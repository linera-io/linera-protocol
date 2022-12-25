// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{Block, BlockAndRound, BlockProposal, Certificate, Value, Vote},
    ChainError,
};
use linera_base::{
    crypto::{HashValue, KeyPair},
    data_types::{BlockHeight, Owner, RoundNumber},
    ensure,
};
use linera_execution::{ApplicationId, ChainOwnership, Destination, Effect};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// How to produce new blocks.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
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
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct SingleOwnerManager {
    /// The owner of the chain.
    pub owner: Owner,
    /// Latest proposal that we have voted on last (both to validate and confirm it).
    pub pending: Option<Vote>,
}

/// The specific state of a chain managed by multiple owners.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
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
            if let Value::ValidatedBlock { round, .. } = &cert.value {
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
    #[allow(clippy::result_large_err)]
    pub fn check_proposed_block(
        &self,
        block_hash: Option<HashValue>,
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
                    match &vote.value {
                        Value::ConfirmedBlock { block, .. } if block != new_block => {
                            log::error!("Attempting to sign a different block at the same height:\n{:?}\n{:?}", block, new_block);
                            return Err(ChainError::PreviousBlockMustBeConfirmedFirst);
                        }
                        Value::ValidatedBlock { .. } => {
                            return Err(ChainError::InvalidBlockProposal);
                        }
                        _ => {
                            return Ok(Outcome::Skip);
                        }
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
                if let Some(cert) = &manager.locked {
                    match &cert.value {
                        Value::ValidatedBlock { round, .. } if new_round <= *round => {
                            return Err(ChainError::InsufficientRound(*round));
                        }
                        Value::ValidatedBlock { block, round, .. } if new_block != block => {
                            return Err(ChainError::HasLockedBlock(block.height, *round));
                        }
                        _ => (),
                    }
                }
                Ok(Outcome::Accept)
            }
            _ => panic!("unexpected chain manager"),
        }
    }

    #[allow(clippy::result_large_err)]
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
                if let Some(vote) = &manager.pending {
                    match &vote.value {
                        Value::ConfirmedBlock { block, .. } if block == new_block => {
                            return Ok(Outcome::Skip);
                        }
                        Value::ValidatedBlock { round, .. } if new_round < *round => {
                            return Err(ChainError::InsufficientRound(
                                round.try_sub_one().unwrap(),
                            ));
                        }
                        _ => (),
                    }
                }
                if let Some(cert) = &manager.locked {
                    match &cert.value {
                        Value::ValidatedBlock { round, .. } if new_round < *round => {
                            return Err(ChainError::InsufficientRound(
                                round.try_sub_one().unwrap(),
                            ));
                        }
                        _ => (),
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
        effects: Vec<(ApplicationId, Destination, Effect)>,
        state_hash: HashValue,
        key_pair: Option<&KeyPair>,
    ) {
        match self {
            ChainManager::Single(manager) => {
                if let Some(key_pair) = key_pair {
                    // Vote to confirm.
                    let BlockAndRound { block, .. } = proposal.content;
                    let value = Value::ConfirmedBlock {
                        block,
                        effects,
                        state_hash,
                    };
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
                    let value = Value::ValidatedBlock {
                        block,
                        round,
                        effects,
                        state_hash,
                    };
                    let vote = Vote::new(value, key_pair);
                    manager.pending = Some(vote);
                }
            }
            _ => panic!("unexpected chain manager"),
        }
    }

    pub fn create_final_vote(
        &mut self,
        block: Block,
        effects: Vec<(ApplicationId, Destination, Effect)>,
        state_hash: HashValue,
        certificate: Certificate,
        key_pair: Option<&KeyPair>,
    ) {
        match self {
            ChainManager::Multi(manager) => {
                // Record validity certificate. This is important to keep track of rounds
                // for non-voting nodes.
                manager.locked = Some(certificate);
                if let Some(key_pair) = key_pair {
                    // Vote to confirm.
                    let value = Value::ConfirmedBlock {
                        block,
                        effects,
                        state_hash,
                    };
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
