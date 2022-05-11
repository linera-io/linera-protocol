// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{base_types::*, ensure, error::Error, messages::*};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// How to produce new blocks.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
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
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct SingleOwnerManager {
    /// The owner of the chain.
    pub owner: Owner,
    /// Latest proposal that we have voted on last (both to validate and confirm it).
    pub pending: Option<Vote>,
}

/// The specific state of a chain managed by multiple owners.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct MultiOwnerManager {
    /// The co-owners of the chain.
    pub owners: HashSet<Owner>,
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
    pub fn new(owners: HashSet<Owner>) -> Self {
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
            if let Value::Validated { round, .. } = &cert.value {
                if current_round < *round {
                    current_round = *round;
                }
            }
        }
        current_round
    }
}

impl ChainManager {
    pub fn single(owner: Owner) -> Self {
        ChainManager::Single(Box::new(SingleOwnerManager::new(owner)))
    }

    pub fn multiple(owners: HashSet<Owner>) -> Self {
        ChainManager::Multi(Box::new(MultiOwnerManager::new(owners)))
    }

    pub fn reset(&mut self) {
        match self {
            ChainManager::None => (),
            ChainManager::Single(manager) => {
                *manager = Box::new(SingleOwnerManager::new(manager.owner));
            }
            ChainManager::Multi(manager) => {
                let owners = std::mem::take(&mut manager.owners);
                *manager = Box::new(MultiOwnerManager::new(owners));
            }
        }
    }

    pub fn is_active(&self) -> bool {
        !matches!(self, ChainManager::None)
    }

    pub fn has_owner(&self, owner: &Owner) -> bool {
        match self {
            ChainManager::Single(manager) => manager.owner == *owner,
            ChainManager::Multi(manager) => manager.owners.contains(owner),
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
        block_hash: Option<HashValue>,
        next_block_height: BlockHeight,
        new_block: &Block,
        new_round: RoundNumber,
    ) -> Result<Outcome, Error> {
        ensure!(
            new_block.height <= BlockHeight::max(),
            Error::InvalidBlockHeight
        );
        ensure!(
            new_block.previous_block_hash == block_hash,
            Error::UnexpectedPreviousBlockHash
        );
        ensure!(
            new_block.height == next_block_height,
            Error::UnexpectedBlockHeight
        );
        match self {
            ChainManager::Single(manager) => {
                ensure!(
                    new_round == RoundNumber::default(),
                    Error::InvalidBlockProposal
                );
                if let Some(vote) = &manager.pending {
                    match &vote.value {
                        Value::Confirmed { block } if block != new_block => {
                            return Err(Error::PreviousBlockMustBeConfirmedFirst);
                        }
                        Value::Validated { .. } => {
                            return Err(Error::InvalidBlockProposal);
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
                        return Err(Error::InsufficientRound(proposal.content.round));
                    }
                }
                if let Some(cert) = &manager.locked {
                    match &cert.value {
                        Value::Validated { round, .. } if new_round <= *round => {
                            return Err(Error::InsufficientRound(*round));
                        }
                        Value::Validated { block, round } if new_block != block => {
                            return Err(Error::HasLockedBlock(block.height, *round));
                        }
                        _ => (),
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
    ) -> Result<Outcome, Error> {
        if next_block_height < new_block.height {
            return Err(Error::MissingEarlierBlocks {
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
                        Value::Confirmed { block } if block == new_block => {
                            return Ok(Outcome::Skip);
                        }
                        Value::Validated { round, .. } if new_round < *round => {
                            return Err(Error::InsufficientRound(round.try_sub_one().unwrap()));
                        }
                        _ => (),
                    }
                }
                if let Some(cert) = &manager.locked {
                    match &cert.value {
                        Value::Validated { round, .. } if new_round < *round => {
                            return Err(Error::InsufficientRound(round.try_sub_one().unwrap()));
                        }
                        _ => (),
                    }
                }
                Ok(Outcome::Accept)
            }
            _ => panic!("unexpected chain manager"),
        }
    }

    pub fn create_vote(&mut self, proposal: BlockProposal, key_pair: Option<&KeyPair>) {
        match self {
            ChainManager::Single(manager) => {
                if let Some(key_pair) = key_pair {
                    // Vote to confirm.
                    let BlockAndRound { block, .. } = proposal.content;
                    let value = Value::Confirmed { block };
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
                    let value = Value::Validated { block, round };
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
                    let value = Value::Confirmed { block };
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
