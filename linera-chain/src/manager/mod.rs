// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod multi;
mod single;

pub use multi::{MultiOwnerManager, MultiOwnerManagerInfo};
pub use single::{SingleOwnerManager, SingleOwnerManagerInfo};

use crate::{
    data_types::{Block, BlockProposal, Certificate, LiteVote, OutgoingEffect, Vote},
    ChainError,
};
use linera_base::{
    crypto::{CryptoHash, KeyPair, PublicKey},
    data_types::{BlockHeight, RoundNumber},
    doc_scalar, ensure,
    identifiers::Owner,
};
use linera_execution::ChainOwnership;
use serde::{Deserialize, Serialize};
use tracing::error;

/// The state of the certification process for a chain's next block.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub enum ChainManager {
    /// The chain is not active. (No blocks can be created)
    #[default]
    None,
    /// The chain is managed by a single owner.
    Single(Box<SingleOwnerManager>),
    /// The chain is managed by multiple owners.
    Multi(Box<MultiOwnerManager>),
}

doc_scalar!(
    ChainManager,
    "The state of the certification process for a chain's next block"
);

/// The result of verifying a (valid) query.
#[derive(Eq, PartialEq)]
pub enum Outcome {
    Accept,
    Skip,
}

impl ChainManager {
    pub fn reset(&mut self, ownership: &ChainOwnership) {
        match ownership {
            ChainOwnership::None => {
                *self = ChainManager::None;
            }
            ChainOwnership::Single { owner, public_key } => {
                *self =
                    ChainManager::Single(Box::new(SingleOwnerManager::new(*owner, *public_key)));
            }
            ChainOwnership::Multi { owners } => {
                *self = ChainManager::Multi(Box::new(MultiOwnerManager::new(owners.clone())));
            }
        }
    }

    pub fn is_active(&self) -> bool {
        !matches!(self, ChainManager::None)
    }

    pub fn verify_owner(&self, owner: &Owner) -> Option<PublicKey> {
        match self {
            ChainManager::Single(manager) => {
                if manager.owner == *owner {
                    Some(manager.public_key)
                } else {
                    None
                }
            }
            ChainManager::Multi(manager) => manager.owners.get(owner).copied(),
            ChainManager::None => None,
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
            ChainManager::Single(manager) => manager.pending(),
            ChainManager::Multi(manager) => manager.pending(),
            _ => None,
        }
    }

    /// Verifies the safety of the block w.r.t. voting rules.
    pub fn check_proposed_block(
        &self,
        new_block: &Block,
        new_round: RoundNumber,
    ) -> Result<Outcome, ChainError> {
        // When a block is certified, incrementing its height must succeed.
        ensure!(
            new_block.height < BlockHeight::max(),
            ChainError::InvalidBlockHeight
        );
        match self {
            ChainManager::Single(manager) => manager.check_proposed_block(new_block, new_round),
            ChainManager::Multi(manager) => manager.check_proposed_block(new_block, new_round),
            _ => panic!("unexpected chain manager"),
        }
    }

    pub fn check_validated_block(
        &self,
        new_block: &Block,
        new_round: RoundNumber,
    ) -> Result<Outcome, ChainError> {
        match self {
            ChainManager::Multi(manager) => manager.check_validated_block(new_block, new_round),
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
                manager.create_vote(proposal, effects, state_hash, key_pair)
            }
            ChainManager::Multi(manager) => {
                manager.create_vote(proposal, effects, state_hash, key_pair)
            }
            _ => panic!("unexpected chain manager"),
        }
    }

    pub fn create_final_vote(&mut self, certificate: Certificate, key_pair: Option<&KeyPair>) {
        match self {
            ChainManager::Multi(manager) => manager.create_final_vote(certificate, key_pair),
            _ => panic!("unexpected chain manager"),
        }
    }
}

/// Chain manager information that is included in `ChainInfo` sent to clients.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub enum ChainManagerInfo {
    /// The chain is not active. (No blocks can be created)
    #[default]
    None,
    /// The chain is managed by a single owner.
    Single(Box<SingleOwnerManagerInfo>),
    /// The chain is managed by multiple owners.
    Multi(Box<MultiOwnerManagerInfo>),
}

impl From<&ChainManager> for ChainManagerInfo {
    fn from(manager: &ChainManager) -> Self {
        match manager {
            ChainManager::Single(single) => ChainManagerInfo::Single(Box::new((&**single).into())),
            ChainManager::Multi(multi) => ChainManagerInfo::Multi(Box::new((&**multi).into())),
            ChainManager::None => ChainManagerInfo::None,
        }
    }
}

impl ChainManagerInfo {
    pub fn add_values(&mut self, manager: &ChainManager) {
        match (self, manager) {
            (ChainManagerInfo::None, ChainManager::None) => {}
            (ChainManagerInfo::Single(info), ChainManager::Single(single)) => {
                info.add_values(single)
            }
            (ChainManagerInfo::Multi(info), ChainManager::Multi(multi)) => info.add_values(multi),
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
            ChainManagerInfo::Multi(multi) => multi.next_round(),
            _ => RoundNumber::default(),
        }
    }
}
