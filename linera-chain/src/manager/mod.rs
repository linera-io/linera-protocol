// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod multi;
mod single;

pub use multi::{MultiOwnerManager, MultiOwnerManagerInfo};
pub use single::{SingleOwnerManager, SingleOwnerManagerInfo};

use crate::{
    data_types::{BlockProposal, Certificate, LiteVote, OutgoingMessage, Vote},
    ChainError,
};
use linera_base::{
    crypto::{CryptoHash, KeyPair, PublicKey},
    data_types::{BlockHeight, RoundNumber},
    doc_scalar, ensure,
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
                let owners = owners
                    .iter()
                    .map(|(owner, public_key)| (*owner, *public_key))
                    .collect();
                *self = ChainManager::Multi(Box::new(MultiOwnerManager::new(owners)));
            }
        }
    }

    pub fn is_active(&self) -> bool {
        !matches!(self, ChainManager::None)
    }

    pub fn verify_owner(&self, proposal: &BlockProposal) -> Option<PublicKey> {
        match self {
            ChainManager::Single(manager) => manager.verify_owner(proposal),
            ChainManager::Multi(manager) => manager.verify_owner(proposal),
            ChainManager::None => None,
        }
    }

    pub fn next_round(&self) -> RoundNumber {
        match self {
            ChainManager::Multi(manager) => {
                let round = manager.round();
                round.try_add_one().unwrap_or(round)
            }
            ChainManager::None | ChainManager::Single(_) => RoundNumber::default(),
        }
    }

    pub fn pending(&self) -> Option<&Vote> {
        match self {
            ChainManager::Single(manager) => manager.pending(),
            ChainManager::Multi(manager) => manager.pending(),
            ChainManager::None => None,
        }
    }

    /// Verifies the safety of the block w.r.t. voting rules.
    pub fn check_proposed_block(&self, proposal: &BlockProposal) -> Result<Outcome, ChainError> {
        // When a block is certified, incrementing its height must succeed.
        ensure!(
            proposal.content.block.height < BlockHeight::max(),
            ChainError::InvalidBlockHeight
        );
        match self {
            ChainManager::Single(manager) => manager.check_proposed_block(proposal),
            ChainManager::Multi(manager) => manager.check_proposed_block(proposal),
            ChainManager::None => panic!("unexpected chain manager"),
        }
    }

    pub fn check_validated_block(&self, certificate: &Certificate) -> Result<Outcome, ChainError> {
        match self {
            ChainManager::Multi(manager) => manager.check_validated_block(certificate),
            ChainManager::None | ChainManager::Single(_) => panic!("unexpected chain manager"),
        }
    }

    pub fn create_vote(
        &mut self,
        proposal: BlockProposal,
        messages: Vec<OutgoingMessage>,
        state_hash: CryptoHash,
        key_pair: Option<&KeyPair>,
    ) {
        match self {
            ChainManager::Single(manager) => {
                manager.create_vote(proposal, messages, state_hash, key_pair)
            }
            ChainManager::Multi(manager) => {
                manager.create_vote(proposal, messages, state_hash, key_pair)
            }
            ChainManager::None => panic!("unexpected chain manager"),
        }
    }

    pub fn create_final_vote(&mut self, certificate: Certificate, key_pair: Option<&KeyPair>) {
        match self {
            ChainManager::Multi(manager) => manager.create_final_vote(certificate, key_pair),
            ChainManager::None | ChainManager::Single(_) => panic!("unexpected chain manager"),
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

    pub fn highest_validated(&self) -> Option<&Certificate> {
        match self {
            ChainManagerInfo::Multi(multi) => multi.highest_validated(),
            ChainManagerInfo::None | ChainManagerInfo::Single(_) => None,
        }
    }

    pub fn next_round(&self) -> RoundNumber {
        match self {
            ChainManagerInfo::Multi(multi) => multi.next_round(),
            _ => RoundNumber::default(),
        }
    }
}
