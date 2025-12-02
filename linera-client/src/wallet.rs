// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::iter::IntoIterator;

use linera_base::{
    data_types::{BlockHeight, Epoch, Timestamp},
    ensure,
    identifiers::{AccountOwner, ChainId},
};
use linera_core::{client::PendingProposal, data_types::ChainInfo, wallet};
use serde::{Deserialize, Serialize};

use crate::{config::GenesisConfig, error, Error};

#[derive(Serialize, Deserialize)]
pub struct Wallet {
    pub chains: papaya::HashMap<ChainId, wallet::Chain>,
    pub default: Option<ChainId>,
    genesis_config: GenesisConfig,
}

impl Wallet {
    pub fn new(genesis_config: GenesisConfig) -> Self {
        Wallet {
            chains: papaya::HashMap::new(),
            default: None,
            genesis_config,
        }
    }

    fn mutate<R>(&self, chain_id: ChainId, mut mutate: impl FnMut(&mut wallet::Chain) -> R) -> Option<R> {
        use papaya::Operation::*;

        let mut outcome = None;
        self.chains.pin().compute(chain_id, |chain| if let Some((_, chain)) = chain {
            let mut chain = chain.clone();
            outcome = Some(mutate(&mut chain));
            Insert(chain)
        } else {
            Abort(())
        });

        outcome
    }

    pub fn get(&self, chain_id: ChainId) -> Option<wallet::Chain> {
        self.chains.pin().get(&chain_id).cloned()
    }

    pub fn forget_keys(&self, chain_id: ChainId) -> Result<AccountOwner, Error> {
        Ok(self.mutate(chain_id, |chain| chain.owner.take())
            .ok_or(error::Inner::NonexistentChain(chain_id))?
            .ok_or(error::Inner::NonexistentKeypair(chain_id))?)
    }

    pub fn forget_chain(&self, chain_id: ChainId) -> Result<wallet::Chain, Error> {
        Ok(self
           .chains
           .pin()
           .remove(&chain_id)
           .ok_or(error::Inner::NonexistentChain(chain_id))?
           .clone())
    }

    pub fn default_chain(&self) -> Option<ChainId> {
        self.default
    }

    pub fn first_non_admin_chain(&self) -> Option<ChainId> {
        self.chain_ids()
            .into_iter()
            .find(|chain_id| *chain_id != self.genesis_config.admin_id())
    }

    pub fn chain_ids(&self) -> Vec<ChainId> {
        self.chains.pin().keys().copied().collect()
    }

    /// Returns the list of all chain IDs for which we have a secret key.
    pub fn owned_chain_ids(&self) -> Vec<ChainId> {
        self.chains
            .pin()
            .iter()
            .filter_map(|(chain_id, chain)| chain.owner.is_some().then_some(*chain_id))
            .collect()
    }

    pub fn num_chains(&self) -> usize {
        self.chains.len()
    }

    pub fn insert(&mut self, id: ChainId, chain: wallet::Chain) {
        self.chains.pin().insert(id, chain);
        if self.default.is_none() {
            self.default = Some(id);
        }
    }

    pub fn assign_new_chain_to_owner(
        &self,
        owner: AccountOwner,
        chain_id: ChainId,
        timestamp: Timestamp,
        epoch: Epoch,
    ) -> Result<(), Error> {
        self.chains.pin().insert(chain_id, wallet::Chain {
            owner: Some(owner),
            block_hash: None,
            timestamp,
            next_block_height: BlockHeight(0),
            pending_proposal: None,
            epoch: Some(epoch),
        });
        Ok(())
    }

    pub fn set_default_chain(&mut self, chain_id: ChainId) -> Result<(), Error> {
        ensure!(
            self.chains.pin().contains_key(&chain_id),
            error::Inner::NonexistentChain(chain_id)
        );
        self.default = Some(chain_id);
        Ok(())
    }

    pub fn update_from_info(
        &mut self,
        pending_proposal: Option<PendingProposal>,
        owner: Option<AccountOwner>,
        info: &ChainInfo,
    ) {
        self.insert(info.chain_id, wallet::Chain {
            owner,
            block_hash: info.block_hash,
            next_block_height: info.next_block_height,
            timestamp: info.timestamp,
            pending_proposal,
            epoch: Some(info.epoch),
        });
    }

    pub fn genesis_admin_chain(&self) -> ChainId {
        self.genesis_config.admin_id()
    }

    pub fn genesis_config(&self) -> &GenesisConfig {
        &self.genesis_config
    }
}
