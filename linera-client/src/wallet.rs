// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, BTreeSet},
    iter::IntoIterator,
};

use linera_base::{
    crypto::{AccountPublicKey, CryptoHash},
    data_types::{BlockHeight, Timestamp},
    ensure,
    identifiers::{AccountOwner, ChainDescription, ChainId},
};
use linera_core::{
    client::{ChainClient, PendingProposal},
    node::ValidatorNodeProvider,
};
use linera_storage::Storage;
use serde::{Deserialize, Serialize};

use crate::{config::GenesisConfig, error, Error};

#[derive(Serialize, Deserialize)]
pub struct Wallet {
    pub chains: BTreeMap<ChainId, UserChain>,
    pub unassigned_keys: BTreeSet<AccountOwner>,
    pub assigned_keys: BTreeMap<ChainId, AccountOwner>,
    pub default: Option<ChainId>,
    pub genesis_config: GenesisConfig,
}

impl Extend<UserChain> for Wallet {
    fn extend<Chains: IntoIterator<Item = UserChain>>(&mut self, chains: Chains) {
        let mut chains = chains.into_iter();
        if let Some(chain) = chains.next() {
            // Ensures that the default chain gets set appropriately to the first chain,
            // if it isn't already set.
            self.insert(chain);

            self.chains
                .extend(chains.map(|chain| (chain.chain_id, chain)));
        }
    }
}

impl Wallet {
    pub fn new(genesis_config: GenesisConfig) -> Self {
        Wallet {
            chains: BTreeMap::new(),
            unassigned_keys: BTreeSet::new(),
            assigned_keys: BTreeMap::new(),
            default: None,
            genesis_config,
        }
    }

    pub fn get(&self, chain_id: ChainId) -> Option<&UserChain> {
        self.chains.get(&chain_id)
    }

    pub fn insert(&mut self, chain: UserChain) {
        if self.default.is_none() {
            self.default = Some(chain.chain_id);
        }

        self.chains.insert(chain.chain_id, chain);
    }

    pub fn forget_keys(&mut self, chain_id: &ChainId) -> Result<AccountOwner, Error> {
        let chain = self
            .chains
            .get_mut(chain_id)
            .ok_or(error::Inner::NonexistentChain(*chain_id))?;

        let owner = chain
            .owner
            .take()
            .ok_or(error::Inner::NonexistentKeypair(*chain_id))?;

        self.assigned_keys.remove(chain_id);

        Ok(owner)
    }

    pub fn forget_chain(&mut self, chain_id: &ChainId) -> Result<UserChain, Error> {
        let user_chain = self
            .chains
            .remove(chain_id)
            .ok_or::<Error>(error::Inner::NonexistentChain(*chain_id).into())?;
        self.assigned_keys.remove(chain_id);
        Ok(user_chain)
    }

    pub fn default_chain(&self) -> Option<ChainId> {
        self.default
    }

    pub fn chain_ids(&self) -> Vec<ChainId> {
        self.chains.keys().copied().collect()
    }

    /// Returns the list of all chain IDs for which we have a secret key.
    pub fn owned_chain_ids(&self) -> Vec<ChainId> {
        self.chains
            .iter()
            .filter_map(|(chain_id, chain)| chain.owner.is_some().then_some(*chain_id))
            .collect()
    }

    pub fn num_chains(&self) -> usize {
        self.chains.len()
    }

    pub fn last_chain(&self) -> Option<&UserChain> {
        self.chains.values().last()
    }

    pub fn chains_mut(&mut self) -> impl Iterator<Item = &mut UserChain> {
        self.chains.values_mut()
    }

    pub fn add_unassigned_key_pair(&mut self, public_key: AccountPublicKey) {
        self.unassigned_keys.insert(AccountOwner::from(public_key));
    }

    pub fn assign_new_chain_to_owner(
        &mut self,
        owner: AccountOwner,
        chain_id: ChainId,
        timestamp: Timestamp,
    ) -> Result<(), Error> {
        if !self.unassigned_keys.remove(&owner) {
            return Err(error::Inner::NonexistentKeypair(chain_id).into());
        }
        let user_chain = UserChain {
            chain_id,
            owner: Some(owner),
            block_hash: None,
            timestamp,
            next_block_height: BlockHeight(0),
            pending_proposal: None,
        };
        self.assigned_keys.insert(chain_id, owner);
        self.insert(user_chain);
        Ok(())
    }

    pub fn set_default_chain(&mut self, chain_id: ChainId) -> Result<(), Error> {
        ensure!(
            self.chains.contains_key(&chain_id),
            error::Inner::NonexistentChain(chain_id)
        );
        self.default = Some(chain_id);
        Ok(())
    }

    pub async fn update_from_state<P, S>(
        &mut self,
        chain_client: &ChainClient<P, S>,
    ) -> Result<(), Error>
    where
        P: ValidatorNodeProvider + Sync + 'static,
        S: Storage + Clone + Send + Sync + 'static,
    {
        let owner = match self.assigned_keys.get(&chain_client.chain_id()) {
            None => None,
            Some(preferred_owner) => {
                let client_owner = chain_client.identity().await?;
                assert_eq!(Some(*preferred_owner), client_owner);
                client_owner
            }
        };

        let state = chain_client.state();
        self.chains.insert(
            chain_client.chain_id(),
            UserChain {
                chain_id: chain_client.chain_id(),
                owner,
                block_hash: state.block_hash(),
                next_block_height: state.next_block_height(),
                timestamp: state.timestamp(),
                pending_proposal: state.pending_proposal().clone(),
            },
        );
        Ok(())
    }

    pub fn genesis_admin_chain(&self) -> ChainId {
        self.genesis_config.admin_id
    }

    pub fn genesis_config(&self) -> &GenesisConfig {
        &self.genesis_config
    }
}

#[derive(Serialize, Deserialize)]
pub struct UserChain {
    pub chain_id: ChainId,
    /// The owner of the chain, if we own it.
    pub owner: Option<AccountOwner>,
    pub block_hash: Option<CryptoHash>,
    pub timestamp: Timestamp,
    pub next_block_height: BlockHeight,
    pub pending_proposal: Option<PendingProposal>,
}

impl Clone for UserChain {
    fn clone(&self) -> Self {
        Self {
            chain_id: self.chain_id,
            owner: self.owner,
            block_hash: self.block_hash,
            timestamp: self.timestamp,
            next_block_height: self.next_block_height,
            pending_proposal: self.pending_proposal.clone(),
        }
    }
}

impl UserChain {
    /// Create a user chain that we own.
    pub fn make_initial(
        owner: AccountOwner,
        description: ChainDescription,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            chain_id: description.into(),
            owner: Some(owner),
            block_hash: None,
            timestamp,
            next_block_height: BlockHeight::ZERO,
            pending_proposal: None,
        }
    }

    /// Creates an entry for a chain that we don't own. The timestamp must be the genesis
    /// timestamp or earlier.
    pub fn make_other(chain_id: ChainId, timestamp: Timestamp) -> Self {
        Self {
            chain_id,
            owner: None,
            block_hash: None,
            timestamp,
            next_block_height: BlockHeight::ZERO,
            pending_proposal: None,
        }
    }
}
