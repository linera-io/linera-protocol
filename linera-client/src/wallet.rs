// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashMap},
    iter::IntoIterator,
};

use linera_base::{
    crypto::{CryptoHash, CryptoRng, KeyPair},
    data_types::{Blob, BlockHeight, Timestamp},
    ensure,
    identifiers::{BlobId, ChainDescription, ChainId, Owner},
};
use linera_chain::data_types::ProposedBlock;
use linera_core::{client::ChainClient, node::ValidatorNodeProvider};
use linera_storage::Storage;
use rand::Rng as _;
use serde::{Deserialize, Serialize};

use crate::{config::GenesisConfig, error, Error};

#[derive(Serialize, Deserialize)]
pub struct Wallet {
    pub chains: BTreeMap<ChainId, UserChain>,
    pub unassigned_key_pairs: HashMap<Owner, KeyPair>,
    pub default: Option<ChainId>,
    pub genesis_config: GenesisConfig,
    pub testing_prng_seed: Option<u64>,
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
    pub fn new(genesis_config: GenesisConfig, testing_prng_seed: Option<u64>) -> Self {
        Wallet {
            chains: BTreeMap::new(),
            unassigned_key_pairs: HashMap::new(),
            default: None,
            genesis_config,
            testing_prng_seed,
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

    pub fn forget_keys(&mut self, chain_id: &ChainId) -> Result<KeyPair, Error> {
        let chain = self
            .chains
            .get_mut(chain_id)
            .ok_or(error::Inner::NonexistentChain(*chain_id))?;
        chain
            .key_pair
            .take()
            .ok_or(error::Inner::NonexistentKeypair(*chain_id).into())
    }

    pub fn forget_chain(&mut self, chain_id: &ChainId) -> Result<UserChain, Error> {
        self.chains
            .remove(chain_id)
            .ok_or(error::Inner::NonexistentChain(*chain_id).into())
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
            .filter_map(|(chain_id, chain)| chain.key_pair.is_some().then_some(*chain_id))
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

    pub fn add_unassigned_key_pair(&mut self, key_pair: KeyPair) {
        let owner = key_pair.public().into();
        self.unassigned_key_pairs.insert(owner, key_pair);
    }

    pub fn key_pair_for_owner(&self, owner: &Owner) -> Option<KeyPair> {
        if let Some(key_pair) = self
            .unassigned_key_pairs
            .get(owner)
            .map(|key_pair| key_pair.copy())
        {
            return Some(key_pair);
        }
        self.chains
            .values()
            .filter_map(|user_chain| user_chain.key_pair.as_ref())
            .find(|key_pair| Owner::from(key_pair.public()) == *owner)
            .map(|key_pair| key_pair.copy())
    }

    pub fn assign_new_chain_to_owner(
        &mut self,
        owner: Owner,
        chain_id: ChainId,
        timestamp: Timestamp,
    ) -> Result<(), Error> {
        let key_pair = self
            .unassigned_key_pairs
            .remove(&owner)
            .ok_or(error::Inner::NonexistentKeypair(chain_id))?;
        let user_chain = UserChain {
            chain_id,
            key_pair: Some(key_pair),
            block_hash: None,
            timestamp,
            next_block_height: BlockHeight(0),
            pending_block: None,
            pending_blobs: BTreeMap::new(),
        };
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

    pub async fn update_from_state<P, S>(&mut self, chain_client: &ChainClient<P, S>)
    where
        P: ValidatorNodeProvider + Sync + 'static,
        S: Storage + Clone + Send + Sync + 'static,
    {
        let key_pair = chain_client.key_pair().await.map(|k| k.copy()).ok();
        let state = chain_client.state();
        self.chains.insert(
            chain_client.chain_id(),
            UserChain {
                chain_id: chain_client.chain_id(),
                key_pair,
                block_hash: state.block_hash(),
                next_block_height: state.next_block_height(),
                timestamp: state.timestamp(),
                pending_block: state.pending_proposal().clone(),
                pending_blobs: state.pending_blobs().clone(),
            },
        );
    }

    pub fn genesis_admin_chain(&self) -> ChainId {
        self.genesis_config.admin_id
    }

    pub fn genesis_config(&self) -> &GenesisConfig {
        &self.genesis_config
    }

    pub fn make_prng(&self) -> Box<dyn CryptoRng> {
        self.testing_prng_seed.into()
    }

    pub fn refresh_prng_seed<R: CryptoRng>(&mut self, rng: &mut R) {
        if self.testing_prng_seed.is_some() {
            self.testing_prng_seed = Some(rng.gen());
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct UserChain {
    pub chain_id: ChainId,
    pub key_pair: Option<KeyPair>,
    pub block_hash: Option<CryptoHash>,
    pub timestamp: Timestamp,
    pub next_block_height: BlockHeight,
    pub pending_block: Option<ProposedBlock>,
    pub pending_blobs: BTreeMap<BlobId, Blob>,
}

impl UserChain {
    /// Create a user chain that we own.
    pub fn make_initial<R: CryptoRng>(
        rng: &mut R,
        description: ChainDescription,
        timestamp: Timestamp,
    ) -> Self {
        let key_pair = KeyPair::generate_from(rng);
        Self {
            chain_id: description.into(),
            key_pair: Some(key_pair),
            block_hash: None,
            timestamp,
            next_block_height: BlockHeight::ZERO,
            pending_block: None,
            pending_blobs: BTreeMap::new(),
        }
    }

    /// Creates an entry for a chain that we don't own. The timestamp must be the genesis
    /// timestamp or earlier.
    pub fn make_other(chain_id: ChainId, timestamp: Timestamp) -> Self {
        Self {
            chain_id,
            key_pair: None,
            block_hash: None,
            timestamp,
            next_block_height: BlockHeight::ZERO,
            pending_block: None,
            pending_blobs: BTreeMap::new(),
        }
    }
}
