// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashMap},
    iter::IntoIterator,
};

use anyhow::Context as _;
use comfy_table::{
    modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL, Attribute, Cell, Color, ContentArrangement,
    Table,
};
use linera_base::{
    crypto::{CryptoHash, CryptoRng, KeyPair, PublicKey},
    data_types::{BlockHeight, HashedBlob, Timestamp},
    identifiers::{BlobId, ChainDescription, ChainId, Owner},
};
use linera_chain::data_types::Block;
use linera_core::{client::ChainClient, node::ValidatorNodeProvider};
use linera_storage::Storage;
use linera_views::views::ViewError;
use rand::Rng as _;
use serde::{Deserialize, Serialize};

use crate::config::GenesisConfig;

#[derive(Serialize, Deserialize)]
pub struct Wallet {
    chains: BTreeMap<ChainId, UserChain>,
    unassigned_key_pairs: HashMap<PublicKey, KeyPair>,
    default: Option<ChainId>,
    genesis_config: GenesisConfig,
    testing_prng_seed: Option<u64>,
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

    pub fn forget_keys(&mut self, chain_id: &ChainId) -> Result<KeyPair, anyhow::Error> {
        let chain = self
            .chains
            .get_mut(chain_id)
            .context(format!("Failed to get chain for chain id: {}", chain_id))?;
        chain.key_pair.take().context("Failed to take keypair")
    }

    pub fn forget_chain(&mut self, chain_id: &ChainId) -> Result<UserChain, anyhow::Error> {
        self.chains
            .remove(chain_id)
            .context(format!("Failed to remove chain: {}", chain_id))
    }

    pub fn default_chain(&self) -> Option<ChainId> {
        self.default
    }

    pub fn chain_ids(&self) -> Vec<ChainId> {
        self.chains.keys().copied().collect()
    }

    /// Returns the list of all chain IDs for which we have a secret key.
    pub fn own_chain_ids(&self) -> Vec<ChainId> {
        self.chains
            .iter()
            .filter_map(|(chain_id, chain)| chain.key_pair.is_some().then_some(*chain_id))
            .collect()
    }

    pub fn num_chains(&self) -> usize {
        self.chains.len()
    }

    pub fn last_chain(&mut self) -> Option<&UserChain> {
        self.chains.values().last()
    }

    pub fn chains_mut(&mut self) -> impl Iterator<Item = &mut UserChain> {
        self.chains.values_mut()
    }

    pub fn add_unassigned_key_pair(&mut self, keypair: KeyPair) {
        self.unassigned_key_pairs.insert(keypair.public(), keypair);
    }

    pub fn key_pair_for_pk(&self, key: &PublicKey) -> Option<KeyPair> {
        if let Some(key_pair) = self
            .unassigned_key_pairs
            .get(key)
            .map(|key_pair| key_pair.copy())
        {
            return Some(key_pair);
        }
        self.chains
            .values()
            .filter_map(|user_chain| user_chain.key_pair.as_ref())
            .find(|key_pair| key_pair.public() == *key)
            .map(|key_pair| key_pair.copy())
    }

    pub fn assign_new_chain_to_key(
        &mut self,
        key: PublicKey,
        chain_id: ChainId,
        timestamp: Timestamp,
    ) -> Result<(), anyhow::Error> {
        let key_pair = self
            .unassigned_key_pairs
            .remove(&key)
            .context("could not assign chain to key as unassigned key was not found")?;
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

    pub fn set_default_chain(&mut self, chain_id: ChainId) -> Result<(), anyhow::Error> {
        anyhow::ensure!(
            self.chains.contains_key(&chain_id),
            "Chain {} cannot be assigned as the default chain since it does not exist in the \
             wallet.",
            &chain_id
        );
        self.default = Some(chain_id);
        Ok(())
    }

    pub async fn update_from_state<P, S>(&mut self, state: &mut ChainClient<P, S>)
    where
        P: ValidatorNodeProvider + Sync + 'static,
        S: Storage + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        self.chains.insert(
            state.chain_id(),
            UserChain {
                chain_id: state.chain_id(),
                key_pair: state.key_pair().await.map(|k| k.copy()).ok(),
                block_hash: state.block_hash(),
                next_block_height: state.next_block_height(),
                timestamp: state.timestamp(),
                pending_block: state.pending_block().clone(),
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

    pub fn pretty_print(&self, chain_id: Option<ChainId>) {
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .apply_modifier(UTF8_ROUND_CORNERS)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_header(vec![
                Cell::new("Chain Id").add_attribute(Attribute::Bold),
                Cell::new("Latest Block").add_attribute(Attribute::Bold),
            ]);
        if let Some(chain_id) = chain_id {
            if let Some(user_chain) = self.chains.get(&chain_id) {
                Self::update_table_with_chain(
                    &mut table,
                    chain_id,
                    user_chain,
                    Some(chain_id) == self.default,
                );
            } else {
                panic!("Chain {} not found.", chain_id);
            }
        } else {
            for (chain_id, user_chain) in &self.chains {
                Self::update_table_with_chain(
                    &mut table,
                    *chain_id,
                    user_chain,
                    Some(chain_id) == self.default.as_ref(),
                );
            }
        }
        println!("{}", table);
    }

    fn update_table_with_chain(
        table: &mut Table,
        chain_id: ChainId,
        user_chain: &UserChain,
        is_default_chain: bool,
    ) {
        let chain_id_cell = if is_default_chain {
            Cell::new(format!("{}", chain_id)).fg(Color::Green)
        } else {
            Cell::new(format!("{}", chain_id))
        };
        table.add_row(vec![
            chain_id_cell,
            Cell::new(format!(
                r#"Public Key:         {}
Owner:              {}
Block Hash:         {}
Timestamp:          {}
Next Block Height:  {}"#,
                user_chain
                    .key_pair
                    .as_ref()
                    .map(|kp| kp.public().to_string())
                    .unwrap_or_else(|| "-".to_string()),
                user_chain
                    .key_pair
                    .as_ref()
                    .map(|kp| Owner::from(kp.public()))
                    .map(|o| o.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                user_chain
                    .block_hash
                    .map(|bh| bh.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                user_chain.timestamp,
                user_chain.next_block_height
            )),
        ]);
    }
}

#[derive(Serialize, Deserialize)]
pub struct UserChain {
    pub chain_id: ChainId,
    pub key_pair: Option<KeyPair>,
    pub block_hash: Option<CryptoHash>,
    pub timestamp: Timestamp,
    pub next_block_height: BlockHeight,
    pub pending_block: Option<Block>,
    pub pending_blobs: BTreeMap<BlobId, HashedBlob>,
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
