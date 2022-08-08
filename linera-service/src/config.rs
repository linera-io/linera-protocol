// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::network::{ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig};
use linera_base::{
    chain::ChainState,
    committee::{Committee, ValidatorState},
    crypto::*,
    messages::{BlockHeight, ChainDescription, ChainId, Owner, ValidatorName},
    system::Balance,
};
use linera_core::{
    client::{ChainClientState, ValidatorNodeProvider},
    node::ValidatorNode,
};
use linera_storage::Storage;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{BufReader, BufWriter, Write},
    path::Path,
};

pub trait Import: DeserializeOwned {
    fn read(path: &Path) -> Result<Self, std::io::Error> {
        let data = fs::read(path)?;
        Ok(serde_json::from_slice(data.as_slice())?)
    }
}

pub trait Export: Serialize {
    fn write(&self, path: &Path) -> Result<(), std::io::Error> {
        let file = OpenOptions::new().create(true).write(true).open(path)?;
        let mut writer = BufWriter::new(file);
        let data = serde_json::to_string_pretty(self).unwrap();
        writer.write_all(data.as_ref())?;
        writer.write_all(b"\n")?;
        Ok(())
    }
}

/// The public configuration of a validator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidatorConfig {
    /// The public key of the validator.
    pub name: ValidatorName,
    /// The network configuration for the validator.
    pub network: ValidatorPublicNetworkConfig,
}

/// The private configuration of a validator service.
#[derive(Serialize, Deserialize)]
pub struct ValidatorServerConfig {
    pub validator: ValidatorConfig,
    pub key: KeyPair,
    pub internal_network: ValidatorInternalNetworkConfig,
}

impl Import for ValidatorServerConfig {}
impl Export for ValidatorServerConfig {}

/// The (public) configuration for all validators.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct CommitteeConfig {
    pub validators: Vec<ValidatorConfig>,
}

impl Import for CommitteeConfig {}
impl Export for CommitteeConfig {}

impl CommitteeConfig {
    pub fn into_committee(self) -> Committee {
        let validators = self
            .validators
            .into_iter()
            .map(|v| {
                (
                    v.name,
                    ValidatorState {
                        network_address: v.network.to_string(),
                        votes: 1,
                    },
                )
            })
            .collect();
        Committee::new(validators)
    }
}

#[derive(Serialize, Deserialize)]
pub struct UserChain {
    pub chain_id: ChainId,
    pub key_pair: Option<KeyPair>,
    pub block_hash: Option<HashValue>,
    pub next_block_height: BlockHeight,
}

impl UserChain {
    pub fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            key_pair: None,
            block_hash: None,
            next_block_height: BlockHeight::from(0),
        }
    }

    pub fn make_initial(description: ChainDescription) -> Self {
        let key_pair = KeyPair::generate();
        Self {
            chain_id: description.into(),
            key_pair: Some(key_pair),
            block_hash: None,
            next_block_height: BlockHeight::from(0),
        }
    }
}

pub struct WalletState {
    chains: BTreeMap<ChainId, UserChain>,
}

impl WalletState {
    pub fn get(&self, chain_id: ChainId) -> Option<&UserChain> {
        self.chains.get(&chain_id)
    }

    pub fn get_or_insert(&mut self, chain_id: ChainId) -> &UserChain {
        self.chains
            .entry(chain_id)
            .or_insert_with(|| UserChain::new(chain_id))
    }

    pub fn insert(&mut self, chain: UserChain) {
        self.chains.insert(chain.chain_id, chain);
    }

    pub fn chain_ids(&self) -> Vec<ChainId> {
        self.chains.keys().copied().collect()
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

    pub async fn update_from_state<P, S>(&mut self, state: &mut ChainClientState<P, S>)
    where
        P: ValidatorNodeProvider + Send + 'static,
        P::Node: ValidatorNode + Send + Sync + 'static + Clone,
        S: Storage + Clone + Send + Sync + 'static,
    {
        let chain = self
            .chains
            .entry(state.chain_id())
            .or_insert_with(|| UserChain::new(state.chain_id()));
        chain.key_pair = state.key_pair().await.map(|k| k.copy()).ok();
        chain.block_hash = state.block_hash();
        chain.next_block_height = state.next_block_height();
    }

    pub fn read_or_create(path: &Path) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        let reader = BufReader::new(file);
        let stream = serde_json::Deserializer::from_reader(reader).into_iter();
        Ok(Self {
            chains: stream
                .filter_map(Result::ok)
                .map(|chain: UserChain| (chain.chain_id, chain))
                .collect(),
        })
    }

    pub fn write(&self, path: &Path) -> Result<(), std::io::Error> {
        let file = OpenOptions::new().write(true).open(path)?;
        let mut writer = BufWriter::new(file);
        for chain in self.chains.values() {
            serde_json::to_writer(&mut writer, chain)?;
            writer.write_all(b"\n")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenesisConfig {
    pub committee: CommitteeConfig,
    pub admin_id: ChainId,
    pub chains: Vec<(ChainDescription, Owner, Balance)>,
}

impl Import for GenesisConfig {}
impl Export for GenesisConfig {}

impl GenesisConfig {
    pub fn new(committee: CommitteeConfig, admin_id: ChainId) -> Self {
        Self {
            committee,
            admin_id,
            chains: Vec::new(),
        }
    }

    pub async fn initialize_store<S>(&self, store: &mut S) -> Result<(), anyhow::Error>
    where
        S: Storage + Clone + 'static,
    {
        for (description, owner, balance) in &self.chains {
            let chain = ChainState::create(
                self.committee.clone().into_committee(),
                self.admin_id,
                *description,
                *owner,
                *balance,
            );
            store.write_chain(chain.clone()).await?;
        }
        Ok(())
    }
}
