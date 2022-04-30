// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::transport::NetworkProtocol;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{BufReader, BufWriter, Write},
    path::Path,
};
use zef_base::{base_types::*, chain::ChainState, committee::Committee};
use zef_core::{client::ChainClientState, node::ValidatorNode};
use zef_storage::Storage;

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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidatorConfig {
    pub network_protocol: NetworkProtocol,
    pub name: ValidatorName,
    pub host: String,
    pub base_port: u32,
    pub num_shards: u32,
}

impl ValidatorConfig {
    pub fn print(&self) {
        let data = serde_json::to_string(self).unwrap();
        println!("{}", data);
    }
}

#[derive(Serialize, Deserialize)]
pub struct ValidatorServerConfig {
    pub validator: ValidatorConfig,
    pub key: KeyPair,
}

impl Import for ValidatorServerConfig {}
impl Export for ValidatorServerConfig {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CommitteeConfig {
    pub validators: Vec<ValidatorConfig>,
}

impl Import for CommitteeConfig {}
impl Export for CommitteeConfig {}

impl CommitteeConfig {
    pub fn into_committee(self) -> Committee {
        Committee::new(self.voting_rights())
    }

    fn voting_rights(&self) -> BTreeMap<ValidatorName, usize> {
        let mut map = BTreeMap::new();
        for validator in &self.validators {
            map.insert(validator.name, 1);
        }
        map
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
            next_block_height: BlockHeight::new(),
        }
    }

    pub fn make_initial(chain_id: ChainId) -> Self {
        let key_pair = KeyPair::generate();
        Self {
            chain_id,
            key_pair: Some(key_pair),
            block_hash: None,
            next_block_height: BlockHeight::new(),
        }
    }
}

pub struct WalletState {
    chains: BTreeMap<ChainId, UserChain>,
}

impl WalletState {
    pub fn get(&self, chain_id: &ChainId) -> Option<&UserChain> {
        self.chains.get(chain_id)
    }

    pub fn get_or_insert(&mut self, chain_id: ChainId) -> &UserChain {
        self.chains
            .entry(chain_id.clone())
            .or_insert_with(|| UserChain::new(chain_id))
    }

    pub fn insert(&mut self, chain: UserChain) {
        self.chains.insert(chain.chain_id.clone(), chain);
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

    pub async fn update_from_state<A, S>(&mut self, state: &mut ChainClientState<A, S>)
    where
        A: ValidatorNode + Send + Sync + 'static + Clone,
        S: Storage + Clone + 'static,
    {
        let chain = self
            .chains
            .entry(state.chain_id().clone())
            .or_insert_with(|| UserChain::new(state.chain_id().clone()));
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
                .map(|chain: UserChain| (chain.chain_id.clone(), chain))
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

#[derive(Serialize, Deserialize)]
pub struct GenesisConfig {
    pub committee: CommitteeConfig,
    pub chains: Vec<(ChainId, Owner, Balance)>,
}

impl Import for GenesisConfig {}
impl Export for GenesisConfig {}

impl GenesisConfig {
    pub fn new(committee: CommitteeConfig) -> Self {
        Self {
            committee,
            chains: Vec::new(),
        }
    }

    pub async fn initialize_store<S>(&self, store: &mut S) -> Result<(), failure::Error>
    where
        S: Storage + Clone + 'static,
    {
        for (chain_id, owner, balance) in &self.chains {
            let chain = ChainState::create(
                self.committee.clone().into_committee(),
                chain_id.clone(),
                *owner,
                *balance,
            );
            store.write_chain(chain.clone()).await?;
        }
        Ok(())
    }
}
