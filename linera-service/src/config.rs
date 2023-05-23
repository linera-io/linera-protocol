// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, bail};
use comfy_table::{
    modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL, Attribute, Cell, Color, ContentArrangement,
    Table,
};
use file_lock::{FileLock, FileOptions};
use linera_base::{
    crypto::{CryptoHash, KeyPair, PublicKey},
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{ChainDescription, ChainId, Owner},
};
use linera_core::client::{ChainClient, ValidatorNodeProvider};
use linera_execution::{
    committee::{Committee, ValidatorName, ValidatorState},
    pricing::Pricing,
};
use linera_rpc::config::{ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig};
use linera_storage::Store;
use linera_views::views::ViewError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
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
    pub fn into_committee(self, pricing: Pricing) -> Committee {
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
        Committee::new(validators, pricing)
    }
}

#[derive(Serialize, Deserialize)]
pub struct UserChain {
    pub chain_id: ChainId,
    pub key_pair: Option<KeyPair>,
    pub block_hash: Option<CryptoHash>,
    pub timestamp: Timestamp,
    pub next_block_height: BlockHeight,
}

impl UserChain {
    pub fn make_initial(description: ChainDescription, timestamp: Timestamp) -> Self {
        let key_pair = KeyPair::generate();
        Self {
            chain_id: description.into(),
            key_pair: Some(key_pair),
            block_hash: None,
            timestamp,
            next_block_height: BlockHeight::from(0),
        }
    }
}

/// A wrapper around `InnerWalletState` which owns a [`FileLock`] to prevent
/// two processes accessing it at the same time.
pub struct WalletState {
    inner: InnerWallet,
    wallet_path: PathBuf,
    _lock: FileLock,
}

#[derive(Serialize, Deserialize)]
struct InnerWallet {
    chains: BTreeMap<ChainId, UserChain>,
    unassigned_key_pairs: HashMap<PublicKey, KeyPair>,
    default: Option<ChainId>,
    genesis_config: GenesisConfig,
}

impl WalletState {
    pub fn get(&self, chain_id: ChainId) -> Option<&UserChain> {
        self.inner.chains.get(&chain_id)
    }

    pub fn insert(&mut self, chain: UserChain) {
        if self.inner.chains.is_empty() {
            self.inner.default = Some(chain.chain_id);
        }
        self.inner.chains.insert(chain.chain_id, chain);
    }

    pub fn default_chain(&self) -> Option<ChainId> {
        self.inner.default
    }

    pub fn chain_ids(&self) -> Vec<ChainId> {
        self.inner.chains.keys().copied().collect()
    }

    /// Returns the list of all chain IDs for which we have a secret key.
    pub fn own_chain_ids(&self) -> Vec<ChainId> {
        self.inner
            .chains
            .iter()
            .filter_map(|(chain_id, chain)| chain.key_pair.is_some().then_some(*chain_id))
            .collect()
    }

    pub fn num_chains(&self) -> usize {
        self.inner.chains.len()
    }

    pub fn last_chain(&mut self) -> Option<&UserChain> {
        self.inner.chains.values().last()
    }

    pub fn chains_mut(&mut self) -> impl Iterator<Item = &mut UserChain> {
        self.inner.chains.values_mut()
    }

    pub fn add_unassigned_key_pair(&mut self, keypair: KeyPair) {
        self.inner
            .unassigned_key_pairs
            .insert(keypair.public(), keypair);
    }

    pub fn key_pair_for_pk(&self, key: &PublicKey) -> Option<KeyPair> {
        self.inner
            .unassigned_key_pairs
            .get(key)
            .map(|key_pair| key_pair.copy())
    }

    pub fn assign_new_chain_to_key(
        &mut self,
        key: PublicKey,
        chain_id: ChainId,
        timestamp: Timestamp,
    ) -> Result<(), anyhow::Error> {
        let key_pair = self
            .inner
            .unassigned_key_pairs
            .remove(&key)
            .ok_or_else(|| {
                anyhow!("could not assign chain to key as unassigned key was not found")
            })?;
        let user_chain = UserChain {
            chain_id,
            key_pair: Some(key_pair),
            block_hash: None,
            timestamp,
            next_block_height: BlockHeight(0),
        };
        self.insert(user_chain);
        Ok(())
    }

    pub fn set_default_chain(&mut self, chain_id: ChainId) -> Result<(), anyhow::Error> {
        if !self.inner.chains.contains_key(&chain_id) {
            bail!("Chain {} cannot be assigned as the default chain since it does not exist in the wallet.", &chain_id);
        }
        self.inner.default = Some(chain_id);
        Ok(())
    }

    pub async fn update_from_state<P, S>(&mut self, state: &mut ChainClient<P, S>)
    where
        P: ValidatorNodeProvider + Sync + 'static,
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        self.inner.chains.insert(
            state.chain_id(),
            UserChain {
                chain_id: state.chain_id(),
                key_pair: state.key_pair().await.map(|k| k.copy()).ok(),
                block_hash: state.block_hash(),
                next_block_height: state.next_block_height(),
                timestamp: state.timestamp(),
            },
        );
    }

    pub fn genesis_admin_chain(&self) -> ChainId {
        self.inner.genesis_config.admin_id
    }

    pub fn genesis_config(&self) -> &GenesisConfig {
        &self.inner.genesis_config
    }

    pub fn from_file(path: &Path) -> Result<Self, anyhow::Error> {
        let file = FileOptions::new().read(true).write(true);
        let block = false;
        let file_lock = match FileLock::lock(path, block, file) {
            Ok(lock) => lock,
            Err(err) => bail!("Error getting write lock to wallet: {}", err),
        };
        let inner = serde_json::from_reader(BufReader::new(&file_lock.file))?;
        Ok(Self {
            inner,
            wallet_path: path.into(),
            _lock: file_lock,
        })
    }

    pub fn create(path: &Path, genesis_config: GenesisConfig) -> Result<Self, anyhow::Error> {
        let file = FileOptions::new().create(true).write(true).read(true);
        let block = false;
        let file_lock = match FileLock::lock(path, block, file) {
            Ok(lock) => lock,
            Err(err) => bail!("Error getting write lock to wallet: {}", err),
        };
        let mut reader = BufReader::new(&file_lock.file);
        if reader.fill_buf()?.is_empty() {
            let inner = InnerWallet {
                chains: Default::default(),
                unassigned_key_pairs: Default::default(),
                default: None,
                genesis_config,
            };
            Ok(Self {
                inner,
                wallet_path: path.into(),
                _lock: file_lock,
            })
        } else {
            let inner = serde_json::from_reader(reader)?;
            Ok(Self {
                inner,
                wallet_path: path.into(),
                _lock: file_lock,
            })
        }
    }

    /// Writes the wallet to disk.
    ///
    /// The contents of the wallet need to be over-written completely, so
    /// a temporary file is created as a backup in case a crash occurs while
    /// writing to disk.
    ///
    /// The temporary file is then renamed to the original wallet name. If
    /// serialization or writing to disk fails, the temporary filed is
    /// deleted.
    pub fn write(&mut self) -> Result<(), anyhow::Error> {
        let mut temp_file_path = self.wallet_path.clone();
        temp_file_path.set_extension("json.bak");
        let backup_file = File::create(&temp_file_path)?;
        let mut temp_file_writer = BufWriter::new(backup_file);
        if let Err(e) = serde_json::to_writer_pretty(&mut temp_file_writer, &self.inner) {
            fs::remove_file(&temp_file_path)?;
            bail!("failed to serialize the wallet state: {}", e)
        }
        if let Err(e) = temp_file_writer.flush() {
            fs::remove_file(&temp_file_path)?;
            bail!("failed to write the wallet state: {}", e);
        }
        fs::rename(&temp_file_path, &self.wallet_path)?;
        Ok(())
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
            let user_chain = self.inner.chains.get(&chain_id).unwrap();
            Self::update_table_with_chain(
                &mut table,
                chain_id,
                user_chain,
                Some(chain_id) == self.inner.default,
            );
        } else {
            for (chain_id, user_chain) in &self.inner.chains {
                Self::update_table_with_chain(
                    &mut table,
                    *chain_id,
                    user_chain,
                    Some(chain_id) == self.inner.default.as_ref(),
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenesisConfig {
    pub committee: CommitteeConfig,
    pub admin_id: ChainId,
    pub chains: Vec<(ChainDescription, PublicKey, Amount, Timestamp)>,
    pub pricing: Pricing,
}

impl Import for GenesisConfig {}
impl Export for GenesisConfig {}

impl GenesisConfig {
    pub fn new(committee: CommitteeConfig, admin_id: ChainId, pricing: Pricing) -> Self {
        Self {
            committee,
            admin_id,
            chains: Vec::new(),
            pricing,
        }
    }

    pub async fn initialize_store<S>(&self, store: &mut S) -> Result<(), anyhow::Error>
    where
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        for (description, public_key, balance, timestamp) in &self.chains {
            store
                .create_chain(
                    self.create_committee(),
                    self.admin_id,
                    *description,
                    *public_key,
                    *balance,
                    *timestamp,
                )
                .await?;
        }
        Ok(())
    }

    pub fn create_committee(&self) -> Committee {
        self.committee.clone().into_committee(self.pricing.clone())
    }
}
