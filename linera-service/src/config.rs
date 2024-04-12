// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashMap},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context as _};
use comfy_table::{
    modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL, Attribute, Cell, Color, ContentArrangement,
    Table,
};
use fs4::FileExt as _;
use fs_err::{self, File, OpenOptions};
use linera_base::{
    crypto::{BcsSignable, CryptoHash, CryptoRng, KeyPair, PublicKey},
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{ChainDescription, ChainId, Owner},
};
use linera_chain::data_types::Block;
use linera_core::{client::ChainClient, node::ValidatorNodeProvider};
use linera_execution::{
    committee::{Committee, ValidatorName, ValidatorState},
    ResourceControlPolicy,
};
use linera_rpc::config::{ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig};
use linera_storage::Storage;
use linera_views::views::ViewError;
use rand::Rng as _;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub trait Import: DeserializeOwned {
    fn read(path: &Path) -> Result<Self, std::io::Error> {
        let data = fs_err::read(path)?;
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
    pub fn into_committee(self, policy: ResourceControlPolicy) -> Committee {
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
        Committee::new(validators, policy)
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
        }
    }
}

/// A guard that keeps an exclusive lock on a file.
pub struct FileLock {
    file: File,
}

impl FileLock {
    /// Acquires an exclusive lock on a provided `file`, returning a [`FileLock`] which will
    /// release the lock when dropped.
    pub fn new(file: File, path: &Path) -> Result<Self, anyhow::Error> {
        file.file().try_lock_exclusive().with_context(|| {
            format!(
                "Error getting write lock to wallet \"{}\". Please make sure the file exists \
                 and that it is not in use by another process already.",
                path.display()
            )
        })?;

        Ok(FileLock { file })
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        if let Err(error) = self.file.file().unlock() {
            tracing::warn!("Failed to unlock wallet file: {error}");
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
    testing_prng_seed: Option<u64>,
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

    pub fn forget_keys(&mut self, chain_id: &ChainId) -> Result<KeyPair, anyhow::Error> {
        let chain = self
            .inner
            .chains
            .get_mut(chain_id)
            .context(format!("Failed to get chain for chain id: {}", chain_id))?;
        chain.key_pair.take().context("Failed to take keypair")
    }

    pub fn forget_chain(&mut self, chain_id: &ChainId) -> Result<UserChain, anyhow::Error> {
        self.inner
            .chains
            .remove(chain_id)
            .context(format!("Failed to remove chain: {}", chain_id))
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
        if let Some(key_pair) = self
            .inner
            .unassigned_key_pairs
            .get(key)
            .map(|key_pair| key_pair.copy())
        {
            return Some(key_pair);
        }
        self.inner
            .chains
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
            .inner
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
        };
        self.insert(user_chain);
        Ok(())
    }

    pub fn set_default_chain(&mut self, chain_id: ChainId) -> Result<(), anyhow::Error> {
        anyhow::ensure!(
            self.inner.chains.contains_key(&chain_id),
            "Chain {} cannot be assigned as the default chain since it does not exist in the \
             wallet.",
            &chain_id
        );
        self.inner.default = Some(chain_id);
        Ok(())
    }

    pub async fn update_from_state<P, S>(&mut self, state: &mut ChainClient<P, S>)
    where
        P: ValidatorNodeProvider + Sync + 'static,
        S: Storage + Clone + Send + Sync + 'static,
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
                pending_block: state.pending_block().clone(),
            },
        );
    }

    pub fn genesis_admin_chain(&self) -> ChainId {
        self.inner.genesis_config.admin_id
    }

    pub fn genesis_config(&self) -> &GenesisConfig {
        &self.inner.genesis_config
    }

    pub fn make_prng(&self) -> Box<dyn CryptoRng> {
        self.inner.testing_prng_seed.into()
    }

    pub fn refresh_prng_seed<R: CryptoRng>(&mut self, rng: &mut R) {
        if self.inner.testing_prng_seed.is_some() {
            self.inner.testing_prng_seed = Some(rng.gen());
        }
    }

    pub fn from_file(path: &Path) -> Result<Self, anyhow::Error> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let file_lock = FileLock::new(file, path)?;
        let inner = serde_json::from_reader(BufReader::new(&file_lock.file))?;
        Ok(Self {
            inner,
            wallet_path: path.into(),
            _lock: file_lock,
        })
    }

    pub fn create(
        path: &Path,
        genesis_config: GenesisConfig,
        testing_prng_seed: Option<u64>,
    ) -> Result<Self, anyhow::Error> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        let file_lock = FileLock::new(file, path)?;
        let mut reader = BufReader::new(&file_lock.file);
        if reader.fill_buf()?.is_empty() {
            let inner = InnerWallet {
                chains: BTreeMap::new(),
                unassigned_key_pairs: HashMap::new(),
                default: None,
                genesis_config,
                testing_prng_seed,
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
            fs_err::remove_file(&temp_file_path)?;
            bail!("failed to serialize the wallet state: {}", e)
        }
        if let Err(e) = temp_file_writer.flush() {
            fs_err::remove_file(&temp_file_path)?;
            bail!("failed to write the wallet state: {}", e);
        }
        fs_err::rename(&temp_file_path, &self.wallet_path)?;
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
            if let Some(user_chain) = self.inner.chains.get(&chain_id) {
                Self::update_table_with_chain(
                    &mut table,
                    chain_id,
                    user_chain,
                    Some(chain_id) == self.inner.default,
                );
            } else {
                panic!("Chain {} not found.", chain_id);
            }
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
    pub timestamp: Timestamp,
    pub chains: Vec<(PublicKey, Amount)>,
    pub policy: ResourceControlPolicy,
    pub network_name: String,
}

impl Import for GenesisConfig {}
impl Export for GenesisConfig {}
impl BcsSignable for GenesisConfig {}

impl GenesisConfig {
    pub fn new(
        committee: CommitteeConfig,
        admin_id: ChainId,
        timestamp: Timestamp,
        policy: ResourceControlPolicy,
        network_name: String,
    ) -> Self {
        Self {
            committee,
            admin_id,
            timestamp,
            chains: Vec::new(),
            policy,
            network_name,
        }
    }

    pub async fn initialize_storage<S>(&self, storage: &mut S) -> Result<(), anyhow::Error>
    where
        S: Storage + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        let committee = self.create_committee();
        for (chain_number, (public_key, balance)) in (0..).zip(&self.chains) {
            let description = ChainDescription::Root(chain_number);
            storage
                .create_chain(
                    committee.clone(),
                    self.admin_id,
                    description,
                    *public_key,
                    *balance,
                    self.timestamp,
                )
                .await?;
        }
        Ok(())
    }

    pub fn create_committee(&self) -> Committee {
        self.committee.clone().into_committee(self.policy.clone())
    }
}
