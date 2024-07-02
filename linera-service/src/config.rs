// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    iter::IntoIterator,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context as _};
use fs4::FileExt as _;
use fs_err::{self, File, OpenOptions};
use linera_base::{
    crypto::{BcsSignable, CryptoRng, KeyPair, PublicKey},
    data_types::{Amount, Timestamp},
    identifiers::{ChainDescription, ChainId},
};
use linera_execution::{
    committee::{Committee, ValidatorName, ValidatorState},
    ResourceControlPolicy,
};
use linera_rpc::config::{ValidatorInternalNetworkConfig, ValidatorPublicNetworkConfig};
use linera_storage::Storage;
use linera_views::views::ViewError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::wallet::{UserChain, Wallet};

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
                        votes: 100,
                    },
                )
            })
            .collect();
        Committee::new(validators, policy)
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

/// A wrapper around `Wallet` which owns a [`FileLock`] to prevent
/// two processes accessing it at the same time.
pub struct WalletState {
    inner: Wallet,
    prng: Box<dyn CryptoRng>,
    wallet_path: PathBuf,
    _lock: FileLock,
}

impl Extend<UserChain> for WalletState {
    fn extend<Chains: IntoIterator<Item = UserChain>>(&mut self, chains: Chains) {
        self.inner.extend(chains);
    }
}

impl WalletState {
    pub fn inner(&self) -> &Wallet {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut Wallet {
        &mut self.inner
    }

    pub fn into_inner(self) -> Wallet {
        self.inner
    }

    pub fn from_file(path: &Path) -> Result<Self, anyhow::Error> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let file_lock = FileLock::new(file, path)?;
        let inner: Wallet = serde_json::from_reader(BufReader::new(&file_lock.file))?;
        Ok(Self {
            prng: inner.make_prng(),
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
        let file = Self::open_options().read(true).open(path)?;
        let file_lock = FileLock::new(file, path)?;
        let mut reader = BufReader::new(&file_lock.file);
        let inner = if reader.fill_buf()?.is_empty() {
            Wallet::new(genesis_config, testing_prng_seed)
        } else {
            serde_json::from_reader(reader)?
        };

        Ok(Self {
            prng: inner.make_prng(),
            inner,
            wallet_path: path.into(),
            _lock: file_lock,
        })
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
        let backup_file = Self::open_options().open(&temp_file_path)?;
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

    pub fn generate_key_pair(&mut self) -> KeyPair {
        KeyPair::generate_from(&mut self.prng)
    }

    pub fn save(&mut self) -> anyhow::Result<()> {
        self.inner.refresh_prng_seed(&mut self.prng);
        self.write()?;
        tracing::info!("Saved user chain states");
        Ok(())
    }

    pub fn refresh_prng_seed(&mut self) {
        self.inner.refresh_prng_seed(&mut self.prng)
    }

    /// Returns options for opening and writing to the wallet file, creating it if it doesn't
    /// exist. On Unix, this restricts read and write permissions to the current user.
    // TODO(#1924): Implement better key management.
    fn open_options() -> OpenOptions {
        let mut options = OpenOptions::new();
        #[cfg(target_family = "unix")]
        fs_err::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        options.create(true).write(true);
        options
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
