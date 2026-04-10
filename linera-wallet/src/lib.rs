// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    iter::IntoIterator,
    path::Path,
    sync::{Arc, RwLock},
};

use futures::{stream, Stream};
use linera_base::{
    crypto::{AccountPublicKey, AccountSignature, CryptoHash, InMemorySigner, Signer},
    identifiers::{AccountOwner, ChainId},
};
use linera_core::{wallet::*, GenesisConfig};
use linera_persistent::{self as persistent, Persist as _};

/// A persistent keystore backed by a JSON file with exclusive locking.
pub struct Keystore(persistent::File<InMemorySigner>);

impl Signer for Keystore {
    type Error = <InMemorySigner as Signer>::Error;

    async fn sign(
        &self,
        owner: &AccountOwner,
        value: &CryptoHash,
    ) -> Result<AccountSignature, Self::Error> {
        (*self.0).sign(owner, value).await
    }

    async fn contains_key(&self, owner: &AccountOwner) -> Result<bool, Self::Error> {
        (*self.0).contains_key(owner).await
    }
}

impl Keystore {
    /// Reads an existing keystore from disk.
    pub fn read(path: &Path) -> Result<Self, persistent::file::Error> {
        Ok(Self(persistent::File::read(path)?))
    }

    /// Creates a new keystore at `path`. If `testing_prng_seed` is provided,
    /// uses deterministic key generation.
    pub fn create(
        path: &Path,
        testing_prng_seed: Option<u64>,
    ) -> Result<Self, persistent::file::Error> {
        Ok(Self(persistent::File::read_or_create(path, || {
            Ok(InMemorySigner::new(testing_prng_seed))
        })?))
    }

    /// Generates a new key pair, persists the keystore, and returns the public key.
    pub async fn generate_key(&mut self) -> Result<AccountPublicKey, persistent::file::Error> {
        let key = self.0.generate_new();
        self.0.persist().await?;
        Ok(key)
    }

    /// Generates `count` new key pairs, persists the keystore, and returns the public keys.
    pub async fn generate_keys(
        &mut self,
        count: usize,
    ) -> Result<Vec<AccountPublicKey>, persistent::file::Error> {
        let keys: Vec<_> = std::iter::repeat_with(|| self.0.generate_new())
            .take(count)
            .collect();
        self.0.persist().await?;
        Ok(keys)
    }

    /// Saves the keystore to disk.
    pub async fn save(&mut self) -> Result<(), persistent::file::Error> {
        self.0.persist().await
    }

    /// Consumes the keystore and returns the inner signer.
    pub fn into_signer(self) -> InMemorySigner {
        self.0.into_value()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Data {
    pub chains: Memory,
    default: Arc<RwLock<Option<ChainId>>>,
    genesis_config: GenesisConfig,
}

pub struct PersistentWallet(persistent::File<Data>);

// TODO(#5081): `persistent` is no longer necessary here, we can move the locking
// logic right here

impl Wallet for PersistentWallet {
    type Error = persistent::file::Error;

    async fn get(&self, id: ChainId) -> Result<Option<Chain>, Self::Error> {
        Ok(self.get(id))
    }

    async fn remove(&self, id: ChainId) -> Result<Option<Chain>, Self::Error> {
        self.remove(id)
    }

    fn items(&self) -> impl Stream<Item = Result<(ChainId, Chain), Self::Error>> {
        stream::iter(self.items().into_iter().map(Ok))
    }

    async fn insert(&self, id: ChainId, chain: Chain) -> Result<Option<Chain>, Self::Error> {
        self.insert(id, chain)
    }

    async fn try_insert(&self, id: ChainId, chain: Chain) -> Result<Option<Chain>, Self::Error> {
        let chain = self.try_insert(id, chain)?;
        self.save()?;
        Ok(chain)
    }

    async fn modify(
        &self,
        id: ChainId,
        f: impl Fn(&mut Chain) + Send,
    ) -> Result<Option<()>, Self::Error> {
        self.mutate(id, f).transpose()
    }
}

impl Extend<(ChainId, Chain)> for PersistentWallet {
    fn extend<It: IntoIterator<Item = (ChainId, Chain)>>(&mut self, chains: It) {
        for (id, chain) in chains {
            if self.0.chains.try_insert(id, chain).is_none() {
                self.try_set_default(id);
            }
        }
    }
}

impl PersistentWallet {
    pub fn get(&self, id: ChainId) -> Option<Chain> {
        self.0.chains.get(id)
    }

    pub fn remove(&self, id: ChainId) -> Result<Option<Chain>, persistent::file::Error> {
        let chain = self.0.chains.remove(id);
        {
            let mut default = self.0.default.write().unwrap();
            if *default == Some(id) {
                *default = None;
            }
        }
        self.0.save()?;
        Ok(chain)
    }

    pub fn items(&self) -> Vec<(ChainId, Chain)> {
        self.0.chains.items()
    }

    fn try_set_default(&self, id: ChainId) {
        let mut guard = self.0.default.write().unwrap();
        if guard.is_none() {
            *guard = Some(id);
        }
    }

    pub fn insert(
        &self,
        id: ChainId,
        chain: Chain,
    ) -> Result<Option<Chain>, persistent::file::Error> {
        let has_owner = chain.owner.is_some();
        let old_chain = self.0.chains.insert(id, chain.clone());
        if has_owner {
            self.try_set_default(id);
        }
        self.0.save()?;
        Ok(old_chain)
    }

    pub fn try_insert(
        &self,
        id: ChainId,
        chain: Chain,
    ) -> Result<Option<Chain>, persistent::file::Error> {
        let chain = self.0.chains.try_insert(id, chain);
        if chain.is_none() {
            self.try_set_default(id);
        }
        self.save()?;
        Ok(chain)
    }

    pub fn create(
        path: &std::path::Path,
        genesis_config: GenesisConfig,
    ) -> Result<Self, persistent::file::Error> {
        Ok(Self(persistent::File::new(
            path,
            Data {
                chains: Memory::default(),
                default: Arc::new(RwLock::new(None)),
                genesis_config,
            },
        )?))
    }

    pub fn read(path: &std::path::Path) -> Result<Self, persistent::file::Error> {
        Ok(Self(persistent::File::read(path)?))
    }

    pub fn genesis_config(&self) -> &GenesisConfig {
        &self.0.genesis_config
    }

    pub fn genesis_admin_chain_id(&self) -> ChainId {
        self.0.genesis_config.admin_chain_id()
    }

    pub fn default_chain(&self) -> Option<ChainId> {
        *self.0.default.read().unwrap()
    }

    pub fn set_default_chain(&mut self, id: ChainId) -> Result<(), persistent::file::Error> {
        assert!(self.0.chains.get(id).is_some());
        *self.0.default.write().unwrap() = Some(id);
        self.0.save()
    }

    pub fn mutate<R>(
        &self,
        chain_id: ChainId,
        mutate: impl Fn(&mut Chain) -> R,
    ) -> Option<Result<R, persistent::file::Error>> {
        self.0
            .chains
            .mutate(chain_id, mutate)
            .map(|outcome| self.0.save().map(|()| outcome))
    }

    pub fn forget_keys(
        &self,
        chain_id: ChainId,
    ) -> Result<Option<AccountOwner>, persistent::file::Error> {
        self.mutate(chain_id, |chain| chain.owner.take())
            .transpose()
            .map(|opt| opt.flatten())
    }

    pub fn forget_chain(
        &self,
        chain_id: ChainId,
    ) -> Result<Option<Chain>, persistent::file::Error> {
        let chain = self.0.chains.remove(chain_id);
        if chain.is_some() {
            self.0.save()?;
        }
        Ok(chain)
    }

    pub fn save(&self) -> Result<(), persistent::file::Error> {
        self.0.save()
    }

    pub fn num_chains(&self) -> usize {
        self.0.chains.items().len()
    }

    pub fn chain_ids(&self) -> Vec<ChainId> {
        self.0.chains.chain_ids()
    }

    pub fn owned_chain_ids(&self) -> Vec<ChainId> {
        self.0.chains.owned_chain_ids()
    }
}
