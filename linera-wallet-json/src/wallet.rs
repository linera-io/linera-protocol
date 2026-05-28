// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A wallet persisted as a JSON file, tracking the client's chains and default chain.

use std::{
    iter::IntoIterator,
    sync::{Arc, RwLock},
};

use futures::{stream, Stream};
use linera_base::identifiers::{AccountOwner, ChainId};
use linera_client::config::GenesisConfig;
use linera_core::wallet::*;
use linera_persistent::{self as persistent};

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct Data {
    pub chains: Memory,
    pub default: Arc<RwLock<Option<ChainId>>>,
    pub genesis_config: GenesisConfig,
}

/// A wallet backed by a JSON file, holding the client's chains and which one is the default.
pub struct PersistentWallet(persistent::File<Data>);

// TODO(#5081): `persistent` is no longer necessary here, we can move the locking
// logic right here

impl linera_core::Wallet for PersistentWallet {
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
        self.insert(id, &chain)
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
    /// Returns the chain with the given ID, if it is in the wallet.
    pub fn get(&self, id: ChainId) -> Option<Chain> {
        self.0.chains.get(id)
    }

    /// Removes the chain with the given ID, adjusting the default chain if needed, and saves.
    pub fn remove(&self, id: ChainId) -> Result<Option<Chain>, persistent::file::Error> {
        let chain = self.0.chains.remove(id);
        {
            let mut default = self.0.default.write().unwrap();
            if *default == Some(id) {
                *default = None;
            }
            if default.is_none() {
                let items = self.0.chains.items();
                if items.len() == 1 {
                    *default = Some(items[0].0);
                }
            }
        }
        self.0.save()?;
        Ok(chain)
    }

    /// Returns all `(chain ID, chain)` pairs held by the wallet.
    pub fn items(&self) -> Vec<(ChainId, Chain)> {
        self.0.chains.items()
    }

    fn try_set_default(&self, id: ChainId) {
        let mut guard = self.0.default.write().unwrap();
        if guard.is_none() {
            *guard = Some(id);
        }
    }

    /// Inserts or replaces a chain, making it the default chain if it has an owner, and saves.
    pub fn insert(
        &self,
        id: ChainId,
        chain: &Chain,
    ) -> Result<Option<Chain>, persistent::file::Error> {
        let has_owner = chain.owner.is_some();
        let old_chain = self.0.chains.insert(id, chain.clone());
        if has_owner {
            self.try_set_default(id);
        }
        self.0.save()?;
        Ok(old_chain)
    }

    /// Inserts a chain only if its ID is not already present, making it the default if it is
    /// the first chain, and saves.
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

    /// Creates a new wallet file at `path` for the given genesis configuration.
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

    /// Reads an existing wallet from the file at `path`.
    pub fn read(path: &std::path::Path) -> Result<Self, persistent::file::Error> {
        Ok(Self(persistent::File::read(path)?))
    }

    /// Returns the network's genesis configuration.
    pub fn genesis_config(&self) -> &GenesisConfig {
        &self.0.genesis_config
    }

    /// Returns the admin chain ID from the genesis configuration.
    pub fn genesis_admin_chain_id(&self) -> ChainId {
        self.0.genesis_config.admin_chain_id()
    }

    /// Returns the default chain, if one is set.
    pub fn default_chain(&self) -> Option<ChainId> {
        *self.0.default.read().unwrap()
    }

    /// Sets the default chain, which must already be in the wallet, and saves.
    pub fn set_default_chain(&mut self, id: ChainId) -> Result<(), persistent::file::Error> {
        assert!(self.0.chains.get(id).is_some());
        *self.0.default.write().unwrap() = Some(id);
        self.0.save()
    }

    /// Applies a mutation to the chain with the given ID, saving afterwards. Returns `None`
    /// if the chain is not in the wallet.
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

    /// Removes and returns the owner of the given chain, erroring if the chain or owner is absent.
    pub fn forget_keys(&self, chain_id: ChainId) -> anyhow::Result<AccountOwner> {
        self.mutate(chain_id, |chain| chain.owner.take())
            .ok_or_else(|| anyhow::anyhow!("nonexistent chain `{chain_id}`"))??
            .ok_or_else(|| anyhow::anyhow!("keypair not found for chain `{chain_id}`"))
    }

    /// Writes the wallet to its file.
    pub fn save(&self) -> Result<(), persistent::file::Error> {
        self.0.save()
    }

    /// Returns the number of chains in the wallet.
    pub fn num_chains(&self) -> usize {
        self.0.chains.items().len()
    }

    /// Returns the IDs of all chains in the wallet.
    pub fn chain_ids(&self) -> Vec<ChainId> {
        self.0.chains.chain_ids()
    }

    /// Returns the list of all chain IDs for which we have a secret key.
    pub fn owned_chain_ids(&self) -> Vec<ChainId> {
        self.0.chains.owned_chain_ids()
    }

    pub(crate) fn data(&self) -> &Data {
        &self.0
    }
}
