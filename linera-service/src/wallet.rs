// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    iter::IntoIterator,
    sync::{Arc, RwLock},
};

use futures::{stream, Stream};
use linera_base::{
    data_types::{ChainDescription, ChainOrigin},
    identifiers::{AccountOwner, ChainId},
};
use linera_client::config::GenesisConfig;
use linera_core::wallet;
use linera_persistent as persistent;

/// The maximum length of a chain name. Chain names must be shorter than a chain ID
/// (which is 64 hex characters) to avoid ambiguity.
pub const MAX_CHAIN_NAME_LENGTH: usize = 63;

#[derive(serde::Serialize, serde::Deserialize)]
struct Data {
    pub chains: wallet::Memory,
    default: Arc<RwLock<Option<ChainId>>>,
    genesis_config: GenesisConfig,
    /// Explicit chain name mappings. Names that are not in this map will be computed
    /// automatically ("admin" for the admin chain, "user-N" for user chains).
    #[serde(default)]
    chain_names: Arc<RwLock<BTreeMap<String, ChainId>>>,
}

struct ChainDetails {
    name: Option<String>,
    is_default: bool,
    is_admin: bool,
    origin: Option<ChainOrigin>,
    chain_id: ChainId,
    user_chain: wallet::Chain,
}

impl ChainDetails {
    fn new(chain_id: ChainId, wallet: &Data, chain_name: Option<String>) -> Self {
        let Some(user_chain) = wallet.chains.get(chain_id) else {
            panic!("Chain {} not found.", chain_id);
        };
        ChainDetails {
            name: chain_name,
            is_default: Some(chain_id) == *wallet.default.read().unwrap(),
            is_admin: chain_id == wallet.genesis_config.admin_id(),
            chain_id,
            origin: wallet
                .genesis_config
                .chains
                .iter()
                .find(|description| description.id() == chain_id)
                .map(ChainDescription::origin),
            user_chain,
        }
    }

    fn print_paragraph(&self) {
        println!("-----------------------");
        if let Some(name) = &self.name {
            println!("{:<20}  {}", "Name:", name);
        }
        println!("{:<20}  {}", "Chain ID:", self.chain_id);

        let mut tags = Vec::new();
        if self.is_default {
            tags.push("DEFAULT");
        }
        if self.is_admin {
            tags.push("ADMIN");
        }
        if self.user_chain.is_follow_only() {
            tags.push("FOLLOW-ONLY");
        }
        if !tags.is_empty() {
            println!("{:<20}  {}", "Tags:", tags.join(", "));
        }

        match self.origin {
            Some(ChainOrigin::Root(_)) | None => {
                println!("{:<20}  -", "Parent chain:");
            }
            Some(ChainOrigin::Child { parent, .. }) => {
                println!("{:<20}  {parent}", "Parent chain:");
            }
        }

        if let Some(owner) = &self.user_chain.owner {
            println!("{:<20}  {owner}", "Default owner:");
        } else {
            println!("{:<20}  No owner key", "Default owner:");
        }

        println!("{:<20}  {}", "Timestamp:", self.user_chain.timestamp);
        println!("{:<20}  {}", "Blocks:", self.user_chain.next_block_height);

        if let Some(epoch) = self.user_chain.epoch {
            println!("{:<20}  {epoch}", "Epoch:");
        } else {
            println!("{:<20}  -", "Epoch:");
        }

        if let Some(hash) = self.user_chain.block_hash {
            println!("{:<20}  {hash}", "Latest block hash:");
        }

        if self.user_chain.pending_proposal.is_some() {
            println!("{:<20}  present", "Pending proposal:");
        }
    }
}

pub struct Wallet(persistent::File<Data>);

// TODO(#5081): `persistent` is no longer necessary here, we can move the locking
// logic right here

impl linera_core::Wallet for Wallet {
    type Error = persistent::file::Error;

    async fn get(&self, id: ChainId) -> Result<Option<wallet::Chain>, Self::Error> {
        Ok(self.get(id))
    }

    async fn remove(&self, id: ChainId) -> Result<Option<wallet::Chain>, Self::Error> {
        self.remove(id)
    }

    fn items(&self) -> impl Stream<Item = Result<(ChainId, wallet::Chain), Self::Error>> {
        stream::iter(self.items().into_iter().map(Ok))
    }

    async fn insert(
        &self,
        id: ChainId,
        chain: wallet::Chain,
    ) -> Result<Option<wallet::Chain>, Self::Error> {
        self.insert(id, chain)
    }

    async fn try_insert(
        &self,
        id: ChainId,
        chain: wallet::Chain,
    ) -> Result<Option<wallet::Chain>, Self::Error> {
        let chain = self.try_insert(id, chain)?;
        self.save()?;
        Ok(chain)
    }

    async fn modify(
        &self,
        id: ChainId,
        f: impl FnMut(&mut wallet::Chain) + Send,
    ) -> Result<Option<()>, Self::Error> {
        self.mutate(id, f).transpose()
    }
}

impl Extend<(ChainId, wallet::Chain)> for Wallet {
    fn extend<It: IntoIterator<Item = (ChainId, wallet::Chain)>>(&mut self, chains: It) {
        for (id, chain) in chains {
            if self.0.chains.try_insert(id, chain).is_none() {
                self.try_set_default(id);
            }
        }
    }
}

impl Wallet {
    pub fn get(&self, id: ChainId) -> Option<wallet::Chain> {
        self.0.chains.get(id)
    }

    pub fn remove(&self, id: ChainId) -> Result<Option<wallet::Chain>, persistent::file::Error> {
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

    pub fn items(&self) -> Vec<(ChainId, wallet::Chain)> {
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
        chain: wallet::Chain,
    ) -> Result<Option<wallet::Chain>, persistent::file::Error> {
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
        chain: wallet::Chain,
    ) -> Result<Option<wallet::Chain>, persistent::file::Error> {
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
                chains: wallet::Memory::default(),
                default: Arc::new(RwLock::new(None)),
                genesis_config,
                chain_names: Arc::new(RwLock::new(BTreeMap::new())),
            },
        )?))
    }

    pub fn read(path: &std::path::Path) -> Result<Self, persistent::file::Error> {
        Ok(Self(persistent::File::read(path)?))
    }

    pub fn genesis_config(&self) -> &GenesisConfig {
        &self.0.genesis_config
    }

    pub fn genesis_admin_chain(&self) -> ChainId {
        self.0.genesis_config.admin_id()
    }

    /// Returns the name of a chain, if any.
    ///
    /// This looks up explicit names first, then falls back to default names:
    /// - "admin" for the admin chain
    /// - "user-N" for the N-th user chain (0-indexed, in wallet order, excluding admin)
    pub fn chain_name(&self, chain_id: ChainId) -> Option<String> {
        // Check for explicit name first.
        let chain_names = self.0.chain_names.read().unwrap();
        for (name, id) in chain_names.iter() {
            if *id == chain_id {
                return Some(name.clone());
            }
        }
        drop(chain_names);
        // Compute default name.
        if chain_id == self.genesis_admin_chain() {
            return Some("admin".to_string());
        }
        // Find the index of this chain among non-admin chains.
        let admin_id = self.genesis_admin_chain();
        for (index, id) in self
            .0
            .chains
            .chain_ids()
            .into_iter()
            .filter(|id| *id != admin_id)
            .enumerate()
        {
            if id == chain_id {
                return Some(format!("user-{index}"));
            }
        }
        None
    }

    /// Resolves a chain name to a chain ID.
    ///
    /// This looks up explicit names first, then checks default names:
    /// - "admin" resolves to the admin chain
    /// - "user-N" resolves to the N-th user chain (0-indexed, excluding admin)
    pub fn resolve_chain_name(&self, name: &str) -> Option<ChainId> {
        // Check for explicit name first.
        let chain_names = self.0.chain_names.read().unwrap();
        if let Some(chain_id) = chain_names.get(name) {
            return Some(*chain_id);
        }
        drop(chain_names);
        // Check for default names.
        if name == "admin" {
            return Some(self.genesis_admin_chain());
        }
        if let Some(index_str) = name.strip_prefix("user-") {
            if let Ok(index) = index_str.parse::<usize>() {
                let admin_id = self.genesis_admin_chain();
                return self
                    .0
                    .chains
                    .chain_ids()
                    .into_iter()
                    .filter(|id| *id != admin_id)
                    .nth(index);
            }
        }
        None
    }

    /// Sets or removes the name of a chain.
    ///
    /// If `name` is `Some`, sets the chain's name. If `name` is `None`, removes any
    /// explicit name (the chain will use its default name).
    ///
    /// Returns an error if the name is too long, if the chain doesn't exist in the wallet,
    /// or if the name is already used by another chain.
    pub fn set_chain_name(&self, chain_id: ChainId, name: Option<String>) -> anyhow::Result<()> {
        // Verify the chain exists.
        if self.0.chains.get(chain_id).is_none() {
            anyhow::bail!("chain `{chain_id}` not found in wallet");
        }
        let mut chain_names = self.0.chain_names.write().unwrap();
        // Remove any existing name for this chain.
        chain_names.retain(|_, id| *id != chain_id);
        if let Some(name) = name {
            // Validate the name.
            if name.len() > MAX_CHAIN_NAME_LENGTH {
                anyhow::bail!(
                    "chain name is too long ({} characters, maximum is {})",
                    name.len(),
                    MAX_CHAIN_NAME_LENGTH
                );
            }
            if name.is_empty() {
                anyhow::bail!("chain name cannot be empty");
            }
            // Check if the name is already used.
            if chain_names.contains_key(&name) {
                anyhow::bail!("chain name `{name}` is already used by another chain");
            }
            // Check if it conflicts with a default name (need to drop lock first).
            drop(chain_names);
            if self.resolve_chain_name(&name).is_some()
                && self.resolve_chain_name(&name) != Some(chain_id)
            {
                anyhow::bail!("chain name `{name}` conflicts with a default name");
            }
            // Re-acquire the lock and insert.
            let mut chain_names = self.0.chain_names.write().unwrap();
            chain_names.insert(name, chain_id);
        }
        self.0.save()?;
        Ok(())
    }

    // TODO(#5082): now that wallets only store chains, not keys, there's not much point in
    // allowing wallets with no default chain (i.e. no chains)
    pub fn default_chain(&self) -> Option<ChainId> {
        *self.0.default.read().unwrap()
    }

    pub fn pretty_print(&self, chain_ids: Vec<ChainId>) {
        let chain_ids: Vec<_> = chain_ids.into_iter().collect();
        let total_chains = chain_ids.len();

        let plural_s = if total_chains == 1 { "" } else { "s" };
        tracing::info!("Found {total_chains} chain{plural_s}");

        let mut chains = chain_ids
            .into_iter()
            .map(|chain_id| {
                let name = self.chain_name(chain_id);
                ChainDetails::new(chain_id, &self.0, name)
            })
            .collect::<Vec<_>>();
        // Print first the default, then the admin chain, then other root chains, and finally the
        // child chains.
        chains.sort_unstable_by_key(|chain| {
            let root_id = chain
                .origin
                .and_then(|origin| origin.root())
                .unwrap_or(u32::MAX);
            let chain_id = chain.chain_id;
            (!chain.is_default, !chain.is_admin, root_id, chain_id)
        });
        for chain in chains {
            chain.print_paragraph();
        }
        println!("------------------------");
    }

    pub fn set_default_chain(&mut self, id: ChainId) -> Result<(), persistent::file::Error> {
        assert!(self.0.chains.get(id).is_some());
        *self.0.default.write().unwrap() = Some(id);
        self.0.save()
    }

    pub fn mutate<R>(
        &self,
        chain_id: ChainId,
        mutate: impl FnMut(&mut wallet::Chain) -> R,
    ) -> Option<Result<R, persistent::file::Error>> {
        self.0
            .chains
            .mutate(chain_id, mutate)
            .map(|outcome| self.0.save().map(|()| outcome))
    }

    pub fn forget_keys(&self, chain_id: ChainId) -> anyhow::Result<AccountOwner> {
        self.mutate(chain_id, |chain| chain.owner.take())
            .ok_or(anyhow::anyhow!("nonexistent chain `{chain_id}`"))??
            .ok_or(anyhow::anyhow!("keypair not found for chain `{chain_id}`"))
    }

    pub fn forget_chain(&self, chain_id: ChainId) -> anyhow::Result<wallet::Chain> {
        let chain = self
            .0
            .chains
            .remove(chain_id)
            .ok_or(anyhow::anyhow!("nonexistent chain `{chain_id}`"))?;
        self.0.save()?;
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

    /// Returns the list of all chain IDs for which we have a secret key.
    pub fn owned_chain_ids(&self) -> Vec<ChainId> {
        self.0.chains.owned_chain_ids()
    }
}
