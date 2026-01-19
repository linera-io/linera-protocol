// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::ops::Deref;

use futures::{Stream, StreamExt as _, TryStreamExt as _};
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, ChainDescription, Epoch, Timestamp},
    identifiers::{AccountOwner, ChainId},
};

use crate::{client::PendingProposal, data_types::ChainInfo};

mod memory;
pub use memory::Memory;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Chain {
    /// The name of this chain in the wallet.
    pub name: String,
    pub owner: Option<AccountOwner>,
    pub block_hash: Option<CryptoHash>,
    pub next_block_height: BlockHeight,
    pub timestamp: Timestamp,
    pub pending_proposal: Option<PendingProposal>,
    pub epoch: Option<Epoch>,
}

impl Chain {
    /// Creates a chain that we haven't interacted with before.
    pub fn new(
        name: impl Into<String>,
        owner: Option<AccountOwner>,
        current_epoch: Epoch,
        now: Timestamp,
    ) -> Self {
        Self {
            name: name.into(),
            owner,
            block_hash: None,
            timestamp: now,
            next_block_height: BlockHeight::ZERO,
            pending_proposal: None,
            epoch: Some(current_epoch),
        }
    }

    /// Creates a chain from a chain description.
    pub fn from_description(name: impl Into<String>, description: &ChainDescription) -> Self {
        Self::new(
            name,
            None,
            description.config().epoch,
            description.timestamp(),
        )
    }

    /// Creates a chain from chain info.
    pub fn from_info(name: impl Into<String>, info: &ChainInfo) -> Self {
        Self {
            name: name.into(),
            owner: None,
            block_hash: info.block_hash,
            next_block_height: info.next_block_height,
            timestamp: info.timestamp,
            pending_proposal: None,
            epoch: Some(info.epoch),
        }
    }

    /// Returns `true` if we only follow this chain's blocks without participating in consensus.
    ///
    /// A chain is follow-only if there is no key pair configured for it, i.e., if `owner` is
    /// `None`.
    pub fn is_follow_only(&self) -> bool {
        self.owner.is_none()
    }
}

/// A trait for the wallet (i.e. set of chain states) tracked by the client.
#[cfg_attr(not(web), trait_variant::make(Send))]
pub trait Wallet {
    type Error: std::error::Error + Send + Sync;
    async fn get(&self, id: ChainId) -> Result<Option<Chain>, Self::Error>;
    async fn remove(&self, id: ChainId) -> Result<Option<Chain>, Self::Error>;
    fn items(&self) -> impl Stream<Item = Result<(ChainId, Chain), Self::Error>>;
    async fn insert(&self, id: ChainId, chain: Chain) -> Result<Option<Chain>, Self::Error>;
    async fn try_insert(&self, id: ChainId, chain: Chain) -> Result<Option<Chain>, Self::Error>;

    fn chain_ids(&self) -> impl Stream<Item = Result<ChainId, Self::Error>> {
        self.items().map(|result| result.map(|kv| kv.0))
    }

    fn owned_chain_ids(&self) -> impl Stream<Item = Result<ChainId, Self::Error>> {
        self.items()
            .try_filter_map(|(id, chain)| async move { Ok(chain.owner.map(|_| id)) })
    }

    /// Modifies a chain in the wallet. Returns `Ok(None)` if the chain doesn't exist.
    async fn modify(
        &self,
        id: ChainId,
        f: impl FnMut(&mut Chain) + Send,
    ) -> Result<Option<()>, Self::Error>;
}

impl<W: Deref<Target: Wallet> + linera_base::util::traits::AutoTraits> Wallet for W {
    type Error = <W::Target as Wallet>::Error;

    async fn get(&self, id: ChainId) -> Result<Option<Chain>, Self::Error> {
        self.deref().get(id).await
    }

    async fn remove(&self, id: ChainId) -> Result<Option<Chain>, Self::Error> {
        self.deref().remove(id).await
    }

    fn items(&self) -> impl Stream<Item = Result<(ChainId, Chain), Self::Error>> {
        self.deref().items()
    }

    async fn insert(&self, id: ChainId, chain: Chain) -> Result<Option<Chain>, Self::Error> {
        self.deref().insert(id, chain).await
    }

    async fn try_insert(&self, id: ChainId, chain: Chain) -> Result<Option<Chain>, Self::Error> {
        self.deref().try_insert(id, chain).await
    }

    fn chain_ids(&self) -> impl Stream<Item = Result<ChainId, Self::Error>> {
        self.deref().chain_ids()
    }

    fn owned_chain_ids(&self) -> impl Stream<Item = Result<ChainId, Self::Error>> {
        self.deref().owned_chain_ids()
    }

    async fn modify(
        &self,
        id: ChainId,
        f: impl FnMut(&mut Chain) + Send,
    ) -> Result<Option<()>, Self::Error> {
        self.deref().modify(id, f).await
    }
}
