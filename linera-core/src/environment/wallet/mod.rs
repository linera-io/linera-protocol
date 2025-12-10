// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use futures::{Stream, StreamExt as _, TryStreamExt as _};
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, ChainDescription, Epoch, Timestamp},
    identifiers::{AccountOwner, ChainId},
};

use crate::{client::PendingProposal, data_types::ChainInfo};

mod memory;
pub use memory::Memory;

#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Chain {
    pub owner: Option<AccountOwner>,
    pub block_hash: Option<CryptoHash>,
    pub next_block_height: BlockHeight,
    pub timestamp: Timestamp,
    pub pending_proposal: Option<PendingProposal>,
    pub epoch: Option<Epoch>,
    /// If true, we only follow this chain's blocks without downloading sender chain blocks
    /// or participating in consensus rounds. Use this for chains we're interested in observing
    /// but don't intend to propose blocks for.
    #[serde(default)]
    pub follow_only: bool,
}

impl From<&ChainInfo> for Chain {
    fn from(info: &ChainInfo) -> Self {
        Self {
            owner: None,
            block_hash: info.block_hash,
            next_block_height: info.next_block_height,
            timestamp: info.timestamp,
            pending_proposal: None,
            epoch: Some(info.epoch),
            follow_only: false,
        }
    }
}

impl From<ChainInfo> for Chain {
    fn from(info: ChainInfo) -> Self {
        Self::from(&info)
    }
}

impl From<&ChainDescription> for Chain {
    fn from(description: &ChainDescription) -> Self {
        Self::new(None, description.config().epoch, description.timestamp())
    }
}

impl From<ChainDescription> for Chain {
    fn from(description: ChainDescription) -> Self {
        (&description).into()
    }
}

impl Chain {
    /// Create a chain that we haven't interacted with before.
    pub fn new(owner: Option<AccountOwner>, current_epoch: Epoch, now: Timestamp) -> Self {
        Self {
            owner,
            block_hash: None,
            timestamp: now,
            next_block_height: BlockHeight::ZERO,
            pending_proposal: None,
            epoch: Some(current_epoch),
            follow_only: false,
        }
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
