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

/// The state of a chain tracked in the wallet.
#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Chain {
    pub owner: Option<AccountOwner>,
    pub block_hash: Option<CryptoHash>,
    pub next_block_height: BlockHeight,
    pub timestamp: Timestamp,
    pub pending_proposal: Option<PendingProposal>,
    pub epoch: Option<Epoch>,
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
        }
    }

    /// Returns whether this chain is in follow-only mode.
    ///
    /// A chain is in follow-only mode if it has no associated key pair (owner).
    pub fn follow_only(&self) -> bool {
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

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::AccountPublicKey, data_types::BlockHeight, identifiers::AccountOwner,
    };
    use serde_json::json;

    use super::Chain;

    /// Test that `follow_only()` returns the correct value based on owner presence.
    #[test]
    fn test_chain_follow_only() {
        // Chain with owner should not be follow-only.
        let json_with_owner = json!({
            "owner": AccountOwner::from(AccountPublicKey::test_key(0)),
            "block_hash": null,
            "next_block_height": BlockHeight::ZERO,
            "timestamp": 0,
            "pending_proposal": null,
            "epoch": null
        });
        let chain: Chain = serde_json::from_value(json_with_owner).unwrap();
        assert!(
            !chain.follow_only(),
            "chain with owner should not be follow-only"
        );

        // Chain without owner should be follow-only.
        let json_without_owner = json!({
            "owner": null,
            "block_hash": null,
            "next_block_height": BlockHeight::ZERO,
            "timestamp": 0,
            "pending_proposal": null,
            "epoch": null
        });
        let chain: Chain = serde_json::from_value(json_without_owner).unwrap();
        assert!(
            chain.follow_only(),
            "chain without owner should be follow-only"
        );
    }
}
