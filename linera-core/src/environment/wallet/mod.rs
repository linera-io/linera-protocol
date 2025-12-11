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
    /// If `Some(true)`, we only follow this chain's blocks without downloading sender chain
    /// blocks or participating in consensus rounds. Use this for chains we're interested in
    /// observing but don't intend to propose blocks for.
    ///
    /// For backwards compatibility with wallets created before this field existed, `None`
    /// defaults to `true` for chains without an owner (since they can't propose blocks anyway).
    /// Use [`Chain::follow_only()`] to get the effective value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub follow_only: Option<bool>,
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
            follow_only: Some(false),
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
            follow_only: Some(false),
        }
    }

    /// Returns whether this chain is in follow-only mode.
    ///
    /// For backwards compatibility with wallets created before this field existed,
    /// `None` defaults to `true` for chains without an owner (since they can't propose
    /// blocks anyway), and `false` for chains with an owner.
    pub fn follow_only(&self) -> bool {
        self.follow_only.unwrap_or(self.owner.is_none())
    }

    /// Sets whether this chain is in follow-only mode.
    pub fn set_follow_only(&mut self, follow_only: bool) {
        self.follow_only = Some(follow_only);
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

    /// Test that deserialization of old wallet data (without `follow_only` field)
    /// correctly defaults based on whether the chain has an owner.
    #[test]
    fn test_chain_deserialize_backwards_compatibility() {
        // Old wallet format without follow_only field, with owner.
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
            chain.follow_only.is_none(),
            "old wallet should have follow_only=None"
        );
        assert!(
            !chain.follow_only(),
            "chain with owner should default to follow_only()=false"
        );

        // Old wallet format without follow_only field, without owner.
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
            chain.follow_only.is_none(),
            "old wallet should have follow_only=None"
        );
        assert!(
            chain.follow_only(),
            "chain without owner should default to follow_only()=true"
        );

        // New wallet format with explicit follow_only field should preserve value.
        let json_with_explicit_follow_only = json!({
            "owner": AccountOwner::from(AccountPublicKey::test_key(0)),
            "block_hash": null,
            "next_block_height": BlockHeight::ZERO,
            "timestamp": 0,
            "pending_proposal": null,
            "epoch": null,
            "follow_only": true
        });
        let chain: Chain = serde_json::from_value(json_with_explicit_follow_only).unwrap();
        assert_eq!(chain.follow_only, Some(true));
        assert!(
            chain.follow_only(),
            "explicit follow_only=true should be preserved"
        );
    }

    /// Test that serialization and deserialization round-trip preserves the `follow_only` field.
    #[test]
    fn test_chain_serialize_deserialize_roundtrip() {
        use linera_base::data_types::{Epoch, Timestamp};

        // Chain with owner, follow_only = false.
        let chain1 = Chain {
            owner: Some(AccountOwner::from(AccountPublicKey::test_key(0))),
            block_hash: None,
            next_block_height: BlockHeight::ZERO,
            timestamp: Timestamp::from(0),
            pending_proposal: None,
            epoch: Some(Epoch::ZERO),
            follow_only: Some(false),
        };
        let json1 = serde_json::to_string(&chain1).unwrap();
        let chain1_roundtrip: Chain = serde_json::from_str(&json1).unwrap();
        assert_eq!(chain1.follow_only, chain1_roundtrip.follow_only);
        assert!(!chain1_roundtrip.follow_only());

        // Chain with owner, follow_only = true (e.g., after forget-keys).
        let chain2 = Chain {
            owner: Some(AccountOwner::from(AccountPublicKey::test_key(1))),
            block_hash: None,
            next_block_height: BlockHeight::ZERO,
            timestamp: Timestamp::from(0),
            pending_proposal: None,
            epoch: Some(Epoch::ZERO),
            follow_only: Some(true),
        };
        let json2 = serde_json::to_string(&chain2).unwrap();
        let chain2_roundtrip: Chain = serde_json::from_str(&json2).unwrap();
        assert_eq!(chain2.follow_only, chain2_roundtrip.follow_only);
        assert!(chain2_roundtrip.follow_only());

        // Chain without owner, follow_only = true.
        let chain3 = Chain {
            owner: None,
            block_hash: None,
            next_block_height: BlockHeight::ZERO,
            timestamp: Timestamp::from(0),
            pending_proposal: None,
            epoch: Some(Epoch::ZERO),
            follow_only: Some(true),
        };
        let json3 = serde_json::to_string(&chain3).unwrap();
        let chain3_roundtrip: Chain = serde_json::from_str(&json3).unwrap();
        assert_eq!(chain3.follow_only, chain3_roundtrip.follow_only);
        assert!(chain3_roundtrip.follow_only());

        // Chain without owner, follow_only = false (edge case, but should be preserved).
        let chain4 = Chain {
            owner: None,
            block_hash: None,
            next_block_height: BlockHeight::ZERO,
            timestamp: Timestamp::from(0),
            pending_proposal: None,
            epoch: Some(Epoch::ZERO),
            follow_only: Some(false),
        };
        let json4 = serde_json::to_string(&chain4).unwrap();
        let chain4_roundtrip: Chain = serde_json::from_str(&json4).unwrap();
        assert_eq!(chain4.follow_only, chain4_roundtrip.follow_only);
        assert!(!chain4_roundtrip.follow_only());
    }
}
