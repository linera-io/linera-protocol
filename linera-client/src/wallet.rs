// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, iter::IntoIterator};

use linera_base::{
    crypto::CryptoHash,
    data_types::{BlockHeight, ChainDescription, Epoch, Timestamp},
    ensure,
    identifiers::{AccountOwner, ChainId},
};
use linera_core::{
    client::{ListeningMode, PendingProposal},
    data_types::ChainInfo,
};
use serde::{Deserialize, Serialize};

use crate::{config::GenesisConfig, error, Error};

#[derive(Serialize, Deserialize)]
pub struct Wallet {
    pub chains: BTreeMap<ChainId, UserChain>,
    pub default: Option<ChainId>,
    genesis_config: GenesisConfig,
}

impl Extend<UserChain> for Wallet {
    fn extend<Chains: IntoIterator<Item = UserChain>>(&mut self, chains: Chains) {
        for chain in chains.into_iter() {
            if !self.chains.contains_key(&chain.chain_id) {
                self.insert(chain);
            }
        }
    }
}

impl Wallet {
    pub fn new(genesis_config: GenesisConfig) -> Self {
        Wallet {
            chains: BTreeMap::new(),
            default: None,
            genesis_config,
        }
    }

    pub fn get(&self, chain_id: ChainId) -> Option<&UserChain> {
        self.chains.get(&chain_id)
    }

    pub fn insert(&mut self, chain: UserChain) {
        if self.default.is_none() && chain.owner.is_some() {
            self.default = Some(chain.chain_id);
        }

        self.chains.insert(chain.chain_id, chain);
    }

    pub fn forget_keys(&mut self, chain_id: &ChainId) -> Result<AccountOwner, Error> {
        let chain = self
            .chains
            .get_mut(chain_id)
            .ok_or(error::Inner::NonexistentChain(*chain_id))?;

        let owner = chain
            .owner
            .take()
            .ok_or(error::Inner::NonexistentKeypair(*chain_id))?;

        Ok(owner)
    }

    pub fn forget_chain(&mut self, chain_id: &ChainId) -> Result<UserChain, Error> {
        let user_chain = self
            .chains
            .remove(chain_id)
            .ok_or::<Error>(error::Inner::NonexistentChain(*chain_id).into())?;
        Ok(user_chain)
    }

    pub fn default_chain(&self) -> Option<ChainId> {
        self.default
    }

    pub fn first_non_admin_chain(&self) -> Option<ChainId> {
        self.chain_ids()
            .into_iter()
            .find(|chain_id| *chain_id != self.genesis_config.admin_id())
    }

    pub fn chain_ids(&self) -> Vec<ChainId> {
        self.chains.keys().copied().collect()
    }

    /// Returns the list of all chain IDs for which we have a secret key.
    pub fn owned_chain_ids(&self) -> Vec<ChainId> {
        self.chains
            .iter()
            .filter_map(|(chain_id, chain)| chain.owner.is_some().then_some(*chain_id))
            .collect()
    }

    pub fn num_chains(&self) -> usize {
        self.chains.len()
    }

    pub fn chains_mut(&mut self) -> impl Iterator<Item = &mut UserChain> {
        self.chains.values_mut()
    }

    pub fn assign_new_chain_to_owner(
        &mut self,
        owner: AccountOwner,
        chain_id: ChainId,
        timestamp: Timestamp,
        epoch: Epoch,
    ) -> Result<(), Error> {
        let user_chain = UserChain {
            chain_id,
            owner: Some(owner),
            block_hash: None,
            timestamp,
            next_block_height: BlockHeight(0),
            pending_proposal: None,
            epoch: Some(epoch),
            listening_mode: ListeningMode::FullChain,
        };
        self.insert(user_chain);
        Ok(())
    }

    pub fn set_default_chain(&mut self, chain_id: ChainId) -> Result<(), Error> {
        ensure!(
            self.chains.contains_key(&chain_id),
            error::Inner::NonexistentChain(chain_id)
        );
        self.default = Some(chain_id);
        Ok(())
    }

    pub fn update_from_info(
        &mut self,
        pending_proposal: Option<PendingProposal>,
        owner: Option<AccountOwner>,
        info: &ChainInfo,
    ) {
        // Preserve the existing listening mode if the chain is already in the wallet.
        let listening_mode = self
            .chains
            .get(&info.chain_id)
            .map(|c| c.listening_mode.clone())
            .unwrap_or_default();
        self.insert(UserChain {
            chain_id: info.chain_id,
            owner,
            block_hash: info.block_hash,
            next_block_height: info.next_block_height,
            timestamp: info.timestamp,
            pending_proposal,
            epoch: Some(info.epoch),
            listening_mode,
        });
    }

    pub fn genesis_admin_chain(&self) -> ChainId {
        self.genesis_config.admin_id()
    }

    pub fn genesis_config(&self) -> &GenesisConfig {
        &self.genesis_config
    }
}

#[derive(Serialize, Deserialize)]
pub struct UserChain {
    pub chain_id: ChainId,
    /// The owner of the chain, if we own it.
    pub owner: Option<AccountOwner>,
    pub block_hash: Option<CryptoHash>,
    pub timestamp: Timestamp,
    pub next_block_height: BlockHeight,
    pub pending_proposal: Option<PendingProposal>,
    pub epoch: Option<Epoch>,
    /// How to listen to this chain. Defaults to `FullChain` for backward compatibility.
    #[serde(default)]
    pub listening_mode: ListeningMode,
}

impl Clone for UserChain {
    fn clone(&self) -> Self {
        Self {
            chain_id: self.chain_id,
            owner: self.owner,
            block_hash: self.block_hash,
            timestamp: self.timestamp,
            next_block_height: self.next_block_height,
            pending_proposal: self.pending_proposal.clone(),
            epoch: self.epoch,
            listening_mode: self.listening_mode.clone(),
        }
    }
}

impl UserChain {
    /// Create a user chain that we own.
    pub fn make_initial(
        owner: AccountOwner,
        description: ChainDescription,
        timestamp: Timestamp,
    ) -> Self {
        let epoch = description.config().epoch;
        Self {
            chain_id: description.into(),
            owner: Some(owner),
            block_hash: None,
            timestamp,
            next_block_height: BlockHeight::ZERO,
            pending_proposal: None,
            epoch: Some(epoch),
            listening_mode: ListeningMode::FullChain,
        }
    }

    /// Creates an entry for a chain that we don't own. The timestamp must be the genesis
    /// timestamp or earlier. The Epoch is `None`.
    pub fn make_other(
        chain_id: ChainId,
        timestamp: Timestamp,
        listening_mode: ListeningMode,
    ) -> Self {
        Self {
            chain_id,
            owner: None,
            block_hash: None,
            timestamp,
            next_block_height: BlockHeight::ZERO,
            pending_proposal: None,
            epoch: None,
            listening_mode,
        }
    }
}

#[cfg(test)]
mod tests {
    use linera_base::{
        data_types::{BlockHeight, Epoch, Timestamp},
        identifiers::ChainId,
    };
    use linera_core::client::ListeningMode;

    use super::UserChain;

    /// Tests that wallet.json files created before the `listening_mode` field was added
    /// can still be deserialized, with `listening_mode` defaulting to `FullChain`.
    #[test]
    fn test_user_chain_backwards_compatibility_without_listening_mode() {
        // JSON representing a UserChain from before the listening_mode field was added
        // Note: epoch serializes as a string in human-readable JSON format
        let old_format_json = r#"{
            "chain_id": "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65",
            "owner": null,
            "block_hash": null,
            "timestamp": 0,
            "next_block_height": 0,
            "pending_proposal": null,
            "epoch": "0"
        }"#;

        let user_chain: UserChain =
            serde_json::from_str(old_format_json).expect("Should deserialize old format");

        // Verify the listening_mode defaults to FullChain - this is the key backwards compatibility test
        assert_eq!(
            user_chain.listening_mode,
            ListeningMode::FullChain,
            "Missing listening_mode field should default to FullChain for backwards compatibility"
        );

        // Verify a few other fields were parsed correctly
        assert_eq!(user_chain.owner, None);
        assert_eq!(user_chain.timestamp, Timestamp::from(0));
        assert_eq!(user_chain.next_block_height, BlockHeight::ZERO);
        assert_eq!(user_chain.epoch, Some(Epoch::ZERO));
    }

    /// Tests that a UserChain with an explicit listening_mode is deserialized correctly.
    #[test]
    fn test_user_chain_with_listening_mode() {
        // JSON with explicit SkipSenders listening_mode
        let json_with_skip_senders = r#"{
            "chain_id": "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65",
            "owner": null,
            "block_hash": null,
            "timestamp": 0,
            "next_block_height": 0,
            "pending_proposal": null,
            "epoch": "0",
            "listening_mode": "SkipSenders"
        }"#;

        let user_chain: UserChain = serde_json::from_str(json_with_skip_senders)
            .expect("Should deserialize with SkipSenders");

        assert_eq!(
            user_chain.listening_mode,
            ListeningMode::SkipSenders,
            "Explicit SkipSenders should be preserved"
        );
    }

    /// Tests round-trip serialization preserves listening_mode.
    #[test]
    fn test_user_chain_serialization_round_trip() {
        let original = UserChain {
            chain_id: ChainId::default(),
            owner: None,
            block_hash: None,
            timestamp: Timestamp::from(12345),
            next_block_height: BlockHeight::from(10),
            pending_proposal: None,
            epoch: Some(Epoch::from(1)),
            listening_mode: ListeningMode::SkipSenders,
        };

        let json = serde_json::to_string(&original).expect("Should serialize");
        let deserialized: UserChain = serde_json::from_str(&json).expect("Should deserialize");

        assert_eq!(deserialized.chain_id, original.chain_id);
        assert_eq!(deserialized.listening_mode, ListeningMode::SkipSenders);
        assert_eq!(deserialized.timestamp, original.timestamp);
        assert_eq!(deserialized.next_block_height, original.next_block_height);
        assert_eq!(deserialized.epoch, original.epoch);
    }
}
