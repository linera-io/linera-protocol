// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, RwLock};

use futures::{Stream, StreamExt as _};
use linera_base::identifiers::ChainId;
use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};

use super::{Chain, Wallet};
use crate::GenesisConfig;

/// A complete in-memory wallet implementation.
///
/// This can be used as-is as an ephemeral wallet for testing or ephemeral clients, or as
/// a building block for more complex wallets that layer persistence on top of it.
#[derive(Clone, Deserialize)]
pub struct Memory {
    /// The chains tracked by this wallet.
    chains: papaya::HashMap<ChainId, Chain>,
    /// The default chain ID.
    default: Arc<RwLock<Option<ChainId>>>,
    /// The genesis configuration.
    genesis_config: GenesisConfig,
}

impl Memory {
    /// Creates a new Memory wallet with the given genesis configuration.
    pub fn new(genesis_config: GenesisConfig) -> Self {
        Self {
            chains: papaya::HashMap::new(),
            default: Arc::new(RwLock::new(None)),
            genesis_config,
        }
    }

    /// Returns a reference to the genesis configuration.
    pub fn genesis_config(&self) -> &GenesisConfig {
        &self.genesis_config
    }

    /// Returns the admin chain ID from the genesis configuration.
    pub fn admin_id(&self) -> ChainId {
        self.genesis_config.admin_id()
    }

    /// Returns the default chain ID, if one is set.
    pub fn default_chain(&self) -> Option<ChainId> {
        *self.default.read().unwrap()
    }

    /// Sets the default chain ID.
    pub fn set_default_chain(&self, id: ChainId) {
        *self.default.write().unwrap() = Some(id);
    }

    /// Sets the default chain ID if none is currently set.
    pub fn try_set_default(&self, id: ChainId) {
        let mut guard = self.default.write().unwrap();
        if guard.is_none() {
            *guard = Some(id);
        }
    }

    /// Clears the default chain if it matches the given ID.
    fn clear_default_if(&self, id: ChainId) {
        let mut guard = self.default.write().unwrap();
        if *guard == Some(id) {
            *guard = None;
        }
    }

    pub fn get(&self, id: ChainId) -> Option<Chain> {
        self.chains.pin().get(&id).cloned()
    }

    pub fn insert(&self, id: ChainId, chain: Chain) -> Option<Chain> {
        let has_owner = chain.owner.is_some();
        let result = self.chains.pin().insert(id, chain).cloned();
        if has_owner {
            self.try_set_default(id);
        }
        result
    }

    pub fn try_insert(&self, id: ChainId, chain: Chain) -> Option<Chain> {
        match self.chains.pin().try_insert(id, chain) {
            Ok(_inserted) => {
                self.try_set_default(id);
                None
            }
            Err(error) => Some(error.not_inserted),
        }
    }

    pub fn remove(&self, id: ChainId) -> Option<Chain> {
        let result = self.chains.pin().remove(&id).cloned();
        self.clear_default_if(id);
        result
    }

    pub fn items(&self) -> Vec<(ChainId, Chain)> {
        self.chains
            .pin()
            .iter()
            .map(|(id, chain)| (*id, chain.clone()))
            .collect::<Vec<_>>()
    }

    pub fn chain_ids(&self) -> Vec<ChainId> {
        self.chains.pin().keys().copied().collect::<Vec<_>>()
    }

    pub fn owned_chain_ids(&self) -> Vec<ChainId> {
        self.chains
            .pin()
            .iter()
            .filter_map(|(id, chain)| chain.owner.as_ref().map(|_| *id))
            .collect::<Vec<_>>()
    }

    pub fn mutate<R>(
        &self,
        chain_id: ChainId,
        mut mutate: impl FnMut(&mut Chain) -> R,
    ) -> Option<R> {
        use papaya::Operation::*;

        let mut outcome = None;
        self.chains.pin().compute(chain_id, |chain| {
            if let Some((_, chain)) = chain {
                let mut chain = chain.clone();
                outcome = Some(mutate(&mut chain));
                Insert(chain)
            } else {
                Abort(())
            }
        });

        outcome
    }
}

/// Custom Serialize implementation that ensures stable ordering by sorting chains by ChainId.
impl Serialize for Memory {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let guard = self.chains.pin();
        let mut items: Vec<_> = guard.iter().collect();
        items.sort_by_key(|(k, _)| *k);

        // We need to serialize the chains as a map within the struct.
        // Build the chains map manually for sorted output.
        #[derive(Serialize)]
        struct SortedChains<'a>(
            #[serde(serialize_with = "serialize_sorted_chains")] &'a [(&'a ChainId, &'a Chain)],
        );

        fn serialize_sorted_chains<S: Serializer>(
            items: &[(&ChainId, &Chain)],
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeMap;
            let mut map = serializer.serialize_map(Some(items.len()))?;
            for (k, v) in items {
                map.serialize_entry(k, v)?;
            }
            map.end()
        }

        let mut state = serializer.serialize_struct("Memory", 3)?;
        state.serialize_field("chains", &SortedChains(&items))?;
        state.serialize_field("default", &*self.default.read().unwrap())?;
        state.serialize_field("genesis_config", &self.genesis_config)?;
        state.end()
    }
}

impl Extend<(ChainId, Chain)> for Memory {
    fn extend<It: IntoIterator<Item = (ChainId, Chain)>>(&mut self, chains: It) {
        let map = self.chains.pin();
        for (id, chain) in chains {
            map.insert(id, chain);
        }
    }
}

impl Wallet for Memory {
    type Error = std::convert::Infallible;

    async fn get(&self, id: ChainId) -> Result<Option<Chain>, Self::Error> {
        Ok(self.get(id))
    }

    async fn insert(&self, id: ChainId, chain: Chain) -> Result<Option<Chain>, Self::Error> {
        Ok(self.insert(id, chain))
    }

    async fn try_insert(&self, id: ChainId, chain: Chain) -> Result<Option<Chain>, Self::Error> {
        Ok(self.try_insert(id, chain))
    }

    async fn remove(&self, id: ChainId) -> Result<Option<Chain>, Self::Error> {
        Ok(self.remove(id))
    }

    fn items(&self) -> impl Stream<Item = Result<(ChainId, Chain), Self::Error>> {
        futures::stream::iter(self.items()).map(Ok)
    }

    async fn modify(
        &self,
        id: ChainId,
        f: impl FnMut(&mut Chain) + Send,
    ) -> Result<Option<()>, Self::Error> {
        Ok(self.mutate(id, f))
    }
}

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::CryptoHash,
        data_types::{Amount, Timestamp},
    };

    use super::*;

    fn make_test_genesis_config() -> GenesisConfig {
        use linera_base::{
            crypto::AccountPublicKey,
            data_types::{ChainDescription, ChainOrigin, Epoch, InitialChainConfig},
            ownership::ChainOwnership,
        };
        use linera_execution::{committee::Committee, ResourceControlPolicy};

        let committee = Committee::new(Default::default(), ResourceControlPolicy::default());
        let admin_chain = ChainDescription::new(
            ChainOrigin::Root(0),
            InitialChainConfig {
                application_permissions: Default::default(),
                balance: Amount::ZERO,
                min_active_epoch: Epoch::ZERO,
                max_active_epoch: Epoch::ZERO,
                epoch: Epoch::ZERO,
                ownership: ChainOwnership::single(AccountPublicKey::test_key(0).into()),
            },
            Timestamp::from(0),
        );
        GenesisConfig {
            committee,
            timestamp: Timestamp::from(0),
            chains: vec![admin_chain],
            network_name: "test".to_string(),
        }
    }

    fn make_chain(height: u64) -> Chain {
        Chain {
            owner: None,
            block_hash: None,
            next_block_height: height.into(),
            timestamp: Timestamp::from(0),
            pending_proposal: None,
            epoch: None,
        }
    }

    #[test]
    fn test_memory_serialization_roundtrip() {
        let memory = Memory::new(make_test_genesis_config());

        // Insert chains in non-sorted order using different hashes.
        let id1 = ChainId(CryptoHash::test_hash("chain1"));
        let id2 = ChainId(CryptoHash::test_hash("chain2"));
        let id3 = ChainId(CryptoHash::test_hash("chain3"));

        memory.insert(id2, make_chain(2));
        memory.insert(id1, make_chain(1));
        memory.insert(id3, make_chain(3));
        memory.set_default_chain(id2);

        // Serialize to JSON.
        let json = serde_json::to_string_pretty(&memory).unwrap();

        // Deserialize back.
        let restored: Memory = serde_json::from_str(&json).unwrap();

        // Verify data matches.
        assert_eq!(restored.get(id1).unwrap().next_block_height, 1.into());
        assert_eq!(restored.get(id2).unwrap().next_block_height, 2.into());
        assert_eq!(restored.get(id3).unwrap().next_block_height, 3.into());
        assert_eq!(restored.default_chain(), Some(id2));
    }

    #[test]
    fn test_memory_serialization_has_expected_structure() {
        let memory = Memory::new(make_test_genesis_config());

        let id1 = ChainId(CryptoHash::test_hash("a"));
        memory.insert(id1, make_chain(1));

        // Serialize and verify output has expected structure.
        let json = serde_json::to_string(&memory).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let obj = value.as_object().unwrap();

        // Should have chains, default, and genesis_config fields.
        assert!(obj.contains_key("chains"));
        assert!(obj.contains_key("default"));
        assert!(obj.contains_key("genesis_config"));
    }
}
