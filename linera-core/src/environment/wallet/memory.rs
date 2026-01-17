// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use futures::{Stream, StreamExt as _};
use linera_base::identifiers::ChainId;
use serde::{ser::SerializeMap, Serialize, Serializer};

use super::{Chain, Wallet};

/// A basic implementation of `Wallet` that doesn't persist anything and merely tracks the
/// chains in memory.
///
/// This can be used as-is as an ephemeral wallet for testing or ephemeral clients, or as
/// a building block for more complex wallets that layer persistence on top of it.
#[derive(Default, Clone, serde::Deserialize)]
pub struct Memory(papaya::HashMap<ChainId, Chain>);

/// Custom Serialize implementation that ensures stable ordering by sorting entries by ChainId.
impl Serialize for Memory {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let guard = self.0.pin();
        let mut items: Vec<_> = guard.iter().collect();
        items.sort_by_key(|(k, _)| *k);
        let mut map = serializer.serialize_map(Some(items.len()))?;
        for (k, v) in items {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

impl Memory {
    pub fn get(&self, id: ChainId) -> Option<Chain> {
        self.0.pin().get(&id).cloned()
    }

    pub fn insert(&self, id: ChainId, chain: Chain) -> Option<Chain> {
        self.0.pin().insert(id, chain).cloned()
    }

    pub fn try_insert(&self, id: ChainId, chain: Chain) -> Option<Chain> {
        match self.0.pin().try_insert(id, chain) {
            Ok(_inserted) => None,
            Err(error) => Some(error.not_inserted),
        }
    }

    pub fn remove(&self, id: ChainId) -> Option<Chain> {
        self.0.pin().remove(&id).cloned()
    }

    pub fn items(&self) -> Vec<(ChainId, Chain)> {
        self.0
            .pin()
            .iter()
            .map(|(id, chain)| (*id, chain.clone()))
            .collect::<Vec<_>>()
    }

    pub fn chain_ids(&self) -> Vec<ChainId> {
        self.0.pin().keys().copied().collect::<Vec<_>>()
    }

    pub fn owned_chain_ids(&self) -> Vec<ChainId> {
        self.0
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
        self.0.pin().compute(chain_id, |chain| {
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

impl Extend<(ChainId, Chain)> for Memory {
    fn extend<It: IntoIterator<Item = (ChainId, Chain)>>(&mut self, chains: It) {
        let map = self.0.pin();
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
    use linera_base::{crypto::CryptoHash, data_types::Timestamp};

    use super::*;

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
        let memory = Memory::default();

        // Insert chains in non-sorted order using different hashes
        let id1 = ChainId(CryptoHash::test_hash("chain1"));
        let id2 = ChainId(CryptoHash::test_hash("chain2"));
        let id3 = ChainId(CryptoHash::test_hash("chain3"));

        memory.insert(id2, make_chain(2));
        memory.insert(id1, make_chain(1));
        memory.insert(id3, make_chain(3));

        // Serialize to JSON
        let json = serde_json::to_string_pretty(&memory).unwrap();

        // Deserialize back
        let restored: Memory = serde_json::from_str(&json).unwrap();

        // Verify data matches
        assert_eq!(restored.get(id1).unwrap().next_block_height, 1.into());
        assert_eq!(restored.get(id2).unwrap().next_block_height, 2.into());
        assert_eq!(restored.get(id3).unwrap().next_block_height, 3.into());
    }

    #[test]
    fn test_memory_serialization_is_sorted() {
        let memory = Memory::default();

        let id1 = ChainId(CryptoHash::test_hash("a"));
        let id2 = ChainId(CryptoHash::test_hash("b"));
        let id3 = ChainId(CryptoHash::test_hash("c"));

        // Insert in non-sorted order
        memory.insert(id3, make_chain(3));
        memory.insert(id1, make_chain(1));
        memory.insert(id2, make_chain(2));

        // Serialize and verify output keys are sorted
        let json = serde_json::to_string(&memory).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let keys: Vec<_> = value.as_object().unwrap().keys().collect();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        assert_eq!(keys, sorted_keys);
    }
}
