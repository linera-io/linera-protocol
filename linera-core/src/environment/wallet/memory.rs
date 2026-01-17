// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use futures::{Stream, StreamExt as _};
use linera_base::identifiers::ChainId;

use super::{Chain, Wallet};

/// A basic implementation of `Wallet` that doesn't persist anything and merely tracks the
/// chains in memory.
///
/// This can be used as-is as an ephemeral wallet for testing or ephemeral clients, or as
/// a building block for more complex wallets that layer persistence on top of it.
#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Memory(papaya::HashMap<ChainId, Chain>);

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
