// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::identifiers::{
    ChainId,
    AccountOwner,
};

use linera_base::data_types::{
    BlockHeight,
    Epoch,
    Timestamp,
};

use linera_base::crypto::CryptoHash;

use crate::client::PendingProposal;
use crate::data_types::ChainInfo;

#[derive(Default, Clone)]
pub struct Chain {
    pub owner: Option<AccountOwner>,
    pub block_hash: Option<CryptoHash>,
    pub next_block_height: BlockHeight,
    pub timestamp: Timestamp,
    pub pending_proposal: Option<PendingProposal>,
    pub epoch: Option<Epoch>,
}

impl From<ChainInfo> for Chain {
    fn from(info: ChainInfo) -> Self {
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

impl Chain {
    /// Create a chain that we own.
    pub fn new(
        owner: AccountOwner,
        current_epoch: Epoch,
        now: Timestamp,
    ) -> Self {
        Self {
            owner: Some(owner),
            block_hash: None,
            timestamp: now,
            next_block_height: BlockHeight::ZERO,
            pending_proposal: None,
            epoch: Some(current_epoch),
        }
    }
}

pub trait Wallet {
    type Error;
    async fn read(&self, id: ChainId) -> Result<Option<Chain>, Self::Error>;
    async fn write(&self, id: ChainId, chain: Chain) -> Result<(), Self::Error>;
    async fn remove(&self, id: ChainId) -> Result<Option<Chain>, Self::Error>;
}

pub struct Memory(papaya::HashMap<ChainId, Chain>);

impl Wallet for Memory {
    type Error = std::convert::Infallible;

    async fn read(&self, id: ChainId) -> Result<Option<Chain>, Self::Error> {
        Ok(self.0.pin().get(&id).cloned())
    }

    async fn write(&self, id: ChainId, chain: Chain) -> Result<(), Self::Error> {
        let _old_value = self.0.pin().insert(id, chain);
        Ok(())
    }

    async fn remove(&self, id: ChainId) -> Result<Option<Chain>, Self::Error> {
        Ok(self.0.pin().remove(&id).cloned())
    }
}
