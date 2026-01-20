// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::{AccountPublicKey, BcsSignable, CryptoHash},
    data_types::{
        Amount, Blob, ChainDescription, ChainOrigin, Epoch, InitialChainConfig, NetworkDescription,
        Timestamp,
    },
    identifiers::ChainId,
    ownership::ChainOwnership,
};
use linera_chain::ChainError;
use linera_execution::committee::Committee;
use linera_storage::Storage;
use serde::{Deserialize, Serialize};

/// The configuration of the genesis chains and committee.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenesisConfig {
    pub committee: Committee,
    pub timestamp: Timestamp,
    pub chains: Vec<ChainDescription>,
    pub network_name: String,
}

impl BcsSignable<'_> for GenesisConfig {}

fn make_chain(
    index: u32,
    public_key: AccountPublicKey,
    balance: Amount,
    timestamp: Timestamp,
) -> ChainDescription {
    let origin = ChainOrigin::Root(index);
    let config = InitialChainConfig {
        application_permissions: Default::default(),
        balance,
        min_active_epoch: Epoch::ZERO,
        max_active_epoch: Epoch::ZERO,
        epoch: Epoch::ZERO,
        ownership: ChainOwnership::single(public_key.into()),
    };
    ChainDescription::new(origin, config, timestamp)
}

impl GenesisConfig {
    /// Creates a `GenesisConfig` directly from a committee.
    pub fn new_from_committee(
        committee: Committee,
        timestamp: Timestamp,
        network_name: String,
        admin_public_key: AccountPublicKey,
        admin_balance: Amount,
    ) -> Self {
        let admin_chain = make_chain(0, admin_public_key, admin_balance, timestamp);
        Self {
            committee,
            timestamp,
            chains: vec![admin_chain],
            network_name,
        }
    }

    pub fn add_root_chain(
        &mut self,
        public_key: AccountPublicKey,
        balance: Amount,
    ) -> ChainDescription {
        let description = make_chain(
            self.chains.len() as u32,
            public_key,
            balance,
            self.timestamp,
        );
        self.chains.push(description.clone());
        description
    }

    pub fn admin_chain_description(&self) -> &ChainDescription {
        &self.chains[0]
    }

    pub fn admin_id(&self) -> ChainId {
        self.admin_chain_description().id()
    }

    pub async fn initialize_storage<S>(&self, storage: &mut S) -> Result<(), GenesisConfigError>
    where
        S: Storage + Clone + 'static,
    {
        if let Some(description) = storage
            .read_network_description()
            .await
            .map_err(ChainError::from)?
        {
            if description != self.network_description() {
                // We can't initialize storage with a different network description.
                tracing::error!(
                    current_network=?description,
                    new_network=?self.network_description(),
                    "storage already initialized"
                );
                return Err(GenesisConfigError::StorageIsAlreadyInitialized(Box::new(
                    description,
                )));
            }
            tracing::debug!(?description, "storage already initialized");
            return Ok(());
        }
        let network_description = self.network_description();
        storage
            .write_blob(&self.committee_blob())
            .await
            .map_err(ChainError::from)?;
        storage
            .write_network_description(&network_description)
            .await
            .map_err(ChainError::from)?;
        for description in &self.chains {
            storage.create_chain(description.clone()).await?;
        }
        Ok(())
    }

    pub fn hash(&self) -> CryptoHash {
        CryptoHash::new(self)
    }

    pub fn committee_blob(&self) -> Blob {
        Blob::new_committee(
            bcs::to_bytes(&self.committee).expect("serializing a committee should succeed"),
        )
    }

    pub fn network_description(&self) -> NetworkDescription {
        NetworkDescription {
            name: self.network_name.clone(),
            genesis_config_hash: CryptoHash::new(self),
            genesis_timestamp: self.timestamp,
            genesis_committee_blob_hash: self.committee_blob().id().hash,
            admin_chain_id: self.admin_id(),
        }
    }
}

/// Errors that can occur when working with genesis configuration.
#[derive(Debug, thiserror::Error)]
pub enum GenesisConfigError {
    #[error("chain error: {0}")]
    Chain(#[from] ChainError),
    #[error("storage is already initialized: {0:?}")]
    StorageIsAlreadyInitialized(Box<NetworkDescription>),
}
