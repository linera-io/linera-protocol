// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{error::Error, sync::Arc};

use linera_base::{crypto::CryptoHash, data_types::Blob, identifiers::BlobId};
use linera_chain::types::ConfirmedBlock;
use linera_sdk::{
    bcs,
    views::{View, ViewError},
};
use linera_service::storage::StoreConfig;
use linera_storage::BaseKey;
use linera_storage_service::client::ServiceStoreClient;
#[cfg(feature = "dynamodb")]
use linera_views::dynamo_db::DynamoDbStore;
#[cfg(feature = "scylladb")]
use linera_views::scylla_db::ScyllaDbStore;
use linera_views::{
    batch::Batch,
    context::ViewContext,
    memory::{MemoryStore, MemoryStoreConfig},
    rocks_db::RocksDbStore,
    store::KeyValueStore,
};
#[cfg(with_testing)]
use linera_views::{random::generate_test_namespace, store::TestKeyValueStore};
#[cfg(all(feature = "rocksdb", feature = "scylladb"))]
use {linera_storage::ChainStatesFirstAssignment, linera_views::backends::dual::DualStore};

use crate::{state::BlockExporterStateView, ExporterContext, ExporterError, Generic};

const BASE_KEY: &str = "exporter";

/// Extension for the [`DbStorage`] type to abstract the persistant storage and operate in the context of [`BlockExporterStateView`].
#[derive(Clone)]
pub(crate) struct ExporterStorage<Store> {
    store: Arc<Store>,
}

impl<Store> ExporterStorage<Store>
where
    Store: KeyValueStore + Clone + Send + Sync + 'static,
    Store::Error: Error + Send + Sync,
{
    pub(super) async fn load_exporter_state(
        &self,
    ) -> Result<BlockExporterStateView<ViewContext<(), Store>>, ViewError> {
        let root_key = bcs::to_bytes(BASE_KEY)?;
        let store = self.store.clone_with_root_key(&root_key)?;
        let context = ViewContext::create_root_context(store, ()).await?;
        BlockExporterStateView::load(context).await
    }

    pub(super) async fn contains_blob(&self, blob_id: BlobId) -> Result<bool, ViewError> {
        let blob_key = bcs::to_bytes(&BaseKey::Blob(blob_id))?;
        let exists = self.store.contains_key(&blob_key).await?;
        Ok(exists)
    }

    pub(super) async fn missing_blobs(
        &self,
        blob_ids: &[BlobId],
    ) -> Result<Vec<BlobId>, ViewError> {
        let mut keys = Vec::new();
        for blob_id in blob_ids {
            let key = bcs::to_bytes(&BaseKey::Blob(*blob_id))?;
            keys.push(key);
        }

        let results = self.store.contains_keys(keys).await?;
        let mut missing_blobs = Vec::new();
        for (blob_id, result) in blob_ids.iter().zip(results) {
            if !result {
                missing_blobs.push(*blob_id);
            }
        }

        Ok(missing_blobs)
    }

    pub(super) async fn read_blob(&self, blob_id: BlobId) -> Result<Blob, ViewError> {
        let blob_key = bcs::to_bytes(&BaseKey::Blob(blob_id))?;
        let maybe_blob_bytes = self.store.read_value::<Vec<u8>>(&blob_key).await?;
        let blob_bytes = maybe_blob_bytes.ok_or_else(|| ViewError::BlobsNotFound(vec![blob_id]))?;
        Ok(Blob::new_with_id_unchecked(blob_id, blob_bytes))
    }

    pub(super) async fn read_blobs(
        &self,
        blob_ids: &[BlobId],
    ) -> Result<Vec<Option<Blob>>, ViewError> {
        if blob_ids.is_empty() {
            return Ok(Vec::new());
        }
        let blob_keys = blob_ids
            .iter()
            .map(|blob_id| bcs::to_bytes(&BaseKey::Blob(*blob_id)))
            .collect::<Result<Vec<_>, _>>()?;
        let maybe_blob_bytes = self.store.read_multi_values::<Vec<u8>>(blob_keys).await?;

        Ok(blob_ids
            .iter()
            .zip(maybe_blob_bytes)
            .map(|(blob_id, maybe_blob_bytes)| {
                maybe_blob_bytes.map(|blob_bytes| Blob::new_with_id_unchecked(*blob_id, blob_bytes))
            })
            .collect())
    }

    pub(super) async fn read_confirmed_block(
        &self,
        hash: CryptoHash,
    ) -> Result<ConfirmedBlock, ViewError> {
        let block_key = bcs::to_bytes(&BaseKey::ConfirmedBlock(hash))?;
        let maybe_value = self.store.read_value::<ConfirmedBlock>(&block_key).await?;
        let value = maybe_value.ok_or_else(|| ViewError::not_found("value for hash", hash))?;
        Ok(value)
    }

    pub(super) async fn read_confirmed_blocks_downward(
        &self,
        from: CryptoHash,
        limit: u32,
    ) -> Result<Vec<ConfirmedBlock>, ViewError> {
        let mut hash = Some(from);
        let mut values = Vec::new();
        for _ in 0..limit {
            let Some(next_hash) = hash else {
                break;
            };
            let value = self.read_confirmed_block(next_hash).await?;
            hash = value.block().header.previous_block_hash;
            values.push(value);
        }
        Ok(values)
    }
}

impl<Store> ExporterStorage<Store>
where
    Store: KeyValueStore + Clone + Send + Sync + 'static,
    Store::Error: Error + Send + Sync,
{
    pub(super) async fn write_batch(&self, batch: Batch) -> Result<(), ViewError> {
        self.store.write_batch(batch).await?;
        Ok(())
    }

    pub(super) fn create(store: Store) -> Self {
        Self {
            store: Arc::new(store),
        }
    }
}

impl<Store> ExporterStorage<Store>
where
    Store: KeyValueStore + Clone + Send + Sync + 'static,
    Store::Error: Error + Send + Sync,
{
    pub async fn initialize(config: Store::Config, namespace: &str) -> Result<Self, Store::Error> {
        let store = Store::maybe_create_and_connect(&config, namespace).await?;
        Ok(Self::create(store))
    }

    pub async fn new(config: Store::Config, namespace: &str) -> Result<Self, Store::Error> {
        let store = Store::connect(&config, namespace).await?;
        Ok(Self::create(store))
    }
}

#[cfg(with_testing)]
impl<Store> ExporterStorage<Store>
where
    Store: TestKeyValueStore + Clone + Send + Sync + 'static,
    Store::Error: Error + Send + Sync,
{
    pub async fn make_test_storage() -> Self {
        let config = Store::new_test_config().await.unwrap();
        let namespace = generate_test_namespace();
        ExporterStorage::<Store>::new_for_testing(config, &namespace)
            .await
            .unwrap()
    }

    pub async fn new_for_testing(
        config: Store::Config,
        namespace: &str,
    ) -> Result<Self, Store::Error> {
        let store = Store::recreate_and_connect(&config, namespace).await?;
        Ok(Self::create(store))
    }
}

pub(super) async fn run_exporter_with_storage(
    config: StoreConfig,
    context: ExporterContext,
) -> Result<(), ExporterError> {
    match config {
        StoreConfig::Memory(config, namespace) => {
            let store_config = MemoryStoreConfig::new(config.common_config.max_stream_queries);
            let storage = ExporterStorage::<MemoryStore>::new(store_config, &namespace)
                .await
                .into_unknown()?;
            context.run(storage).await
        }

        #[cfg(feature = "storage-service")]
        StoreConfig::Service(config, namespace) => {
            let storage = ExporterStorage::<ServiceStoreClient>::new(config, &namespace)
                .await
                .into_unknown()?;
            context.run(storage).await
        }

        #[cfg(feature = "rocksdb")]
        StoreConfig::RocksDb(config, namespace) => {
            let storage = ExporterStorage::<RocksDbStore>::new(config, &namespace)
                .await
                .into_unknown()?;
            context.run(storage).await
        }

        #[cfg(feature = "dynamodb")]
        StoreConfig::DynamoDb(config, namespace) => {
            let storage = ExporterStorage::<DynamoDbStore>::new(config, &namespace)
                .await
                .into_unknown()?;
            context.run(storage).await
        }

        #[cfg(feature = "scylladb")]
        StoreConfig::ScyllaDb(config, namespace) => {
            let storage = ExporterStorage::<ScyllaDbStore>::new(config, &namespace)
            .await
            .into_unknown()?;
            context.run(storage).await
        }

        #[cfg(all(feature = "rocksdb", feature = "scylladb"))]
        StoreConfig::DualRocksDbScyllaDb(config, namespace) => {
            let storage = ExporterStorage::<
                DualStore<RocksDbStore, ScyllaDbStore, ChainStatesFirstAssignment>,
            >::new(config, &namespace)
            .await
            .into_unknown()?;
            context.run(storage).await
        }
    }
}
