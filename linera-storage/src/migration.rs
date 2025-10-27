// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{
    crypto::CryptoHash,
    identifiers::{BlobId, ChainId, EventId},
};
use linera_views::{
    batch::Batch,
    store::{KeyValueDatabase, KeyValueStore, ReadableKeyValueStore, WritableKeyValueStore},
    ViewError,
};
use serde::{Deserialize, Serialize};


use crate::{
    Clock,
    db_storage::{DbStorage, DEFAULT_KEY, event_key, MultiPartitionBatch, ONE_KEY, RootKey},
};

#[derive(Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
enum SchemaDescription {
    /// Version 0, all the blobs, certificates, confirmed blocks, events and network description on the same partition.
    /// This is marked as default since it does not exist in the old scheme and would be obtained from unwrap_or_default
    #[default]
    Version0,
    /// Version 1, spreading by ChainId, CryptoHash, and BlobId.
    Version1,
}

/// The key containing the schema of the storage.
const SCHEMA_ROOT_KEY: &[u8] = &[0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233];

#[derive(Debug, Serialize, Deserialize)]
enum BaseKey {
    ChainState(ChainId),
    Certificate(CryptoHash),
    ConfirmedBlock(CryptoHash),
    Blob(BlobId),
    BlobState(BlobId),
    Event(EventId),
    BlockExporterState(u32),
    NetworkDescription,
}

const BASE_KEY_BLOCK_EXPORTER: u8 = 6;

const UNUSED_EMPTY_KEY: &[u8] = &[];
// The keys corresponding to `ChainState` are not moved.
// The keys in `BlockExporterState` are moved, but belong to separate root keys.
const MOVABLE_KEYS_0_1: &[u8] = &[1, 2, 3, 4, 5, 7];

/// The total number of keys being migrated in a block.
/// we use chunks to avoid OOM
const BLOCK_KEY_SIZE: usize = 90;

fn map_base_key(base_key: &[u8]) -> Result<(Vec<u8>, Vec<u8>), ViewError> {
    let base_key = bcs::from_bytes::<BaseKey>(base_key)?;
    match base_key {
        BaseKey::ChainState(chain_id) => {
            let root_key = RootKey::ChainState(chain_id).bytes();
            Ok((root_key, UNUSED_EMPTY_KEY.to_vec()))
        }
        BaseKey::Certificate(hash) => {
            let root_key = RootKey::CryptoHash(hash).bytes();
            Ok((root_key, DEFAULT_KEY.to_vec()))
        }
        BaseKey::ConfirmedBlock(hash) => {
            let root_key = RootKey::CryptoHash(hash).bytes();
            Ok((root_key, ONE_KEY.to_vec()))
        }
        BaseKey::Blob(blob_id) => {
            let root_key = RootKey::Blob(blob_id).bytes();
            Ok((root_key, DEFAULT_KEY.to_vec()))
        }
        BaseKey::BlobState(blob_id) => {
            let root_key = RootKey::Blob(blob_id).bytes();
            Ok((root_key, ONE_KEY.to_vec()))
        }
        BaseKey::Event(event_id) => {
            let root_key = RootKey::Event(event_id.chain_id).bytes();
            let key = event_key(&event_id);
            Ok((root_key, key))
        }
        BaseKey::BlockExporterState(index) => {
            let root_key = RootKey::BlockExporterState(index).bytes();
            Ok((root_key, UNUSED_EMPTY_KEY.to_vec()))
        }
        BaseKey::NetworkDescription => {
            let root_key = RootKey::NetworkDescription.bytes();
            Ok((root_key, DEFAULT_KEY.to_vec()))
        }
    }
}

impl<Database, C> DbStorage<Database, C>
where
    Database: KeyValueDatabase + Clone + Send + Sync + 'static,
    Database::Store: KeyValueStore + Clone + Send + Sync + 'static,
    C: Clock + Clone + Send + Sync + 'static,
    Database::Error: Send + Sync,
{
    async fn migrate_single_block_export_partition(
        &self,
        root_base_key: Vec<u8>,
    ) -> Result<(), ViewError> {
        let store_read = self.database.open_exclusive(&root_base_key)?;
        let key_values = store_read.find_key_values_by_prefix(&[]).await?;
        let root_key = map_base_key(&root_base_key)?.0;
        let mut batch_write = Batch::new();
        let mut batch_delete = Batch::new();
        for (key, value) in key_values {
            batch_write.put_key_value_bytes(key.clone(), value);
            batch_delete.delete_key(key);
        }
        let store_write = self.database.open_exclusive(&root_key)?;
        store_write.write_batch(batch_write).await?;
        store_read.write_batch(batch_delete).await?;
        Ok(())
    }

    async fn migrate_all_block_exports_partitions(&self) -> Result<(), ViewError> {
        let root_keys = self.database.list_root_keys().await?;
        for root_key in root_keys {
            if !root_key.is_empty() && root_key[0] == BASE_KEY_BLOCK_EXPORTER {
                self.migrate_single_block_export_partition(root_key).await?;
            }
        }
        Ok(())
    }


    async fn migrate_storage_shared_partition(
        &self,
        first_byte: &u8,
        keys: Vec<Vec<u8>>,
    ) -> Result<(), ViewError> {
        tracing::info!(
            "migrate_storage_shared_partition with first_byte={first_byte} for |base_keys|={}",
            keys.len()
        );
        for (index, chunk_keys) in keys.chunks(BLOCK_KEY_SIZE).enumerate() {
            tracing::info!(
                "index={index} processing chunk of size {}",
                chunk_keys.len()
            );
            let chunk_base_keys = chunk_keys
                .iter()
                .map(|key| {
                    let mut base_key = vec![*first_byte];
                    base_key.extend(key);
                    base_key
                })
                .collect::<Vec<Vec<u8>>>();
            let store = self.database.open_shared(&[])?;
            let values = store
                .read_multi_values_bytes(chunk_base_keys.to_vec())
                .await?;
            let mut batch = MultiPartitionBatch::new();
            for (base_key, value) in chunk_base_keys.iter().zip(values) {
                let value = value.ok_or(ViewError::MissingEntries)?;
                tracing::info!("base_key={base_key:?} value={value:?}");
                let (root_key, key) = map_base_key(base_key)?;
                batch.put_key_value_bytes(root_key, key, value);
            }
            self.write_batch(batch).await?;
            // Now delete the keys
            let mut batch = Batch::new();
            for key in chunk_base_keys {
                batch.delete_key(key.to_vec());
            }
            store.write_batch(batch).await?;
        }
        Ok(())
    }


    async fn migrate_client_shared_partition(
        &self,
        first_byte: &u8,
        keys: Vec<Vec<u8>>,
    ) -> Result<(), ViewError> {
        tracing::info!(
            "migrate_storage_shared_partition with first_byte={first_byte} for |base_keys|={}",
            keys.len()
        );
        for (index, chunk_keys) in keys.chunks(BLOCK_KEY_SIZE).enumerate() {
            tracing::info!(
                "index={index} processing chunk of size {}",
                chunk_keys.len()
            );
            // full_keys, since they are of the form root_key + key
            let chunk_base_keys = chunk_keys
                .iter()
                .map(|key| {
                    let mut base_key = vec![*first_byte];
                    base_key.extend(key);
                    base_key
                })
                .collect::<Vec<Vec<u8>>>();
            let store = self.database.open_shared(&[])?;
            let values = store
                .read_multi_values_bytes(chunk_base_keys.to_vec())
                .await?;
            let values = values
                .into_iter()
                .map(|value| value.ok_or(ViewError::MissingEntries))
                .collect::<Result<Vec<Vec<u8>>, ViewError>>()?;
            let mut batch = MultiPartitionBatch::new();
            for (base_key, value) in chunk_base_keys.iter().zip(values) {
                tracing::info!("base_key={base_key:?} value={value:?}");
                let (root_key, key) = map_base_key(base_key)?;
                batch.put_key_value_bytes(root_key, key, value);
            }
            self.write_batch(batch).await?;
            // Now delete the keys
            let mut batch = Batch::new();
            for key in chunk_base_keys {
                batch.delete_key(key.to_vec());
            }
            store.write_batch(batch).await?;
        }
        Ok(())
    }


    async fn migrate_storage_v0_to_v1(&self) -> Result<(), ViewError> {
        for first_byte in MOVABLE_KEYS_0_1 {
            let store = self.database.open_shared(&[])?;
            let keys = store.find_keys_by_prefix(&[*first_byte]).await?;
            self.migrate_storage_shared_partition(first_byte, keys)
                .await?;
            // Some keys can be left in the value-splitting.
            let mut batch = Batch::new();
            batch.delete_key_prefix(vec![*first_byte]);
            store.write_batch(batch).await?;
        }
        Ok(())
    }

    async fn migrate_client_v0_to_v1(&self) -> Result<(), ViewError> {
        for first_byte in MOVABLE_KEYS_0_1 {
            let store = self.database.open_shared(&[])?;
            let keys = store.find_keys_by_prefix(&[*first_byte]).await?;
            self.migrate_client_shared_partition(first_byte, keys)
                .await?;
        }
        Ok(())
    }
    async fn migrate_v0_to_v1(&self) -> Result<(), ViewError> {
        self.migrate_all_block_exports_partitions().await?;
        let name = Database::get_name();
        if &name == "lru caching value splitting rocksdb internal" {
            return self.migrate_client_v0_to_v1().await;
        }
        if &name == "lru caching value splitting journaling dynamodb internal"
            || &name == "lru caching value splitting journaling scylladb internal"
        {
            return self.migrate_storage_v0_to_v1().await;
        }
        let error = format!("No support for migration of storage named {name}");
        Err(ViewError::NotFound(error))
    }

    async fn get_database_schema(&self) -> Result<SchemaDescription, ViewError> {
        let store = self.database.open_shared(SCHEMA_ROOT_KEY)?;
        let value = store.read_value::<SchemaDescription>(DEFAULT_KEY).await?;
        let value = value.unwrap_or_default();
        Ok(value)
    }

    async fn write_database_schema(&self, schema: &SchemaDescription) -> Result<(), ViewError> {
        let mut batch = Batch::new();
        batch.put_key_value(DEFAULT_KEY.to_vec(), schema)?;
        let store = self.database.open_shared(SCHEMA_ROOT_KEY)?;
        Ok(store.write_batch(batch).await?)
    }

    pub async fn migrate_if_needed(&self) -> Result<(), ViewError> {
        let schema = self.get_database_schema().await?;
        if schema == SchemaDescription::Version0 {
            self.migrate_v0_to_v1().await?;
        }
        self.write_database_schema(&SchemaDescription::Version1)
            .await?;
        Ok(())
    }

    pub async fn assert_is_migrated_database(&self) -> Result<(), ViewError> {
        let schema = self.get_database_schema().await?;
        assert_eq!(schema, SchemaDescription::Version1);
        Ok(())
    }

}
