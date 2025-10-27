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
    db_storage::{DbStorage, DEFAULT_KEY, to_event_key, MultiPartitionBatch, ONE_KEY, RootKey},
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
            let key = to_event_key(&event_id);
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
    Database::Error: From<bcs::Error> + Send + Sync,
{
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
        tracing::info!("migrate_storage_shared_partition with first_byte={first_byte} for |keys|={}", keys.len());
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
        let name = Database::get_name();
        if &name == "lru caching value splitting rocksdb internal" {
            return self.migrate_client_v0_to_v1().await;
        }
        if &name == "memory" || &name == "lru caching value splitting journaling dynamodb internal"
            || &name == "lru caching value splitting journaling scylladb internal"
        {
            return self.migrate_storage_v0_to_v1().await;
        }
        let error = format!("No support for migration of storage named {name}");
        Err(ViewError::NotFound(error))
    }

    async fn get_database_schema(&self) -> Result<SchemaDescription, ViewError> {
        let root_key = RootKey::SchemaDescription.bytes();
        let store = self.database.open_shared(&root_key)?;
        let value = store.read_value::<SchemaDescription>(DEFAULT_KEY).await?;
        let value = value.unwrap_or_default();
        Ok(value)
    }

    async fn write_database_schema(&self, schema: &SchemaDescription) -> Result<(), ViewError> {
        let root_key = RootKey::SchemaDescription.bytes();
        let mut batch = Batch::new();
        batch.put_key_value(DEFAULT_KEY.to_vec(), schema)?;
        let store = self.database.open_shared(&root_key)?;
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

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::CryptoHash,
        identifiers::{BlobId, BlobType, ChainId, EventId, GenericApplicationId, StreamId, StreamName},
    };
    #[cfg(feature = "rocksdb")]
    use linera_views::rocks_db::RocksDbDatabase;
    #[cfg(feature = "scylladb")]
    use linera_views::scylla_db::ScyllaDbDatabase;
    use linera_views::{
        batch::Batch,
        random::make_deterministic_rng,
        store::{KeyValueStore, TestKeyValueDatabase, KeyValueDatabase, ReadableKeyValueStore, WritableKeyValueStore},
        memory::MemoryDatabase,
        ViewError,
    };
    use rand::Rng;
    use test_case::test_case;

    use std::{collections::BTreeMap, marker::PhantomData, ops::Deref};

    use crate::{
        DbStorage,
        db_storage::RestrictedEventId,
        migration::{BaseKey, DEFAULT_KEY, ONE_KEY, RootKey},
        WallClock,
    };

    #[derive(Clone, Debug, Eq, PartialEq)]
    #[allow(clippy::type_complexity)]
    struct StorageState {
        chain_ids_key_values: BTreeMap<ChainId, Vec<(Vec<u8>,Vec<u8>)>>,
        certificates: BTreeMap<CryptoHash, Vec<u8>>,
        confirmed_blocks: BTreeMap<CryptoHash, Vec<u8>>,
        blobs: BTreeMap<BlobId, Vec<u8>>,
        blob_states: BTreeMap<BlobId, Vec<u8>>,
        events: BTreeMap<EventId, Vec<u8>>,
        block_exporter_states: BTreeMap<u32, Vec<(Vec<u8>, Vec<u8>)>>,
        network_description: Option<Vec<u8>>,
    }

    fn get_vector(rng: &mut impl Rng, len: usize) -> Vec<u8> {
        let mut v = Vec::new();
        for _ in 0..len {
            let value = rng.gen::<u8>();
            v.push(value);
        }
        v
    }

    fn get_hash(rng: &mut impl Rng) -> CryptoHash {
        let rnd_val = rng.gen::<usize>();
        CryptoHash::test_hash(format!("rnd_val={rnd_val}"))
    }

    fn get_stream_id(rng: &mut impl Rng) -> StreamId {
        let application_id = GenericApplicationId::System;
        let stream_name = StreamName(get_vector(rng, 10));
        StreamId { application_id, stream_name }
    }

    fn get_event_id(rng: &mut impl Rng) -> EventId {
        let hash = get_hash(rng);
        let chain_id = ChainId(hash);
        let stream_id = get_stream_id(rng);
        let index = rng.gen::<u32>();
        EventId { chain_id, stream_id, index }
    }

    fn reorder_key_values(key_values: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(Vec<u8>,Vec<u8>)> {
        let map = key_values
            .into_iter()
            .collect::<BTreeMap<Vec<u8>,Vec<u8>>>();
        map
            .into_iter()
            .collect::<Vec<(Vec<u8>,Vec<u8>)>>()
    }

    fn get_storage_state() -> StorageState {
        let mut rng = make_deterministic_rng();
        let key_size = 5;
        let value_size = 10;
        // 0: the chain states.
        let n_chain_id = 2;
        let n_key = 1;
        let mut chain_ids_key_values = BTreeMap::new();
        for _i_chain in 0..n_chain_id {
            let hash = get_hash(&mut rng);
            let chain_id = ChainId(hash);
            let mut key_values = Vec::new();
            for _i_key in 0..n_key {
                let key = get_vector(&mut rng, key_size);
                let value = get_vector(&mut rng, value_size);
                key_values.push((key, value));
            }
            chain_ids_key_values.insert(chain_id, reorder_key_values(key_values));
        }
        // 1: the certificates
        let n_certificate = 2;
        let mut certificates = BTreeMap::new();
        for _i_certificate in 0..n_certificate {
            let hash = get_hash(&mut rng);
            let value = get_vector(&mut rng, value_size);
            certificates.insert(hash, value);
        }
        // 2: the confirmed blocks
        let n_blocks = 2;
        let mut confirmed_blocks = BTreeMap::new();
        for _i_block in 0..n_blocks {
            let hash = get_hash(&mut rng);
            let value = get_vector(&mut rng, value_size);
            confirmed_blocks.insert(hash, value);
        }
        // 3: the blobs
        let n_blobs = 2;
        let mut blobs = BTreeMap::new();
        for _i_blob in 0..n_blobs {
            let hash = get_hash(&mut rng);
            let blob_id = BlobId { blob_type: BlobType::Data, hash };
            let value = get_vector(&mut rng, value_size);
            blobs.insert(blob_id, value);
        }
        // 4: the blob states
        let n_blob_states = 2;
        let mut blob_states = BTreeMap::new();
        for _i_blob_state in 0..n_blob_states {
            let hash = get_hash(&mut rng);
            let blob_id = BlobId { blob_type: BlobType::Data, hash };
            let value = get_vector(&mut rng, value_size);
            blob_states.insert(blob_id, value);
        }
        // 5: the events
        let n_events = 2;
        let mut events = BTreeMap::new();
        for _i_event in 0..n_events {
            let event_id = get_event_id(&mut rng);
            let value = get_vector(&mut rng, value_size);
            events.insert(event_id, value);
        }
        // 6: the block exports
        let n_block_exports = 2;
        let n_key = 1;
        let mut block_exporter_states = BTreeMap::new();
        for _i_block_export in 0..n_block_exports {
            let index = rng.gen::<u32>();
            let mut key_values = Vec::new();
            for _i_key in 0..n_key {
                let key = get_vector(&mut rng, key_size);
                let value = get_vector(&mut rng, value_size);
                key_values.push((key, value));
            }
            block_exporter_states.insert(index, reorder_key_values(key_values));
        }
        // 7: network description
        let network_description = Some(get_vector(&mut rng, value_size));
        StorageState {
            chain_ids_key_values,
            certificates,
            confirmed_blocks,
            blobs,
            blob_states,
            events,
            block_exporter_states,
            network_description,
        }
    }

    async fn write_storage_state_old_schema<D>(database: &D, storage_state: StorageState) -> Result<(), ViewError>
    where
        D: KeyValueDatabase + Clone + Send + Sync + 'static,
        D::Store: KeyValueStore + Clone + Send + Sync + 'static,
        D::Error: Send + Sync,
    {
        for (chain_id, key_values) in storage_state.chain_ids_key_values {
            let root_key = bcs::to_bytes(&BaseKey::ChainState(chain_id))?;
            let store = database.open_shared(&root_key)?;
            let mut batch = Batch::new();
            for (key, value) in key_values {
                batch.put_key_value_bytes(key, value);
            }
            store.write_batch(batch).await?;
        }
        for (index, key_values) in storage_state.block_exporter_states {
            let root_key = bcs::to_bytes(&BaseKey::BlockExporterState(index))?;
            let store = database.open_shared(&root_key)?;
            let mut batch = Batch::new();
            for (key, value) in key_values {
                batch.put_key_value_bytes(key, value);
            }
            store.write_batch(batch).await?;
        }
        // Writing in the shared partition
        let mut batch = Batch::new();
        for (hash, value) in storage_state.certificates {
            let key = bcs::to_bytes(&BaseKey::Certificate(hash))?;
            batch.put_key_value_bytes(key, value);
        }
        for (hash, value) in storage_state.confirmed_blocks {
            let key = bcs::to_bytes(&BaseKey::ConfirmedBlock(hash))?;
            batch.put_key_value_bytes(key, value);
        }
        for (blob_id, value) in storage_state.blobs {
            let key = bcs::to_bytes(&BaseKey::Blob(blob_id))?;
            batch.put_key_value_bytes(key, value);
        }
        for (blob_id, value) in storage_state.blob_states {
            let key = bcs::to_bytes(&BaseKey::BlobState(blob_id))?;
            batch.put_key_value_bytes(key, value);
        }
        for (event_id, value) in storage_state.events {
            let key = bcs::to_bytes(&BaseKey::Event(event_id))?;
            batch.put_key_value_bytes(key, value);
        }
        if let Some(network_description) = storage_state.network_description {
            let key = bcs::to_bytes(&BaseKey::NetworkDescription)?;
            batch.put_key_value_bytes(key, network_description);
        }
        let store = database.open_shared(&[])?;
        store.write_batch(batch).await?;
        Ok(())
    }

    fn is_valid_root_key(root_key: &[u8]) -> bool {
        if root_key.is_empty() {
            // It corresponds to the &[]
            return false;
        }
        if root_key == &[4] {
            // It corresponds to the key of the schema database.
            return false;
        }
        true
    }

    async fn read_storage_state_new_schema<D>(database: &D) -> Result<StorageState, ViewError>
    where
        D: KeyValueDatabase + Clone + Send + Sync + 'static,
        D::Store: KeyValueStore + Clone + Send + Sync + 'static,
        D::Error: Send + Sync,
    {
        let mut chain_ids_key_values = BTreeMap::new();
        let mut certificates = BTreeMap::new();
        let mut confirmed_blocks = BTreeMap::new();
        let mut blobs = BTreeMap::new();
        let mut blob_states = BTreeMap::new();
        let mut events = BTreeMap::new();
        let mut block_exporter_states = BTreeMap::new();
        let mut network_description = None;
        let bcs_root_keys = database.list_root_keys().await?;
        for bcs_root_key in bcs_root_keys {
            if is_valid_root_key(&bcs_root_key) {
                let root_key = bcs::from_bytes(&bcs_root_key)?;
                match root_key {
                    RootKey::ChainState(chain_id) => {
                        let store = database.open_shared(&bcs_root_key)?;
                        let key_values = store.find_key_values_by_prefix(&[]).await?;
                        chain_ids_key_values.insert(chain_id, key_values);
                    }
                    RootKey::CryptoHash(hash) => {
                        let store = database.open_shared(&bcs_root_key)?;
                        let value = store.read_value_bytes(DEFAULT_KEY).await?;
                        if let Some(value) = value {
                            certificates.insert(hash, value);
                        }
                        let value = store.read_value_bytes(ONE_KEY).await?;
                        if let Some(value) = value {
                            confirmed_blocks.insert(hash, value);
                        }
                    }
                    RootKey::Blob(blob_id) => {
                        let store = database.open_shared(&bcs_root_key)?;
                        let value = store.read_value_bytes(DEFAULT_KEY).await?;
                        if let Some(value) = value {
                            blobs.insert(blob_id, value);
                        }
                        let value = store.read_value_bytes(ONE_KEY).await?;
                        if let Some(value) = value {
                            blob_states.insert(blob_id, value);
                        }
                    }
                    RootKey::Event(chain_id) => {
                        let store = database.open_shared(&bcs_root_key)?;
                        let key_values = store.find_key_values_by_prefix(&[]).await?;
                        for (key, value) in key_values {
                            let restricted_event_id = bcs::from_bytes::<RestrictedEventId>(&key)?;
                            let event_id = EventId { chain_id, stream_id: restricted_event_id.stream_id, index: restricted_event_id.index };
                            events.insert(event_id, value);
                        }
                    }
                    RootKey::SchemaDescription => {
                        // Not part of the state
                    }
                    RootKey::NetworkDescription => {
                        let store = database.open_shared(&bcs_root_key)?;
                        let value = store.read_value_bytes(DEFAULT_KEY).await?;
                        if let Some(value) = value {
                            network_description = Some(value);
                        }
                    }
                    RootKey::BlockExporterState(index) => {
                        let store = database.open_shared(&bcs_root_key)?;
                        let key_values = store.find_key_values_by_prefix(&[]).await?;
                        block_exporter_states.insert(index, key_values);
                    }
                }
            }
        }
        Ok(StorageState {
            chain_ids_key_values,
            certificates,
            confirmed_blocks,
            blobs,
            blob_states,
            events,
            block_exporter_states,
            network_description,
        })
    }

    async fn test_storage_migration<D>() -> Result<(), ViewError>
    where
        D: TestKeyValueDatabase + Clone + Send + Sync + 'static,
        D::Store: KeyValueStore + Clone + Send + Sync + 'static,
        D::Error: Send + Sync,
    {
        let database = D::connect_test_namespace().await?;
        let storage_state = get_storage_state();
        write_storage_state_old_schema(&database, storage_state.clone()).await?;
        let storage = DbStorage::<D, WallClock>::new(database, None, WallClock);
        storage.migrate_if_needed().await?;
        let read_storage_state = read_storage_state_new_schema(storage.database.deref()).await?;
        assert_eq!(read_storage_state, storage_state);
        Ok(())
    }

    #[test_case(PhantomData::<MemoryDatabase>; "MemoryDatabase")]
    #[cfg_attr(with_rocksdb, test_case(PhantomData::<RocksDbDatabase>; "RocksDbDatabase"))]
    #[cfg_attr(with_scylladb, test_case(PhantomData::<ScyllaDbDatabase>; "ScyllaDbDatabase"))]
    #[tokio::test]
    async fn test_storage_migration_cases<D>(_storage_type: PhantomData<D>) -> Result<(), ViewError>
    where
        D: TestKeyValueDatabase + Clone + Send + Sync + 'static,
        D::Store: KeyValueStore + Clone + Send + Sync + 'static,
        D::Error: Send + Sync,
    {
        test_storage_migration::<D>().await
    }
}
