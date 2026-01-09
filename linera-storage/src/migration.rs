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
use tokio::time::Duration;

use crate::{
    db_storage::{
        to_event_key, DbStorage, MultiPartitionBatch, RootKey, BLOB_KEY, BLOB_STATE_KEY, BLOCK_KEY,
        LITE_CERTIFICATE_KEY, NETWORK_DESCRIPTION_KEY,
    },
    Clock,
};

#[derive(Debug)]
enum SchemaVersion {
    /// No schema version detected.
    Uninitialized,
    /// Version 0. All the blobs, certificates, confirmed blocks, events and network
    /// description are on the same partition.
    Version0,
    /// Version 1. New partitions are assigned by chain ID, crypto hash, and blob ID.
    Version1,
}

/// How long we should wait (in minutes) before retrying when we detect another migration
/// in progress.
const MIGRATION_WAIT_BEFORE_RETRY_MIN: u64 = 3;

const UNUSED_EMPTY_KEY: &[u8] = &[];
// We choose the ordering of the variants in `BaseKey` and `RootKey` so that
// the root keys corresponding to `ChainState` and `BlockExporter` remain the
// same in their serialization.
// This implies that data on those root keys do not need to be moved.
// For other tag variants, that are on the shared partition, we need to move
// the data into their own partitions.
const MOVABLE_KEYS_0_1: &[u8] = &[1, 2, 3, 4, 5, 7];

/// The total number of keys being migrated in a block.
/// We use chunks to avoid OOM.
const BLOCK_KEY_SIZE: usize = 90;

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

// We map a serialized `BaseKey` in the shared partition to a serialized `RootKey`
// and key in the new schema. For `ChainState` and `BlockExporterState`, there is
// no need to move so we use `UNUSED_EMPTY_KEY`.
fn map_base_key(base_key: &[u8]) -> Result<(Vec<u8>, Vec<u8>), ViewError> {
    let base_key = bcs::from_bytes::<BaseKey>(base_key)?;
    match base_key {
        BaseKey::ChainState(chain_id) => {
            let root_key = RootKey::ChainState(chain_id).bytes();
            Ok((root_key, UNUSED_EMPTY_KEY.to_vec()))
        }
        BaseKey::Certificate(hash) => {
            let root_key = RootKey::ConfirmedBlock(hash).bytes();
            Ok((root_key, LITE_CERTIFICATE_KEY.to_vec()))
        }
        BaseKey::ConfirmedBlock(hash) => {
            let root_key = RootKey::ConfirmedBlock(hash).bytes();
            Ok((root_key, BLOCK_KEY.to_vec()))
        }
        BaseKey::Blob(blob_id) => {
            let root_key = RootKey::Blob(blob_id).bytes();
            Ok((root_key, BLOB_KEY.to_vec()))
        }
        BaseKey::BlobState(blob_id) => {
            let root_key = RootKey::Blob(blob_id).bytes();
            Ok((root_key, BLOB_STATE_KEY.to_vec()))
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
            Ok((root_key, NETWORK_DESCRIPTION_KEY.to_vec()))
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
    async fn migrate_shared_partition(
        &self,
        first_byte: &u8,
        keys: Vec<Vec<u8>>,
    ) -> Result<(), ViewError> {
        tracing::info!(
            "Migrating {} keys of shared DB partition starting with {first_byte}",
            keys.len()
        );
        for (index, chunk_keys) in keys.chunks(BLOCK_KEY_SIZE).enumerate() {
            tracing::info!("Processing chunk {index} of size {}", chunk_keys.len());
            let chunk_base_keys = chunk_keys
                .iter()
                .map(|key| {
                    let mut base_key = vec![*first_byte];
                    base_key.extend(key);
                    base_key
                })
                .collect::<Vec<Vec<u8>>>();
            let store = self.database.open_shared(&[])?;
            let values = store.read_multi_values_bytes(&chunk_base_keys).await?;
            let mut batch = MultiPartitionBatch::new();
            for (base_key, value) in chunk_base_keys.iter().zip(values) {
                let value = value.ok_or_else(|| ViewError::MissingEntries("migration".into()))?;
                let (root_key, key) = map_base_key(base_key)?;
                batch.put_key_value(root_key, key, value);
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

    async fn migrate_v0_to_v1(&self) -> Result<(), ViewError> {
        for first_byte in MOVABLE_KEYS_0_1 {
            let store = self.database.open_shared(&[])?;
            let keys = store.find_keys_by_prefix(&[*first_byte]).await?;
            self.migrate_shared_partition(first_byte, keys).await?;
        }
        Ok(())
    }

    pub async fn migrate_if_needed(&self) -> Result<(), ViewError> {
        loop {
            if matches!(
                self.get_storage_state().await?,
                SchemaVersion::Uninitialized | SchemaVersion::Version1
            ) {
                // Nothing to do.
                return Ok(());
            }
            let result = self.migrate_v0_to_v1().await;
            if let Err(ViewError::MissingEntries(_)) = result {
                tracing::warn!(
                    "It looks like a migration is already in progress on this database. \
                     I will wait for {:?} minutes and retry.",
                    MIGRATION_WAIT_BEFORE_RETRY_MIN
                );
                // Duration::from_mins is not yet stable for tokio 1.36.
                tokio::time::sleep(Duration::from_secs(MIGRATION_WAIT_BEFORE_RETRY_MIN * 60)).await;
                continue;
            }
            return result;
        }
    }

    async fn get_storage_state(&self) -> Result<SchemaVersion, ViewError> {
        let store = self.database.open_shared(&[])?;
        let key = bcs::to_bytes(&BaseKey::NetworkDescription).unwrap();
        if store.contains_key(&key).await? {
            return Ok(SchemaVersion::Version0);
        }

        let root_key = RootKey::NetworkDescription.bytes();
        let store = self.database.open_shared(&root_key)?;
        if store.contains_key(NETWORK_DESCRIPTION_KEY).await? {
            return Ok(SchemaVersion::Version1);
        }

        Ok(SchemaVersion::Uninitialized)
    }

    /// Assert that the storage is at the last version (or not yet initialized).
    pub async fn assert_is_migrated_storage(&self) -> Result<(), ViewError> {
        let state = self.get_storage_state().await?;
        assert!(matches!(
            state,
            SchemaVersion::Uninitialized | SchemaVersion::Version1
        ));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        marker::PhantomData,
        ops::Deref,
    };

    use linera_base::{
        crypto::CryptoHash,
        identifiers::{BlobId, BlobType, ChainId, EventId, StreamId, StreamName},
    };
    #[cfg(feature = "rocksdb")]
    use linera_views::rocks_db::RocksDbDatabase;
    #[cfg(feature = "scylladb")]
    use linera_views::scylla_db::ScyllaDbDatabase;
    use linera_views::{
        batch::Batch,
        memory::MemoryDatabase,
        random::make_deterministic_rng,
        store::{
            KeyValueDatabase, KeyValueStore, ReadableKeyValueStore, TestKeyValueDatabase,
            WritableKeyValueStore,
        },
        ViewError,
    };
    use rand::{distributions, Rng};
    use test_case::test_case;

    use crate::{
        db_storage::RestrictedEventId,
        migration::{
            BaseKey, RootKey, BLOB_KEY, BLOB_STATE_KEY, BLOCK_KEY, LITE_CERTIFICATE_KEY,
            NETWORK_DESCRIPTION_KEY,
        },
        DbStorage, WallClock,
    };

    #[derive(Clone, Debug, Eq, PartialEq)]
    #[allow(clippy::type_complexity)]
    struct StorageState {
        chain_ids_key_values: BTreeMap<ChainId, Vec<(Vec<u8>, Vec<u8>)>>,
        certificates: BTreeMap<CryptoHash, Vec<u8>>,
        confirmed_blocks: BTreeMap<CryptoHash, Vec<u8>>,
        blobs: BTreeMap<BlobId, Vec<u8>>,
        blob_states: BTreeMap<BlobId, Vec<u8>>,
        events: HashMap<EventId, Vec<u8>>,
        block_exporter_states: BTreeMap<u32, Vec<(Vec<u8>, Vec<u8>)>>,
        network_description: Option<Vec<u8>>,
    }

    impl StorageState {
        fn append_storage_state(&mut self, storage_state: StorageState) {
            self.chain_ids_key_values
                .extend(storage_state.chain_ids_key_values);
            self.certificates.extend(storage_state.certificates);
            self.confirmed_blocks.extend(storage_state.confirmed_blocks);
            self.blobs.extend(storage_state.blobs);
            self.blob_states.extend(storage_state.blob_states);
            self.events.extend(storage_state.events);
            self.block_exporter_states
                .extend(storage_state.block_exporter_states);
            if let Some(value) = storage_state.network_description {
                assert!(self.network_description.is_none());
                self.network_description = Some(value);
            }
        }
    }

    fn create_vector(rng: &mut impl Rng, len: usize) -> Vec<u8> {
        rng.sample_iter(distributions::Standard).take(len).collect()
    }

    fn get_hash(rng: &mut impl Rng) -> CryptoHash {
        let rnd_val = rng.gen::<usize>();
        CryptoHash::test_hash(format!("rnd_val={rnd_val}"))
    }

    fn get_stream_id(rng: &mut impl Rng) -> StreamId {
        let stream_name = StreamName(create_vector(rng, 10));
        StreamId::system(stream_name)
    }

    fn get_event_id(rng: &mut impl Rng) -> EventId {
        let hash = get_hash(rng);
        let chain_id = ChainId(hash);
        let stream_id = get_stream_id(rng);
        let index = rng.gen::<u32>();
        EventId {
            chain_id,
            stream_id,
            index,
        }
    }

    fn get_storage_state() -> StorageState {
        let mut rng = make_deterministic_rng();
        let key_size = 5;
        let value_size = 10;
        // 0: the chain states.
        let chain_id_count = 10;
        let n_key = 1;
        let mut chain_ids_key_values = BTreeMap::new();
        for _i_chain in 0..chain_id_count {
            let hash = get_hash(&mut rng);
            let chain_id = ChainId(hash);
            let mut key_values = Vec::new();
            for _i_key in 0..n_key {
                let key = create_vector(&mut rng, key_size);
                let value = create_vector(&mut rng, value_size);
                key_values.push((key, value));
            }
            key_values.sort_unstable();
            chain_ids_key_values.insert(chain_id, key_values);
        }
        // 1: the certificates
        let certificates_count = 10;
        let mut certificates = BTreeMap::new();
        for _i_certificate in 0..certificates_count {
            let hash = get_hash(&mut rng);
            let value = create_vector(&mut rng, value_size);
            certificates.insert(hash, value);
        }
        // 2: the confirmed blocks (along with certificates)
        let blocks_count = 10;
        let mut confirmed_blocks = BTreeMap::new();
        for _i_block in 0..blocks_count {
            let hash = get_hash(&mut rng);
            let value = create_vector(&mut rng, value_size);
            certificates.insert(hash, value);
            let value = create_vector(&mut rng, value_size);
            confirmed_blocks.insert(hash, value);
        }
        // 3: the blobs
        let blobs_count = 2;
        let mut blobs = BTreeMap::new();
        for _i_blob in 0..blobs_count {
            let hash = get_hash(&mut rng);
            let blob_id = BlobId {
                blob_type: BlobType::Data,
                hash,
            };
            let value = create_vector(&mut rng, value_size);
            blobs.insert(blob_id, value);
        }
        // 4: the blob states
        let blob_states_count = 2;
        let mut blob_states = BTreeMap::new();
        for _i_blob_state in 0..blob_states_count {
            let hash = get_hash(&mut rng);
            let blob_id = BlobId {
                blob_type: BlobType::Data,
                hash,
            };
            let value = create_vector(&mut rng, value_size);
            blob_states.insert(blob_id, value);
        }
        // 5: the events
        let events_count = 2;
        let mut events = HashMap::new();
        for _i_event in 0..events_count {
            let event_id = get_event_id(&mut rng);
            let value = create_vector(&mut rng, value_size);
            events.insert(event_id, value);
        }
        // 6: the block exporters
        let block_exporters_count = 2;
        let n_key = 1;
        let mut block_exporter_states = BTreeMap::new();
        for _i_block_export in 0..block_exporters_count {
            let index = rng.gen::<u32>();
            let mut key_values = Vec::new();
            for _i_key in 0..n_key {
                let key = create_vector(&mut rng, key_size);
                let value = create_vector(&mut rng, value_size);
                key_values.push((key, value));
            }
            key_values.sort_unstable();
            block_exporter_states.insert(index, key_values);
        }
        // 7: network description
        let network_description = Some(create_vector(&mut rng, value_size));
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

    async fn write_storage_state_old_schema<D>(
        database: &D,
        storage_state: StorageState,
    ) -> Result<(), ViewError>
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
        if root_key == [4] {
            // It corresponds to the key of the database schema.
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
        let mut events = HashMap::new();
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
                    RootKey::ConfirmedBlock(hash) => {
                        let store = database.open_shared(&bcs_root_key)?;
                        let value = store.read_value_bytes(LITE_CERTIFICATE_KEY).await?;
                        if let Some(value) = value {
                            certificates.insert(hash, value);
                        }
                        let value = store.read_value_bytes(BLOCK_KEY).await?;
                        if let Some(value) = value {
                            confirmed_blocks.insert(hash, value);
                        }
                    }
                    RootKey::Blob(blob_id) => {
                        let store = database.open_shared(&bcs_root_key)?;
                        let value = store.read_value_bytes(BLOB_KEY).await?;
                        if let Some(value) = value {
                            blobs.insert(blob_id, value);
                        }
                        let value = store.read_value_bytes(BLOB_STATE_KEY).await?;
                        if let Some(value) = value {
                            blob_states.insert(blob_id, value);
                        }
                    }
                    RootKey::Event(chain_id) => {
                        let store = database.open_shared(&bcs_root_key)?;
                        let key_values = store.find_key_values_by_prefix(&[]).await?;
                        for (key, value) in key_values {
                            let restricted_event_id = bcs::from_bytes::<RestrictedEventId>(&key)?;
                            let event_id = EventId {
                                chain_id,
                                stream_id: restricted_event_id.stream_id,
                                index: restricted_event_id.index,
                            };
                            events.insert(event_id, value);
                        }
                    }
                    RootKey::Placeholder => {
                        // Nothing to be done
                    }
                    RootKey::NetworkDescription => {
                        let store = database.open_shared(&bcs_root_key)?;
                        let value = store.read_value_bytes(NETWORK_DESCRIPTION_KEY).await?;
                        if let Some(value) = value {
                            network_description = Some(value);
                        }
                    }
                    RootKey::BlockExporterState(index) => {
                        let store = database.open_shared(&bcs_root_key)?;
                        let key_values = store.find_key_values_by_prefix(&[]).await?;
                        block_exporter_states.insert(index, key_values);
                    }
                    RootKey::BlockByHeight(_, _) => {
                        // Nothing to be done
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
        // Get a storage state and write it.
        let mut storage_state = get_storage_state();
        write_storage_state_old_schema(&database, storage_state.clone()).await?;
        // Creating a storage and migrate to the new database schema.
        let storage = DbStorage::<D, WallClock>::new(database, None, WallClock);
        storage.migrate_if_needed().await?;
        // read the storage state and compare it.
        let read_storage_state = read_storage_state_new_schema(storage.database.deref()).await?;
        assert_eq!(read_storage_state, storage_state);
        // Creates a new storage state, write it and migrate it.
        // That should simulate the partial migration interrupted for some reason and restarted.
        let mut appended_state = get_storage_state();
        appended_state.network_description = None;
        write_storage_state_old_schema(storage.database.deref(), appended_state.clone()).await?;
        storage.migrate_if_needed().await?;
        storage_state.append_storage_state(appended_state);
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
