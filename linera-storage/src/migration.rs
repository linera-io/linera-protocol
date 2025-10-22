// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[derive(Default, Debug, Serialize, Deserialize)]
#[repr(u8)]
enum SchemaDescription {
    /// Version 0, all the blobs, certificates, confirmed blocks, events and network description on the same partition.
    Version0_SingleBlobPartition,
    /// Version 1, spreading by ChainId, CryptoHash, and BlobId.
    Version1_MultiBlobPartition,
}

/// The key containing the schema of the storage.
const SCHEMA_ROOT_KEY: &[u8] = [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233];



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

const EMPTY_KEY: &[u8] = [];
const MOVABLE_KEYS_0_1: &[u8] = [1, 2, 3, 4, 5, 7];

/// The total number of keys being migrated in a block.
/// we use chunks to avoid OOM
const BLOCK_KEY_SIZE: usize = 90;



fn map_base_key(base_key: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let base_key = bcs::from_bytes::<BaseKey>(base_key)?;
    match base_key {
        BaseKey::ChainState(chain_id) => {
            let root_key = RootKey::ChainState(chain_id).bytes();
            (root_key, ZERO_KEY.to_vec())
        }
        BaseKey::Certificate(hash) => {
            let root_key = RootKey::CryptoHash(hash).bytes();
            (root_key, DEFAULT_KEY.to_vec())
        }
        BaseKey::ConfirmedBlock(hash) => {
            let root_key = RootKey::CryptoHash(hash).bytes();
            (root_key, ONE_KEY.to_vec())
        }
        BaseKey::Blob(blob_id) => {
            let root_key = RootKey::Blob(blob_id).bytes();
            (root_key, DEFAULT_KEY.to_vec())
        }
        BaseKey::BlobState(blob_id) => {
            let root_key = RootKey::Blob(blob_id).bytes();
            (root_key, ONE_KEY.to_vec())
        }
        BaseKey::Event(event_id) => {
            let root_key = RootKey::Event(event_id.chain_id).bytes();
            let key = event_key(&event_id);
            (root_key, key)
        }
        BaseKey::BlockExporterState(index) => {
            let root_key = RootKey::NetworkDescription(index).bytes();
            (root_key, ZERO_KEY.to_vec())
        }
        BaseKey::NetworkDescription => {
            let root_key = RootKey::NetworkDescription.bytes();
            (root_key, DEFAULT_KEY.to_vec())
        }
    }
}


async fn migrate_key_set(database: &Database, base_keys: Vec<Vec<u8>>) -> Result<(), ViewError> {
    tracing::info!("migrate_key_set for |keys|={}", keys.len());
    for (index, chunk_base_keys) in base_keys.chunks(BLOCK_KEY_SIZE).enumerate() {
        tracing::info!("index={index} processing chunk of size {}", chunk_base_keys.len());
        let store = database.open_shared(&[])?;
        let values = store.read_multi_values_bytes(chunk_base_keys.clone()).await?;
        let values = values
            .map(|value| value.ok_or(ViewError::MissingEntries))
            .collect::<Result<Vec<Vec<u8>>,ViewError>>()?;
        let batch = MultiPartitionBatch::new();
        for (base_key, value) in chunk_keys.into_iter().zip(values) {
            let (root_key, key) = map_base_key(&base_key);
            batch.put_key_value_bytes(root_key, key, value);
        }
        database.write_batch(batch).await?;
        // Now delete the keys
        let batch = Batch::new();
        for key in chunk_base_keys {
            batch.delete_key(key);
        }
        store.write_batch(batch).await?;
    }
    Ok(())
}




async fn migrate_0_to_1(database: &Database) -> Result<(), ViewError> {
    for first_byte in MOVABLE_KEYS_0_1 {
        let store = database.open_shared(&[])?;
        let keys = store.find_keys_by_prefix(&[*first_byte]).await?;
        let base_keys = keys.map(|key| {
            let mut base_key = vec![*first_byte];
            base_key.extend(key);
            base_key
        }).collect::<Vec<Vec<u8>>();
        migrate_key_set(database, base_keys).await?;
    }
}
