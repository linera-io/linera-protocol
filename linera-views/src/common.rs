use std::collections::HashMap;
use serde::{de::DeserializeOwned, Serialize};
use async_trait::async_trait;

pub enum WriteOperation {
    Delete { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
}

/// A batch of writes inside a transaction;
#[derive(Default)]
pub struct Batch{ pub operations: Vec<WriteOperation>}

/// A key may appear multiple times in the batch
/// The construction of BatchWriteItem and TransactWriteItem does
/// not allow for this to happen.
pub fn simplify_batch(batch: Batch) -> Batch {
    let mut map = HashMap::new();
    for op in batch.operations {
        match op {
            WriteOperation::Delete { key } => map.insert(key, None),
            WriteOperation::Put { key, value } => map.insert(key, Some(value)),
        };
    }
    let mut operations = Vec::with_capacity(map.len());
    for (key, val) in map {
	match val {
            Some(value) => operations.push(WriteOperation::Put { key, value }),
            None => operations.push(WriteOperation::Delete { key }),
        }
    }
    Batch{ operations }
}

/// Low-level, asynchronous key-value operations. Useful for storage APIs not based on views.
#[async_trait]
pub trait KeyValueOperations<E> {
    async fn read_key<V: DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<V>, E>;

    async fn write_key<V: Serialize + Sync>(
        &self,
        key: &[u8],
        value: &V,
    ) -> Result<(), E>;

    async fn find_keys_with_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, E>;

    async fn get_sub_keys<Key: DeserializeOwned + Send>(
        &mut self,
        key_prefix: &[u8],
    ) -> Result<Vec<Key>, E>;
}

