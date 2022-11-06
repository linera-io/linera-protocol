use std::collections::HashMap;
use serde::{de::DeserializeOwned, Serialize};
use async_trait::async_trait;
use crate::views::ViewError;

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

/// Insert a put a key/value in the batch
pub fn put_item_batch(
    batch: &mut Batch,
    key: Vec<u8>,
    value: &impl Serialize,
) -> Result<(), bcs::Error> {
    let bytes = bcs::to_bytes(value)?;
    batch.operations.push(WriteOperation::Put { key, value: bytes });
    Ok(())
}

/// Delete a key and put that command into the batch
pub fn remove_item_batch(batch: &mut Batch, key: Vec<u8>) {
    batch.operations.push(WriteOperation::Delete { key });
}

/// Build a batch using builder. This is used for the macro.
pub async fn build_batch<F>(builder: F) -> Result<Batch, ViewError>
where
    F: FnOnce(&mut Batch) -> futures::future::BoxFuture<Result<(), ViewError>>
    + Send
    + Sync
{
    let mut batch = Batch::default();
    builder(&mut batch).await?;
    Ok(batch)
}

/// Low-level, asynchronous key-value operations. Useful for storage APIs not based on views.
#[async_trait]
pub trait KeyValueOperations {
    type E;
    async fn read_key<V: DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<V>, Self::E>;

    async fn write_key<V: Serialize + Sync>(
        &self,
        key: &[u8],
        value: &V,
    ) -> Result<(), Self::E>;

    async fn find_keys_with_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, Self::E>;

    async fn get_sub_keys<Key: DeserializeOwned + Send>(
        &mut self,
        key_prefix: &[u8],
    ) -> Result<Vec<Key>, Self::E>;

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::E>;
}

