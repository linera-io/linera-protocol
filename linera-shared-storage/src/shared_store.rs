use linera_service::storage::StorageConfig;
use linera_views::common::CommonStoreConfig;
use tonic::{transport::Server, Request, Response, Status};
//use std::process;
use crate::key_value_store::{
    statement::Operation,
    store_processor_server::{StoreProcessor, StoreProcessorServer},
    store_reply::Reply,
    store_request::Query,
    KeyValue, KeyValues, Keys, OptValue, OptValues, StoreReply, StoreRequest,
};
use linera_views::memory::{create_memory_store, MemoryStore};

use linera_views::common::{ReadableKeyValueStore, WritableKeyValueStore};
//use crate::key_value_store::store_response::Pattern::Unit;

#[allow(clippy::derive_partial_eq_without_eq)]
// https://github.com/hyperium/tonic/issues/1056
pub mod key_value_store {
    tonic::include_proto!("key_value_store.v1");
}

pub struct Storage {
    memory_store: MemoryStore,
}

#[derive(clap::Parser)]
struct SharedStoreOptions {
    /// Subcommands. Acceptable values are run and generate.
    #[arg(long = "storage")]
    storage_config: String,

    #[arg(long = "endpoint")]
    endpoint: String,
}

#[tonic::async_trait]
impl StoreProcessor for Storage {
    async fn store_process(
        &self,
        request: Request<StoreRequest>,
    ) -> Result<Response<StoreReply>, Status> {
        use crate::key_value_store::UnitType;
        let request = request.get_ref();
        let response = match request.query.clone().unwrap() {
            Query::ReadValue(key) => {
                let value = self.memory_store.read_value_bytes(&key).await.unwrap();
                let value = OptValue { value };
                StoreReply {
                    reply: Some(Reply::ReadValue(value)),
                }
            }
            Query::ContainsKey(key) => {
                let test = self.memory_store.contains_key(&key).await.unwrap();
                StoreReply {
                    reply: Some(Reply::ContainsKey(test)),
                }
            }
            Query::ReadMultiValues(keys) => {
                let values = self
                    .memory_store
                    .read_multi_values_bytes(keys.keys)
                    .await
                    .unwrap();
                let values = values
                    .into_iter()
                    .map(|value| OptValue { value })
                    .collect::<Vec<_>>();
                let values = OptValues { values };
                StoreReply {
                    reply: Some(Reply::ReadMultiValues(values)),
                }
            }
            Query::FindKeysByPrefix(key_prefix) => {
                let keys = self
                    .memory_store
                    .find_keys_by_prefix(&key_prefix)
                    .await
                    .unwrap();
                let keys = Keys { keys };
                StoreReply {
                    reply: Some(Reply::FindKeysByPrefix(keys)),
                }
            }
            Query::FindKeyValuesByPrefix(key_prefix) => {
                let key_values = self
                    .memory_store
                    .find_key_values_by_prefix(&key_prefix)
                    .await
                    .unwrap();
                let key_values = key_values
                    .into_iter()
                    .map(|x| KeyValue {
                        key: x.0,
                        value: x.1,
                    })
                    .collect::<Vec<_>>();
                let key_values = KeyValues { key_values };
                StoreReply {
                    reply: Some(Reply::FindKeyValuesByPrefix(key_values)),
                }
            }
            Query::WriteBatch(batch) => {
                let mut new_batch = linera_views::batch::Batch::default();
                for statement in batch.statements {
                    match statement.operation.unwrap() {
                        Operation::DeleteKey(key) => {
                            new_batch.delete_key(key);
                        }
                        Operation::InsertKeyValue(key_value) => {
                            new_batch.put_key_value_bytes(key_value.key, key_value.value);
                        }
                        Operation::DeleteKeyPrefix(key_prefix) => {
                            new_batch.delete_key_prefix(key_prefix);
                        }
                    }
                }
                let base_key = batch.base_key;
                self.memory_store
                    .write_batch(new_batch, &base_key)
                    .await
                    .unwrap();
                StoreReply {
                    reply: Some(Reply::ClearJournal(UnitType {})),
                }
            }
            Query::ClearJournal(base_key) => {
                self.memory_store.clear_journal(&base_key).await.unwrap();
                StoreReply {
                    reply: Some(Reply::ClearJournal(UnitType {})),
                }
            }
        };
        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = <SharedStoreOptions as clap::Parser>::parse();

    let storage_config = options.storage_config;
    let common_config = CommonStoreConfig::default();
    let storage_config: StorageConfig = storage_config.parse()?;
    let full_storage_config = storage_config.add_common_config(common_config).await?;

    let endpoint = options.endpoint;
    let endpoint = endpoint.parse().unwrap();

    let memory_store = create_memory_store();
    let storage = Storage { memory_store };

    Server::builder()
        .add_service(StoreProcessorServer::new(storage))
        .serve(endpoint)
        .await?;

    Ok(())
}
