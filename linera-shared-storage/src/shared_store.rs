use tonic::{transport::Server, Request, Response, Status};
use linera_service::storage::StorageConfig;
use linera_views::common::CommonStoreConfig;
//use std::process;
use linera_views::memory::MemoryStore;
use linera_views::memory::create_memory_store;

#[allow(clippy::derive_partial_eq_without_eq)]
// https://github.com/hyperium/tonic/issues/1056
pub mod key_value_store {
    tonic::include_proto!("key_value_store.v1");
}

use key_value_store::{
    StoreProcessorServer,
};

pub struct Storage {
    memory_store: MemoryStore,
}


struct SharedStoreOptions {
    /// Subcommands. Acceptable values are run and generate.
    #[arg(long = "storage")]
    storage_config: String,

    #[arg(long = "endpoint")]
    endpoint: String,
}

#[tonic::async_trait]
impl StoreProcessor for Storage {
    async fn process(
        &self,
        request: Request<StoreRequest>,
    ) -> Result<Response<StoreResponse>, Status> {
        let response = match request {
            ReadKey(read_key) => {
                StoreResponse { clear_journal: ClearJournal { } }
            },
            ContainsKey(contains_key) => {
                StoreResponse { clear_journal: ClearJournal { } }
            },
            ReadMultiKeys(read_multi_keys) => {
                StoreResponse { clear_journal: ClearJournal { } }
            },
            FindKeysByPrefix(find_keys_by_prefix) => {
                StoreResponse { clear_journal: ClearJournal { } }
            },
            FindKeyValuesByPrefix(find_key_values_by_prefix) => {
                StoreResponse { clear_journal: ClearJournal { } }
            },
            WriteBatch(write_batch) => {
                StoreResponse { clear_journal: ClearJournal { } }
            },
            ClearJournal(clear_journal) => {
                StoreResponse { clear_journal: ClearJournal { } }
            }
        };
        Ok(Response::new(response))
    }
}




#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /*
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();
*/
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
        .serve(addr)
        .await?;

    Ok(())
}
