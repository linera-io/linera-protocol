// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_service::aggregator::StorageConfig;
use linera_views::common::CommonStoreConfig;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "Clear database", about = "A tool for cleaning up a database")]
struct DatabaseToolOptions {
    /// Subcommands. Acceptable values are run and generate.
    #[structopt(subcommand)]
    command: DatabaseToolCommand,
}

#[derive(StructOpt)]
enum DatabaseToolCommand {
    /// Subcommands. Acceptable values are delete_all, delete_single, initialize

    /// Delete all the entries of the database
    #[structopt(name = "delete_all")]
    DeleteAll {
        /// Storage configuration for the blockchain history.
        #[structopt(long = "storage")]
        storage_config: String,

        /// The maximal number of simultaneous queries to the database
        #[structopt(long)]
        max_concurrent_queries: Option<usize>,

        /// The maximal number of simultaneous stream queries to the database
        #[structopt(long, default_value = "10")]
        max_stream_queries: usize,

        /// The maximal number of entries in the storage cache.
        #[structopt(long, default_value = "1000")]
        cache_size: usize,
    },

    /// Delete a single table from the database
    #[structopt(name = "delete_single")]
    DeleteSingle {
        /// Storage configuration for the blockchain history.
        #[structopt(long = "storage")]
        storage_config: String,

        /// The maximal number of simultaneous queries to the database
        #[structopt(long)]
        max_concurrent_queries: Option<usize>,

        /// The maximal number of simultaneous stream queries to the database
        #[structopt(long, default_value = "10")]
        max_stream_queries: usize,

        /// The maximal number of entries in the storage cache.
        #[structopt(long, default_value = "1000")]
        cache_size: usize,
    },

    /// Initialize a table in the database
    #[structopt(name = "initialize")]
    Initialize {
        /// Storage configuration for the blockchain history.
        #[structopt(long = "storage")]
        storage_config: String,

        /// The maximal number of simultaneous queries to the database
        #[structopt(long)]
        max_concurrent_queries: Option<usize>,

        /// The maximal number of simultaneous stream queries to the database
        #[structopt(long, default_value = "10")]
        max_stream_queries: usize,

        /// The maximal number of entries in the storage cache.
        #[structopt(long, default_value = "1000")]
        cache_size: usize,
    },
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let options = DatabaseToolOptions::from_args();

    match options.command {
        DatabaseToolCommand::DeleteAll {
            storage_config,
            max_concurrent_queries,
            max_stream_queries,
            cache_size,
        } => {
            let storage_config: StorageConfig = storage_config.parse()?;
            let common_config = CommonStoreConfig {
                max_concurrent_queries,
                max_stream_queries,
                cache_size,
            };
            let full_storage_config = storage_config.add_common_config(common_config).await?;
            full_storage_config
                .delete_all()
                .await
                .expect("successful delete_all operation");
        }
        DatabaseToolCommand::DeleteSingle {
            storage_config,
            max_concurrent_queries,
            max_stream_queries,
            cache_size,
        } => {
            let storage_config: StorageConfig = storage_config.parse()?;
            let common_config = CommonStoreConfig {
                max_concurrent_queries,
                max_stream_queries,
                cache_size,
            };
            let full_storage_config = storage_config.add_common_config(common_config).await?;
            full_storage_config
                .delete_single()
                .await
                .expect("successful delete_all operation");
        }
        DatabaseToolCommand::Initialize {
            storage_config,
            max_concurrent_queries,
            max_stream_queries,
            cache_size,
        } => {
            let storage_config: StorageConfig = storage_config.parse()?;
            let common_config = CommonStoreConfig {
                max_concurrent_queries,
                max_stream_queries,
                cache_size,
            };
            let full_storage_config = storage_config.add_common_config(common_config).await?;
            full_storage_config
                .initialize()
                .await
                .expect("successful delete_all operation");
        }
    }
    tracing::info!("Successful execution of linera-db");
    Ok(())
}
