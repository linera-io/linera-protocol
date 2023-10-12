// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_service::storage::StorageConfig;
use linera_views::common::CommonStoreConfig;
use std::process;
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

    /// Check existence of a database
    #[structopt(name = "check_existence")]
    CheckExistence {
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

    /// List the tables of the database
    #[structopt(name = "list_tables")]
    ListTables {
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

async fn evaluate_options(options: DatabaseToolOptions) -> Result<(), anyhow::Error> {
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
        DatabaseToolCommand::CheckExistence {
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
            let test = full_storage_config
                .test_existence()
                .await
                .expect("successful delete_all operation");
            if test {
                process::exit(0);
            } else {
                process::exit(1);
            }
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
        DatabaseToolCommand::ListTables {
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
            let tables = full_storage_config
                .list_tables()
                .await
                .expect("successful list_tables operation");
            println!("The list of tables is {:?}", tables);
        }
    }
    tracing::info!("Successful execution of linera-db");
    Ok(())
}

#[tokio::main]
async fn main() {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();

    let options = DatabaseToolOptions::from_args();
    let error_code = match evaluate_options(options).await {
        Ok(_) => 0,
        Err(_) => 2,
    };
    process::exit(error_code);
}
