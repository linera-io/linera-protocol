// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::process;

use linera_client::storage::StorageConfigNamespace;
use linera_views::common::CommonStoreConfig;

#[derive(clap::Parser)]
#[command(
    name = "Clear database",
    about = "A tool for cleaning up a database",
    version = linera_version::VersionInfo::default_clap_str(),
)]
struct DatabaseToolOptions {
    /// Subcommands. Acceptable values are run and generate.
    #[command(subcommand)]
    command: DatabaseToolCommand,

    /// The number of Tokio worker threads to use.
    #[arg(long, env = "LINERA_DB_TOOL_TOKIO_THREADS")]
    tokio_threads: Option<usize>,
}

#[derive(clap::Parser)]
enum DatabaseToolCommand {
    /// Delete all the namespaces of the database
    #[command(name = "delete_all")]
    DeleteAll {
        /// Storage configuration for the blockchain history.
        #[arg(long = "storage")]
        storage_config: String,
    },

    /// Delete a single namespace from the database
    #[command(name = "delete_namespace")]
    DeleteNamespace {
        /// Storage configuration for the blockchain history.
        #[arg(long = "storage")]
        storage_config: String,
    },

    /// Check existence of a namespace in the database
    #[command(name = "check_existence")]
    CheckExistence {
        /// Storage configuration for the blockchain history.
        #[arg(long = "storage")]
        storage_config: String,
    },

    /// Check absence of a namespace in the database
    #[command(name = "check_absence")]
    CheckAbsence {
        /// Storage configuration for the blockchain history.
        #[arg(long = "storage")]
        storage_config: String,
    },

    /// Initialize a namespace in the database
    #[command(name = "initialize")]
    Initialize {
        /// Storage configuration for the blockchain history.
        #[arg(long = "storage")]
        storage_config: String,
    },

    /// List the namespaces of the database
    #[command(name = "list_namespaces")]
    ListNamespaces {
        /// Storage configuration for the blockchain history.
        #[arg(long = "storage")]
        storage_config: String,
    },
}

async fn evaluate_options(options: DatabaseToolOptions) -> Result<i32, anyhow::Error> {
    let common_config = CommonStoreConfig::default();
    match options.command {
        DatabaseToolCommand::DeleteAll { storage_config } => {
            let storage_config = storage_config.parse::<StorageConfigNamespace>()?;
            let full_storage_config = storage_config.add_common_config(common_config).await?;
            full_storage_config.delete_all().await?;
        }
        DatabaseToolCommand::DeleteNamespace { storage_config } => {
            let storage_config = storage_config.parse::<StorageConfigNamespace>()?;
            let full_storage_config = storage_config.add_common_config(common_config).await?;
            full_storage_config.delete_namespace().await?;
        }
        DatabaseToolCommand::CheckExistence { storage_config } => {
            let storage_config = storage_config.parse::<StorageConfigNamespace>()?;
            let full_storage_config = storage_config.add_common_config(common_config).await?;
            let test = full_storage_config.test_existence().await?;
            if test {
                tracing::info!("The database does exist");
                return Ok(0);
            } else {
                tracing::info!("The database does not exist");
                return Ok(1);
            }
        }
        DatabaseToolCommand::CheckAbsence { storage_config } => {
            let storage_config = storage_config.parse::<StorageConfigNamespace>()?;
            let full_storage_config = storage_config.add_common_config(common_config).await?;
            let test = full_storage_config.test_existence().await?;
            if test {
                tracing::info!("The database does exist");
                return Ok(1);
            } else {
                tracing::info!("The database does not exist");
                return Ok(0);
            }
        }
        DatabaseToolCommand::Initialize { storage_config } => {
            let storage_config = storage_config.parse::<StorageConfigNamespace>()?;
            let full_storage_config = storage_config.add_common_config(common_config).await?;
            full_storage_config.initialize().await?;
        }
        DatabaseToolCommand::ListNamespaces { storage_config } => {
            let storage_config = storage_config.parse::<StorageConfigNamespace>()?;
            let full_storage_config = storage_config.add_common_config(common_config).await?;
            let namespaces = full_storage_config.list_all().await?;
            println!("The list of namespaces is {:?}", namespaces);
        }
    }
    tracing::info!("Successful execution of linera-db");
    Ok(0)
}

fn main() {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();

    let options = <DatabaseToolOptions as clap::Parser>::parse();

    let mut runtime = if options.tokio_threads == Some(1) {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();

        if let Some(threads) = options.tokio_threads {
            builder.worker_threads(threads);
        }

        builder
    };

    let result = runtime
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
        .block_on(evaluate_options(options));

    let error_code = match result {
        Ok(code) => code,
        Err(msg) => {
            tracing::error!("Error is {:?}", msg);
            2
        }
    };
    process::exit(error_code);
}
