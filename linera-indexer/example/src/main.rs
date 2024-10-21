// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An example of an indexer with the operations plugin.

use linera_indexer::{common::IndexerError, plugin::Plugin, storage_service::StorageServiceRunner};
use linera_indexer_plugins::operations::OperationsPlugin;

#[tokio::main]
async fn main() -> Result<(), IndexerError> {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();

    let mut runner = StorageServiceRunner::load().await?;
    runner
        .add_plugin(OperationsPlugin::load(runner.store.clone()).await?)
        .await?;
    runner.run().await
}
