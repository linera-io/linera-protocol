// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module defines the linera-indexer executable using the RocksDB client.

use linera_indexer::{
    common::IndexerError, operations::OperationsPlugin, plugin::Plugin, rocks_db::RocksDbRunner,
};

#[tokio::main]
async fn main() -> Result<(), IndexerError> {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();

    let mut runner = RocksDbRunner::load().await?;
    runner
        .add_plugin(OperationsPlugin::load(runner.client.clone(), "operations").await?)
        .await?;
    runner.run().await
}
