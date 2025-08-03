// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An example of a gRPC indexer server with SQLite storage.

use clap::Parser;
use linera_indexer::{common::IndexerError, db::sqlite::SqliteDatabase, grpc::IndexerGrpcServer};

#[derive(Parser, Debug)]
#[command(version = linera_version::VersionInfo::default_clap_str())]
struct Args {
    /// The port of the gRPC indexer server
    #[arg(long, default_value = "8081")]
    port: u16,
    /// SQLite database file path
    #[arg(long, default_value = "indexer.db")]
    database_path: String,
}

#[tokio::main]
async fn main() -> Result<(), IndexerError> {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();

    let args = Args::parse();

    // Initialize SQLite database
    let database = SqliteDatabase::new(args.database_path.as_str()).await?;

    // Create and start gRPC server
    let grpc_server = IndexerGrpcServer::new(database);
    grpc_server
        .serve(args.port)
        .await
        .map_err(IndexerError::Other)?;

    Ok(())
}
