// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An example of a gRPC indexer server with multiple database backend support.

use clap::Parser;
use linera_indexer::{
    common::IndexerError,
    db::{postgres::PostgresDatabase, sqlite::SqliteDatabase},
    grpc::IndexerGrpcServer,
};

#[derive(Parser, Debug)]
#[command(version = linera_version::VersionInfo::default_clap_str())]
#[command(group = clap::ArgGroup::new("database").required(true).multiple(false))]
struct Args {
    /// The port of the gRPC indexer server
    #[arg(long, default_value = "8081")]
    port: u16,

    /// Use in-memory SQLite database (data is lost on restart)
    #[arg(long, group = "database")]
    memory: bool,

    /// Use SQLite database with file persistence
    #[arg(long, group = "database", value_name = "PATH")]
    sqlite: Option<String>,

    /// Use PostgreSQL database
    #[arg(long, group = "database", value_name = "URL")]
    postgres: Option<String>,
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

    // Start server with the selected database backend
    if args.memory {
        tracing::info!("Starting indexer with in-memory SQLite database");
        let database = SqliteDatabase::new("sqlite::memory:").await?;
        let grpc_server = IndexerGrpcServer::new(database);
        grpc_server
            .serve(args.port)
            .await
            .map_err(IndexerError::Other)?;
    } else if let Some(path) = args.sqlite {
        tracing::info!(?path, "Starting indexer with SQLite database");
        let database = SqliteDatabase::new(&path).await?;
        let grpc_server = IndexerGrpcServer::new(database);
        grpc_server
            .serve(args.port)
            .await
            .map_err(IndexerError::Other)?;
    } else if let Some(url) = args.postgres {
        tracing::info!(?url, "Starting indexer with PostgreSQL database");
        let database = PostgresDatabase::new(&url).await?;
        let grpc_server = IndexerGrpcServer::new(database);
        grpc_server
            .serve(args.port)
            .await
            .map_err(IndexerError::Other)?;
    } else {
        return Err(IndexerError::Other(
            "No database backend specified. Use --memory, --sqlite, or --postgres".into(),
        ));
    }

    Ok(())
}
