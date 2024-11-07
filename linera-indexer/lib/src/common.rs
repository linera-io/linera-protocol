// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::net::AddrParseError;

use async_graphql::http::GraphiQLSource;
use axum::{
    http::Uri,
    response::{self, IntoResponse},
};
use linera_base::crypto::CryptoHash;
use reqwest::header::InvalidHeaderValue;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IndexerError {
    #[error("View error: {0}")]
    ViewError(#[from] linera_views::views::ViewError),
    #[error("Reqwest error: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("GraphQL error: {0}")]
    GraphQLError(#[from] graphql_ws_client::Error),
    #[error("Tungstenite error: {0}")]
    TungsteniteError(#[from] async_tungstenite::tungstenite::Error),
    #[error("Invalid header value: {0}")]
    InvalidHeader(#[from] InvalidHeaderValue),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Parse error: {0}")]
    ParserError(#[from] AddrParseError),
    #[error("Server error: {0}")]
    ServerError(#[from] hyper::Error),
    #[error("Null GraphQL data: {0:?}")]
    NullData(Option<Vec<graphql_client::Error>>),
    #[error("Block not found: {0:?}")]
    NotFound(Option<CryptoHash>),
    #[error("Unknown plugin: {0}")]
    UnknownPlugin(String),
    #[error("Plugin not loaded: {0}")]
    UnloadedPlugin(String),
    #[error("Unknown certificate status: {0:?}")]
    UnknownCertificateStatus(String),
    #[error("Different plugins in command line and memory")]
    WrongPlugins,
    #[error("Plugin is already registered")]
    PluginAlreadyRegistered,
    #[error("Invalid certificate content: {0:?}")]
    InvalidCertificateValue(CryptoHash),
    #[error("Clone with root key error")]
    CloneWithRootKeyError,

    #[cfg(feature = "rocksdb")]
    #[error("RocksDB error: {0}")]
    RocksDbError(#[from] linera_views::rocks_db::RocksDbStoreError),
    #[cfg(feature = "scylladb")]
    #[error("ScyllaDB error: {0}")]
    ScyllaDbError(#[from] linera_views::scylla_db::ScyllaDbStoreError),
}

pub async fn graphiql(uri: Uri) -> impl IntoResponse {
    response::Html(GraphiQLSource::build().endpoint(uri.path()).finish())
}
