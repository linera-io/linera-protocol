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

use crate::db::sqlite::SqliteError;

#[derive(Error, Debug)]
pub enum IndexerError {
    #[error(transparent)]
    ViewError(#[from] linera_views::ViewError),
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    #[error(transparent)]
    GraphQLError(#[from] graphql_ws_client::Error),
    #[error(transparent)]
    TungsteniteError(#[from] Box<async_tungstenite::tungstenite::Error>),
    #[error(transparent)]
    InvalidHeader(#[from] InvalidHeaderValue),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    ParserError(#[from] AddrParseError),
    #[error("Null GraphQL data: {0:?}")]
    NullData(Option<Vec<graphql_client::Error>>),
    #[error("Block not found: {0:?}")]
    NotFound(Option<CryptoHash>),
    #[error("Unknown plugin: {0}")]
    UnknownPlugin(String),
    #[error(transparent)]
    ConversionError(linera_service_graphql_client::ConversionError),
    #[error("Plugin is already registered")]
    PluginAlreadyRegistered,
    #[error("Open exclusive error")]
    OpenExclusiveError,
    #[error("Other error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[cfg(feature = "rocksdb")]
    #[error(transparent)]
    RocksDbError(#[from] linera_views::rocks_db::RocksDbStoreError),
    #[cfg(feature = "scylladb")]
    #[error(transparent)]
    ScyllaDbError(#[from] Box<linera_views::scylla_db::ScyllaDbStoreError>),
}

impl From<SqliteError> for IndexerError {
    fn from(error: SqliteError) -> Self {
        Self::Other(Box::new(error).into())
    }
}

impl From<async_tungstenite::tungstenite::Error> for IndexerError {
    fn from(error: async_tungstenite::tungstenite::Error) -> Self {
        Box::new(error).into()
    }
}

#[cfg(feature = "scylladb")]
impl From<linera_views::scylla_db::ScyllaDbStoreError> for IndexerError {
    fn from(error: linera_views::scylla_db::ScyllaDbStoreError) -> Self {
        Box::new(error).into()
    }
}

pub async fn graphiql(uri: Uri) -> impl IntoResponse {
    response::Html(GraphiQLSource::build().endpoint(uri.path()).finish())
}
