// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helpers for sending GraphQL requests to the node service.

use graphql_client::{reqwest::post_graphql, GraphQLQuery};
use reqwest::Client;
use thiserror::Error;

/// An error that occurs while performing a GraphQL request.
#[derive(Error, Debug)]
pub enum Error {
    /// An HTTP transport error from `reqwest`.
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    /// The response contained GraphQL errors.
    #[error("GraphQL errors: {0:?}")]
    GraphQLError(Vec<graphql_client::Error>),
}

impl From<Option<Vec<graphql_client::Error>>> for Error {
    fn from(val: Option<Vec<graphql_client::Error>>) -> Self {
        Self::GraphQLError(val.unwrap_or_default())
    }
}

/// Sends the GraphQL query `T` with the given `variables` to `url` and returns
/// its response data.
pub async fn request<T, V>(
    client: &Client,
    url: &str,
    variables: V,
) -> Result<T::ResponseData, Error>
where
    T: GraphQLQuery<Variables = V> + Send + Unpin + 'static,
    V: Send + Unpin,
{
    let response = post_graphql::<T, _>(client, url, variables).await?;
    match response.data {
        None => Err(response.errors.into()),
        Some(data) => Ok(data),
    }
}
