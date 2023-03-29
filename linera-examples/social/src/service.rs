// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![no_main]

mod state;

use async_trait::async_trait;
use linera_sdk::{
    service::system_api::ReadableWasmContext, FromBcsBytes, QueryContext, Service, ViewStateStorage,
};
use linera_views::views::ViewError;
use social::{Key, OwnPost, Post, Query};
use state::Social;
use std::sync::Arc;
use thiserror::Error;

pub type ReadableSocial = Social<ReadableWasmContext>;
linera_sdk::service!(ReadableSocial);

#[async_trait]
impl Service for Social<ReadableWasmContext> {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn query_application(
        self: Arc<Self>,
        context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error> {
        match Query::from_bcs_bytes(argument).map_err(Error::InvalidQuery)? {
            Query::ReceivedPosts(count) => self.handle_received_posts_query(count).await,
            Query::SentPosts(count) => self.handle_sent_posts_query(context, count).await,
        }
    }
}

impl Social<ReadableWasmContext> {
    async fn handle_received_posts_query(self: Arc<Self>, count: u64) -> Result<Vec<u8>, Error> {
        let mut result = vec![];
        let count = count.try_into().unwrap_or(usize::MAX);
        self.received_posts
            .for_each_index_value(|key, text| {
                if result.len() < count {
                    result.push(Post { key, text });
                }
                Ok(())
            })
            .await?;
        Ok(bcs::to_bytes(&result)?)
    }

    async fn handle_sent_posts_query(
        self: Arc<Self>,
        context: &QueryContext,
        count: u64,
    ) -> Result<Vec<u8>, Error> {
        let mut result = vec![];
        let own_count = self.own_posts.count();
        for index in (0..own_count).into_iter().rev().take(count as usize) {
            let OwnPost { timestamp, text } =
                self.own_posts.get(index).await?.expect("missing entry");
            let key = Key {
                timestamp,
                index: index as u64,
                author: context.chain_id,
            };
            result.push(Post { key, text });
        }
        Ok(bcs::to_bytes(&result)?)
    }
}

/// An error that can occur during the service execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid query.
    #[error("Invalid query")]
    InvalidQuery(bcs::Error),

    /// Serialization error.
    #[error(transparent)]
    Serialization(#[from] bcs::Error),

    /// View error.
    #[error(transparent)]
    View(#[from] ViewError),
}
