// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::base::{ChainId, Timestamp};
use linera_views::{
    common::{Context, CustomSerialize},
    views,
};
use serde::{Deserialize, Serialize};

/// A query for the application service.
#[derive(Debug, Serialize, Deserialize)]
pub enum Query {
    /// The most recent posts from other chains. Returns a `Vec<Post>` starting with the latest.
    ReceivedPosts(u64),
    /// The most recent posts we sent. Returns a `Vec<Post>` starting with the latest.
    SentPosts(u64),
}

/// An operation that can be executed by the application.
#[derive(Serialize, Deserialize)]
pub enum Operation {
    /// Request to be subscribed to another chain.
    RequestSubscribe(ChainId),
    /// Request to be unsubscribed from another chain.
    RequestUnsubscribe(ChainId),
    /// Send a new post to everyone who subscribed to us.
    Post(String),
}

/// A post on the social app.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct Post {
    /// The key identifying the post, including the timestamp, author and index.
    pub key: Key,
    /// The post's text content.
    pub text: String,
}

/// A key by which a post is indexed.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct Key {
    /// The timestamp of the block in which the post was included on the author's chain.
    pub timestamp: Timestamp,
    /// The owner of the chain on which the `Post` operation was included.
    pub author: ChainId,
    /// The number of posts by that author before this one.
    pub index: u64,
}

// Serialize so that the lexicographic order of the serialized keys corresponds to reverse
// chronological order, then sorted by author, then by descending index.
impl CustomSerialize for Key {
    fn to_custom_bytes<C: Context>(&self) -> Result<Vec<u8>, views::ViewError>
    where
        views::ViewError: From<<C as Context>::Error>,
    {
        let data = (
            (!self.timestamp.micros()).to_be_bytes(),
            &self.author,
            (!self.index).to_be_bytes(),
        );
        Ok(bcs::to_bytes(&data)?)
    }

    fn from_custom_bytes<C: Context>(short_key: &[u8]) -> Result<Self, views::ViewError>
    where
        views::ViewError: From<<C as Context>::Error>,
        Self: Sized,
    {
        let (time_bytes, author, idx_bytes) = (bcs::from_bytes(short_key))?;
        Ok(Self {
            timestamp: Timestamp::from(!u64::from_be_bytes(time_bytes)),
            author,
            index: !u64::from_be_bytes(idx_bytes),
        })
    }
}
