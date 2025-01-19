// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Social Media Example Application */

use async_graphql::{InputObject, Request, Response, SimpleObject};
use linera_sdk::{
    base::{ChainId, ContractAbi, ServiceAbi, Timestamp},
    graphql::GraphQLMutationRoot,
    views::{CustomSerialize, ViewError},
};
use serde::{Deserialize, Serialize};

pub struct SocialAbi;

impl ContractAbi for SocialAbi {
    type Operation = Operation;
    type Response = ();
}

impl ServiceAbi for SocialAbi {
    type Query = Request;
    type QueryResponse = Response;
}

/// An operation that can be executed by the application.
#[derive(Debug, Serialize, Deserialize, GraphQLMutationRoot)]
pub enum Operation {
    /// Request to be subscribed to another chain.
    Subscribe { chain_id: ChainId },
    /// Request to be unsubscribed from another chain.
    Unsubscribe { chain_id: ChainId },
    /// Send a new post to everyone who subscribed to us.
    Post {
        text: String,
        image_url: Option<String>,
    },
    /// Like a post
    Like { key: Key },
    /// Comment on a post
    Comment { key: Key, comment: String },
}

/// A message of the application on one chain, to be handled on another chain.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Message {
    /// The origin chain wants to subscribe to the target chain.
    Subscribe,
    /// The origin chain wants to unsubscribe from the target chain.
    Unsubscribe,
    /// The origin chain made a post, and the target chain is subscribed.
    Post { index: u64, post: OwnPost },
    /// A Chain liked a post
    Like { key: Key },
    /// A Chain commented on a post
    Comment {
        key: Key,
        chain_id: ChainId,
        comment: String,
    },
}

/// A post's text and timestamp, to use in contexts where author and index are known.
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, SimpleObject)]
pub struct OwnPost {
    /// The timestamp of the block in which the post operation was included.
    pub timestamp: Timestamp,
    /// The posted text.
    pub text: String,
    /// The posted Image_url(optional).
    pub image_url: Option<String>,
}

/// A post on the social app.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, SimpleObject)]
pub struct Post {
    /// The key identifying the post, including the timestamp, author and index.
    pub key: Key,
    /// The post's text content.
    pub text: String,
    /// The post's image_url(optional).
    pub image_url: Option<String>,
    /// The total number of likes
    pub likes: u32,
    /// Comments with there ChainId
    pub comments: Vec<Comment>,
}

/// A comment on a post
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, SimpleObject)]
pub struct Comment {
    /// The comment text
    pub text: String,
    /// The ChainId of the commenter
    pub chain_id: ChainId,
}

/// A key by which a post is indexed.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, SimpleObject, InputObject)]
#[graphql(input_name = "KeyInput")]
pub struct Key {
    /// The timestamp of the block in which the post was included on the author's chain.
    pub timestamp: Timestamp,
    /// The owner of the chain on which the `Post` operation was included.
    pub author: ChainId,
    /// The number of posts by that author before this one.
    pub index: u64,
}

// Serialize keys so that the lexicographic order of the serialized keys corresponds to reverse
// chronological order, then sorted by author, then by descending index.
impl CustomSerialize for Key {
    fn to_custom_bytes(&self) -> Result<Vec<u8>, ViewError> {
        let data = (
            (!self.timestamp.micros()).to_be_bytes(),
            &self.author,
            (!self.index).to_be_bytes(),
        );
        Ok(bcs::to_bytes(&data)?)
    }

    fn from_custom_bytes(short_key: &[u8]) -> Result<Self, ViewError> {
        let (time_bytes, author, idx_bytes) = (bcs::from_bytes(short_key))?;
        Ok(Self {
            timestamp: Timestamp::from(!u64::from_be_bytes(time_bytes)),
            author,
            index: !u64::from_be_bytes(idx_bytes),
        })
    }
}

#[cfg(test)]
mod tests {
    use linera_sdk::{
        base::{ChainId, Timestamp},
        views::CustomSerialize,
    };

    use super::Key;

    #[test]
    fn test_key_custom_serialize() {
        let key = Key {
            timestamp: Timestamp::from(0x123456789ABCDEF),
            author: ChainId([0x12345, 0x6789A, 0xBCDEF, 0x0248A].into()),
            index: 0xFEDCBA9876543210,
        };
        let ser_key = key
            .to_custom_bytes()
            .expect("serialization of Key should succeed");
        let deser_key =
            Key::from_custom_bytes(&ser_key).expect("deserialization of Key should succeed");
        assert_eq!(key, deser_key);
    }
}
