// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Social Media Example Application */

use async_graphql::{InputObject, Request, Response, SimpleObject};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{ChainId, ContractAbi, ServiceAbi, Timestamp},
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

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    /// Like a post, author of a post receives the message.
    Like { key: Key },
    /// Comment on a post, author of a post receives the message.
    Comment { key: Key, comment: String },
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
    /// Comments with their ChainId
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
    pub index: u32,
}

/// An event emitted by the social app.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Event {
    /// A new post was created
    Post { post: OwnPost, index: u32 },
    /// A user liked a post
    Like { key: Key },
    /// A user commented on a post
    Comment { key: Key, comment: String },
}

#[cfg(not(target_arch = "wasm32"))]
pub mod formats {
    use linera_sdk::formats::{BcsApplication, Formats};
    use serde_reflection::{Samples, Tracer, TracerConfig};

    use super::{Comment, Event, Key, Message, Operation, OwnPost, Post, SocialAbi};

    /// The Social application.
    pub struct SocialApplication;

    impl BcsApplication for SocialApplication {
        type Abi = SocialAbi;

        fn formats() -> serde_reflection::Result<Formats> {
            let mut tracer = Tracer::new(
                TracerConfig::default()
                    .record_samples_for_newtype_structs(true)
                    .record_samples_for_tuple_structs(true),
            );
            let samples = Samples::new();

            // Trace the ABI types
            let (operation, _) = tracer.trace_type::<Operation>(&samples)?;
            let (response, _) = tracer.trace_type::<()>(&samples)?;
            let (message, _) = tracer.trace_type::<Message>(&samples)?;
            let (event_value, _) = tracer.trace_type::<Event>(&samples)?;

            // Trace additional supporting types (notably all enums) to populate the registry
            tracer.trace_type::<Key>(&samples)?;
            tracer.trace_type::<OwnPost>(&samples)?;
            tracer.trace_type::<Post>(&samples)?;
            tracer.trace_type::<Comment>(&samples)?;

            let registry = tracer.registry()?;

            Ok(Formats {
                registry,
                operation,
                response,
                message,
                event_value,
            })
        }
    }
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
            index: !u32::from_be_bytes(idx_bytes),
        })
    }
}

#[cfg(test)]
mod tests {
    use linera_sdk::{
        linera_base_types::{ChainId, Timestamp},
        views::CustomSerialize,
    };

    use super::Key;

    #[test]
    fn test_key_custom_serialize() {
        let key = Key {
            timestamp: Timestamp::from(0x123456789ABCDEF),
            author: ChainId([0x12345, 0x6789A, 0xBCDEF, 0x0248A].into()),
            index: 0x76543210,
        };
        let ser_key = key
            .to_custom_bytes()
            .expect("serialization of Key should succeed");
        let deser_key =
            Key::from_custom_bytes(&ser_key).expect("deserialization of Key should succeed");
        assert_eq!(key, deser_key);
    }
}
