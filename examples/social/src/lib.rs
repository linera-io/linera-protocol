// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! # A Social Media Example Application
//!
//! This example illustrates how to use channels for cross-chain messages.
//!
//! For simplicity, each microchain represents one user—its owner. They can subscribe to other
//! users and make text posts that get sent to their subscribers.
//!
//! ## How it Works
//!
//! The application's state on every microchain contains a set of posts created by this chain
//! owner, and a set of posts received from other chains that it has subscribed to. The
//! received posts are indexed by timestamp, sender and index.
//!
//! There are `Subscribe` and `Unsubscribe` operations: If a chain owner includes these in a
//! new block, they can subscribe to or unsubscribe from another chain.
//!
//! There is also a `Post` operation: It creates a new post and sends it to a channel, so that
//! it reaches all subscribers.
//!
//! There are corresponding `RequestSubscribe`, `RequestUnsubscribe` and `Posts` cross-chain
//! message variants that are created when these operations are handled. The first two are
//! sent directly to the chain we want to subscribe to or unsubscribe from. The latter goes
//! to the channel.
//!
//! ## Usage
//!
//! To try it out, first setup a local network with two wallets, and keep it running in a
//! separate terminal:
//!
//! ```bash
//! ./scripts/run_local.sh
//! ```
//!
//! Compile the `social` example and create an application with it:
//!
//! ```bash
//! export LINERA_WALLET="$(realpath target/debug/wallet.json)"
//! export LINERA_STORAGE="rocksdb:$(dirname "$LINERA_WALLET")/linera.db"
//! export LINERA_WALLET_2="$(realpath target/debug/wallet_2.json)"
//! export LINERA_STORAGE_2="rocksdb:$(dirname "$LINERA_WALLET_2")/linera_2.db"
//!
//! cd examples/social && cargo build --release && cd ../..
//!
//! linera --wallet "$LINERA_WALLET" --storage "$LINERA_STORAGE" publish-and-create examples/target/wasm32-unknown-unknown/release/social_{contract,service}.wasm
//! ```
//!
//! This will output the new application ID, e.g.:
//!
//! ```ignore
//! e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000
//! ```
//!
//! With the `wallet show` command you can find the ID of the application creator's chain:
//!
//! ```bash
//! linera --wallet "$LINERA_WALLET" --storage "$LINERA_STORAGE" wallet show
//! ```
//!
//! ```ignore
//! e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65
//! ```
//!
//!
//! Now start a node service for each wallet, using two different ports:
//!
//! ```bash
//! linera --wallet "$LINERA_WALLET" --storage "$LINERA_STORAGE" service --port 8080 &
//! linera --wallet "$LINERA_WALLET_2" --storage "$LINERA_STORAGE_2" service --port 8081 &
//! ```
//!
//! Point your browser to http://localhost:8081. This is the wallet that didn't create the
//! application, so we have to request it from the creator chain:
//!
//! ```json
//! mutation {
//!     requestApplication(
//!         applicationId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65010000000000000001000000e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65030000000000000000000000"
//!     )
//! }
//! ```
//!
//! Now in both http://localhost:8080 and http://localhost:8081, this should list the
//! application and provide a link to its GraphQL API:
//!
//! ```json
//! query { applications { id description link } }
//! ```
//!
//! Open both URLs under the entry `link`. Now you can use the application on each chain.
//! E.g. in the 8081 tab subscribe to the other chain:
//!
//! ```json
//! mutation {
//!     subscribe(
//!         chainId: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65"
//!     )
//! }
//! ```
//!
//! Now make a post in the 8080 tab:
//!
//! ```json
//! mutation {
//!     post(
//!         text: "Linera Social is the new Mastodon!"
//!     )
//! }
//! ```
//!
//! Since 8081 is a subscriber. Let's see if it received any posts:
//!
//! ```json
//! query { receivedPostsKeys { timestamp author index } }
//! ```
//!
//! This should now list one entry, with timestamp, author and an index. If we view that
//! entry, we can see the posted text:
//!
//! ```json
//! query {
//!   receivedPosts(
//!     key: {
//!       timestamp: 1685626618522492,
//!       author: "e476187f6ddfeb9d588c7b45d3df334d5501d6499b3f9ad5595cae86cce16a65",
//!       index: 0
//!     }
//!   )
//! }
//! ```
//!
//! ```json
//! {
//!   "data": {
//!     "receivedPosts": "Linera Social is the new Mastodon!"
//!   }
//! }
//! ```

use async_graphql::{InputObject, Request, Response, SimpleObject};
use linera_sdk::base::{ChainId, ContractAbi, ServiceAbi, Timestamp};
use linera_views::{common::CustomSerialize, views};
use serde::{Deserialize, Serialize};

// TODO(#768): Remove the derive macros.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct SocialAbi;

impl ContractAbi for SocialAbi {
    type InitializationArgument = ();
    type Parameters = ();
    type Operation = Operation;
    type ApplicationCall = ();
    type Effect = Effect;
    type SessionCall = ();
    type Response = ();
    type SessionState = ();
}

impl ServiceAbi for SocialAbi {
    type Query = Request;
    type QueryResponse = Response;
    type Parameters = ();
}

/// An operation that can be executed by the application.
#[derive(Debug, Serialize, Deserialize)]
pub enum Operation {
    /// Request to be subscribed to another chain.
    RequestSubscribe(ChainId),
    /// Request to be unsubscribed from another chain.
    RequestUnsubscribe(ChainId),
    /// Send a new post to everyone who subscribed to us.
    Post(String),
}

/// An effect of the application on one chain, to be handled on another chain.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Effect {
    /// The origin chain wants to subscribe to the target chain.
    RequestSubscribe,
    /// The origin chain wants to unsubscribe from the target chain.
    RequestUnsubscribe,
    /// The origin chain made a post, and the target chain is subscribed.
    /// This includes the most recent posts in reverse order, and the total count of posts by the
    /// sender. I.e. the indices of the posts in the `Vec` are `count - 1, count - 2, ...`.
    Posts { count: u64, posts: Vec<OwnPost> },
}

/// A post's text and timestamp, to use in contexts where author and index are known.
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, SimpleObject)]
pub struct OwnPost {
    /// The timestamp of the block in which the post operation was included.
    pub timestamp: Timestamp,
    /// The posted text.
    pub text: String,
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
    fn to_custom_bytes(&self) -> Result<Vec<u8>, views::ViewError> {
        let data = (
            (!self.timestamp.micros()).to_be_bytes(),
            &self.author,
            (!self.index).to_be_bytes(),
        );
        Ok(bcs::to_bytes(&data)?)
    }

    fn from_custom_bytes(short_key: &[u8]) -> Result<Self, views::ViewError> {
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
    use super::Key;
    use linera_sdk::base::{ChainId, Timestamp};
    use linera_views::common::CustomSerialize;
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn test_key_custom_serialize() {
        let key = Key {
            timestamp: Timestamp::from(0x123456789ABCDEF),
            author: ChainId([0x12345, 0x6789A, 0xBCDEF, 0x0248A].into()),
            index: 0xFEDCBA9876543210,
        };
        let ser_key = key
            .to_custom_bytes()
            .expect("serialization of Key should succeeed");
        let deser_key =
            Key::from_custom_bytes(&ser_key).expect("deserialization of Key should succeed");
        assert_eq!(key, deser_key);
    }
}
