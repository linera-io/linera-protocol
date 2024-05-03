// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{
    base::{ChannelName, Destination, MessageId, WithContractAbi},
    views::{RootView, View, ViewStorageContext},
    Contract, ContractRuntime,
};
use social::{Key, Message, Operation, OwnPost, SocialAbi};
use state::Social;

/// The channel name the application uses for cross-chain messages about new posts.
const POSTS_CHANNEL_NAME: &[u8] = b"posts";
/// The number of recent posts sent in each cross-chain message.
const RECENT_POSTS: usize = 10;

pub struct SocialContract {
    state: Social,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(SocialContract);

impl WithContractAbi for SocialContract {
    type Abi = SocialAbi;
}

impl Contract for SocialContract {
    type Message = Message;
    type InstantiationArgument = ();
    type Parameters = ();

    async fn new(runtime: ContractRuntime<Self>) -> Self {
        let state = Social::load(ViewStorageContext::from(runtime.key_value_store()))
            .await
            .expect("Failed to load state");
        SocialContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: Operation) -> Self::Response {
        let (destination, message) = match operation {
            Operation::Subscribe { chain_id } => (chain_id.into(), Message::Subscribe),
            Operation::Unsubscribe { chain_id } => (chain_id.into(), Message::Unsubscribe),
            Operation::Post { text } => self.execute_post_operation(text).await,
        };

        self.runtime.send_message(destination, message);
    }

    async fn execute_message(&mut self, message: Message) {
        let message_id = self
            .runtime
            .message_id()
            .expect("Message ID has to be available when executing a message");
        match message {
            Message::Subscribe => self.runtime.subscribe(
                message_id.chain_id,
                ChannelName::from(POSTS_CHANNEL_NAME.to_vec()),
            ),
            Message::Unsubscribe => self.runtime.unsubscribe(
                message_id.chain_id,
                ChannelName::from(POSTS_CHANNEL_NAME.to_vec()),
            ),
            Message::Posts { count, posts } => self.execute_posts_message(message_id, count, posts),
        }
    }

    async fn finalize(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

impl SocialContract {
    async fn execute_post_operation(&mut self, text: String) -> (Destination, Message) {
        let timestamp = self.runtime.system_time();
        self.state.own_posts.push(OwnPost { timestamp, text });
        let count = self.state.own_posts.count();
        let mut posts = vec![];
        for index in (0..count).rev().take(RECENT_POSTS) {
            let maybe_post = self
                .state
                .own_posts
                .get(index)
                .await
                .expect("Failed to retrieve post from storage");
            let own_post = maybe_post
                .expect("post with valid index missing; this is a bug in the social application!");
            posts.push(own_post);
        }
        let count = count as u64;
        (
            ChannelName::from(POSTS_CHANNEL_NAME.to_vec()).into(),
            Message::Posts { count, posts },
        )
    }

    fn execute_posts_message(&mut self, message_id: MessageId, count: u64, posts: Vec<OwnPost>) {
        for (index, post) in (0..count).rev().zip(posts) {
            let key = Key {
                timestamp: post.timestamp,
                author: message_id.chain_id,
                index,
            };
            self.state
                .received_posts
                .insert(&key, post.text)
                .expect("Failed to insert received post");
        }
    }
}
