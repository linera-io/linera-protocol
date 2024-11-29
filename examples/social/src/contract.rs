// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{
    base::{ChainId, ChannelName, Destination, MessageId, WithContractAbi},
    views::{RootView, View},
    Contract, ContractRuntime,
};
use social::{Comment, Key, Message, Operation, OwnPost, Post, SocialAbi};
use state::SocialState;

/// The channel name the application uses for cross-chain messages about new posts.
const POSTS_CHANNEL_NAME: &[u8] = b"posts";

pub struct SocialContract {
    state: SocialState,
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

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = SocialState::load(runtime.root_view_storage_context())
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
            Operation::Post { text, image_url } => {
                self.execute_post_operation(text, image_url).await
            }
            Operation::Like { key } => self.execute_like_operation(key).await,
            Operation::Comment { key, comment } => {
                self.execute_comment_operation(key, comment).await
            }
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
            Message::Post { index, post } => self.execute_post_message(message_id, index, post),
            Message::Like { key } => self.execute_like_message(key).await,
            Message::Comment {
                key,
                chain_id,
                comment,
            } => self.execute_comment_message(key, chain_id, comment).await,
        }
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

impl SocialContract {
    async fn execute_post_operation(
        &mut self,
        text: String,
        image_url: Option<String>,
    ) -> (Destination, Message) {
        let timestamp = self.runtime.system_time();
        let post = OwnPost {
            timestamp,
            text,
            image_url,
        };
        let index = self.state.own_posts.count() as u64;
        self.state.own_posts.push(post.clone());
        (
            ChannelName::from(POSTS_CHANNEL_NAME.to_vec()).into(),
            Message::Post { index, post },
        )
    }

    async fn execute_like_operation(&mut self, key: Key) -> (Destination, Message) {
        (
            ChannelName::from(POSTS_CHANNEL_NAME.to_vec()).into(),
            Message::Like { key },
        )
    }

    async fn execute_comment_operation(
        &mut self,
        key: Key,
        comment: String,
    ) -> (Destination, Message) {
        let chain_id = self.runtime.chain_id();
        (
            ChannelName::from(POSTS_CHANNEL_NAME.to_vec()).into(),
            Message::Comment {
                key,
                chain_id,
                comment,
            },
        )
    }

    fn execute_post_message(&mut self, message_id: MessageId, index: u64, post: OwnPost) {
        let key = Key {
            timestamp: post.timestamp,
            author: message_id.chain_id,
            index,
        };
        let new_post = Post {
            key: key.clone(),
            text: post.text,
            image_url: post.image_url,
            likes: 0,
            comments: vec![],
        };

        self.state
            .received_posts
            .insert(&key, new_post)
            .expect("Failed to insert received post");
    }

    async fn execute_like_message(&mut self, key: Key) {
        let mut post = self
            .state
            .received_posts
            .get(&key)
            .await
            .expect("Failed to retrieve post")
            .expect("Post not found");

        post.likes += 1;

        self.state
            .received_posts
            .insert(&key, post)
            .expect("Failed to insert received post");
    }

    async fn execute_comment_message(&mut self, key: Key, chain_id: ChainId, comment: String) {
        let mut post = self
            .state
            .received_posts
            .get(&key)
            .await
            .expect("Failed to retrieve post")
            .expect("Post not found");

        let comment = Comment {
            chain_id,
            text: comment,
        };

        post.comments.push(comment);

        self.state
            .received_posts
            .insert(&key, post)
            .expect("Failed to insert received post");
    }
}
