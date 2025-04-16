// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use linera_sdk::{
    linera_base_types::{ChainId, StreamId, WithContractAbi},
    views::{RootView, View},
    Contract, ContractRuntime,
};
use serde::{Deserialize, Serialize};
use social::{Comment, Key, Operation, OwnPost, Post, SocialAbi};
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
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = Event;

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
        match operation {
            Operation::Subscribe { chain_id } => {
                let app_id = self.runtime.application_id().forget_abi();
                self.runtime
                    .subscribe_to_events(chain_id, app_id, POSTS_CHANNEL_NAME.into());
            }
            Operation::Unsubscribe { chain_id } => {
                let app_id = self.runtime.application_id().forget_abi();
                self.runtime
                    .unsubscribe_from_events(chain_id, app_id, POSTS_CHANNEL_NAME.into());
            }
            Operation::Post { text, image_url } => {
                self.execute_post_operation(text, image_url).await
            }
            Operation::Like { key } => self.execute_like_operation(key).await,
            Operation::Comment { key, comment } => {
                self.execute_comment_operation(key, comment).await
            }
        }
    }

    async fn execute_message(&mut self, (): ()) {
        panic!("Unexpected message");
    }

    async fn process_streams(&mut self, streams: Vec<(ChainId, StreamId, u32)>) {
        for (chain_id, stream_id, next_index) in streams {
            assert_eq!(stream_id.stream_name, POSTS_CHANNEL_NAME.into());
            assert_eq!(
                stream_id.application_id,
                self.runtime.application_id().forget_abi().into()
            );
            let stored_count = self
                .state
                .processed_events
                .get_mut_or_default(&chain_id)
                .await
                .expect("Failed to read from state");
            let count = *stored_count;
            *stored_count = next_index;
            for index in count..next_index {
                let event = self
                    .runtime
                    .read_event(chain_id, POSTS_CHANNEL_NAME.into(), index);
                match event {
                    Event::Post { post } => {
                        self.execute_post_event(chain_id, next_index, post);
                    }
                    Event::Like { key } => self.execute_like_event(key).await,
                    Event::Comment { key, comment } => {
                        self.execute_comment_event(key, chain_id, comment).await;
                    }
                }
            }
        }
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}

impl SocialContract {
    async fn execute_post_operation(&mut self, text: String, image_url: Option<String>) {
        let timestamp = self.runtime.system_time();
        let post = OwnPost {
            timestamp,
            text,
            image_url,
        };
        self.state.own_posts.push(post.clone());
        self.runtime
            .emit(POSTS_CHANNEL_NAME.into(), &Event::Post { post });
    }

    async fn execute_like_operation(&mut self, key: Key) {
        self.runtime
            .emit(POSTS_CHANNEL_NAME.into(), &Event::Like { key });
    }

    async fn execute_comment_operation(&mut self, key: Key, comment: String) {
        self.runtime
            .emit(POSTS_CHANNEL_NAME.into(), &Event::Comment { key, comment });
    }

    fn execute_post_event(&mut self, chain_id: ChainId, index: u32, post: OwnPost) {
        let key = Key {
            timestamp: post.timestamp,
            author: chain_id,
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

    async fn execute_like_event(&mut self, key: Key) {
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

    async fn execute_comment_event(&mut self, key: Key, chain_id: ChainId, comment: String) {
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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum Event {
    /// A new post was created
    Post { post: OwnPost },
    /// A user liked a post
    Like { key: Key },
    /// A user commented on a post
    Comment { key: Key, comment: String },
}
