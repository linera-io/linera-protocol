// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_trait::async_trait;
use linera_sdk::{
    base::{ChannelName, Destination, SessionId, WithContractAbi},
    contract::system_api,
    views::ViewError,
    ApplicationCallOutcome, CalleeContext, Contract, ExecutionOutcome, MessageContext,
    OperationContext, SessionCallOutcome, ViewStateStorage,
};
use social::{Key, Message, Operation, OwnPost};
use state::Social;
use thiserror::Error;

/// The channel name the application uses for cross-chain messages about new posts.
const POSTS_CHANNEL_NAME: &[u8] = b"posts";
/// The number of recent posts sent in each cross-chain message.
const RECENT_POSTS: usize = 10;

linera_sdk::contract!(Social);

impl WithContractAbi for Social {
    type Abi = social::SocialAbi;
}

#[async_trait]
impl Contract for Social {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        _argument: (),
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        // Validate that the application parameters were configured correctly.
        assert!(Self::parameters().is_ok());

        Ok(ExecutionOutcome::default())
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        operation: Operation,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        match operation {
            Operation::RequestSubscribe(chain_id) => {
                Ok(ExecutionOutcome::default().with_message(chain_id, Message::RequestSubscribe))
            }
            Operation::RequestUnsubscribe(chain_id) => {
                Ok(ExecutionOutcome::default().with_message(chain_id, Message::RequestUnsubscribe))
            }
            Operation::Post(text) => self.execute_post_operation(text).await,
        }
    }

    async fn execute_message(
        &mut self,
        context: &MessageContext,
        message: Message,
    ) -> Result<ExecutionOutcome<Self::Message>, Self::Error> {
        let mut outcome = ExecutionOutcome::default();
        match message {
            Message::RequestSubscribe => outcome.subscribe.push((
                ChannelName::from(POSTS_CHANNEL_NAME.to_vec()),
                context.message_id.chain_id,
            )),
            Message::RequestUnsubscribe => outcome.unsubscribe.push((
                ChannelName::from(POSTS_CHANNEL_NAME.to_vec()),
                context.message_id.chain_id,
            )),
            Message::Posts { count, posts } => self.execute_posts_message(context, count, posts)?,
        }
        Ok(outcome)
    }

    async fn handle_application_call(
        &mut self,
        _context: &CalleeContext,
        _call: (),
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<
        ApplicationCallOutcome<Self::Message, Self::Response, Self::SessionState>,
        Self::Error,
    > {
        Err(Error::ApplicationCallsNotSupported)
    }

    async fn handle_session_call(
        &mut self,
        _context: &CalleeContext,
        _state: Self::SessionState,
        _call: (),
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallOutcome<Self::Message, Self::Response, Self::SessionState>, Self::Error>
    {
        Err(Error::SessionsNotSupported)
    }
}

impl Social {
    async fn execute_post_operation(
        &mut self,
        text: String,
    ) -> Result<ExecutionOutcome<Message>, Error> {
        let timestamp = system_api::current_system_time();
        self.own_posts.push(OwnPost { timestamp, text });
        let count = self.own_posts.count();
        let mut posts = vec![];
        for index in (0..count).rev().take(RECENT_POSTS) {
            let maybe_post = self.own_posts.get(index).await?;
            let own_post = maybe_post
                .expect("post with valid index missing; this is a bug in the social application!");
            posts.push(own_post);
        }
        let count = count as u64;
        let dest = Destination::Subscribers(ChannelName::from(POSTS_CHANNEL_NAME.to_vec()));
        Ok(ExecutionOutcome::default().with_message(dest, Message::Posts { count, posts }))
    }

    fn execute_posts_message(
        &mut self,
        context: &MessageContext,
        count: u64,
        posts: Vec<OwnPost>,
    ) -> Result<(), Error> {
        for (index, post) in (0..count).rev().zip(posts) {
            let key = Key {
                timestamp: post.timestamp,
                author: context.message_id.chain_id,
                index,
            };
            self.received_posts.insert(&key, post.text)?;
        }
        Ok(())
    }
}

/// An error that can occur during the contract execution.
#[derive(Debug, Error)]
pub enum Error {
    /// Social application doesn't support any cross-application sessions.
    #[error("Social application doesn't support any cross-application sessions")]
    SessionsNotSupported,

    /// Social application doesn't support any cross-application sessions.
    #[error("Social application doesn't support any application calls")]
    ApplicationCallsNotSupported,

    /// View error.
    #[error(transparent)]
    View(#[from] ViewError),

    /// Failed to deserialize BCS bytes
    #[error("Failed to deserialize BCS bytes")]
    BcsError(#[from] bcs::Error),

    /// Failed to deserialize JSON string
    #[error("Failed to deserialize JSON string")]
    JsonError(#[from] serde_json::Error),
}
