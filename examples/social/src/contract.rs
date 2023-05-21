// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use async_trait::async_trait;
use linera_sdk::{
    base::{ChannelName, Destination, SessionId},
    contract::system_api,
    ApplicationCallResult, CalleeContext, Contract, EffectContext, ExecutionResult, FromBcsBytes,
    OperationContext, SessionCallResult, ViewStateStorage,
};
use linera_views::views::ViewError;
use social::{Effect, Key, Operation, OwnPost};
use state::Social;
use thiserror::Error;

/// The channel name the application uses for cross-chain messages about new posts.
const POSTS_CHANNEL_NAME: &[u8] = b"posts";
/// The number of recent posts sent in each cross-chain message.
const RECENT_POSTS: usize = 10;

linera_sdk::contract!(Social);

#[async_trait]
impl Contract for Social {
    type Error = Error;
    type Storage = ViewStateStorage<Self>;

    async fn initialize(
        &mut self,
        _context: &OperationContext,
        _argument: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        Ok(ExecutionResult::default())
    }

    async fn execute_operation(
        &mut self,
        _context: &OperationContext,
        operation: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        match Operation::from_bcs_bytes(operation).map_err(Error::InvalidOperation)? {
            Operation::RequestSubscribe(chain_id) => {
                Ok(ExecutionResult::default().with_effect(chain_id, &Effect::RequestSubscribe))
            }
            Operation::RequestUnsubscribe(chain_id) => {
                Ok(ExecutionResult::default().with_effect(chain_id, &Effect::RequestUnsubscribe))
            }
            Operation::Post(text) => self.execute_post_operation(text).await,
        }
    }

    async fn execute_effect(
        &mut self,
        context: &EffectContext,
        effect: &[u8],
    ) -> Result<ExecutionResult, Self::Error> {
        let mut result = ExecutionResult::default();
        match Effect::from_bcs_bytes(effect).map_err(Error::InvalidEffect)? {
            Effect::RequestSubscribe => result.subscribe.push((
                ChannelName::from(POSTS_CHANNEL_NAME.to_vec()),
                context.effect_id.chain_id,
            )),
            Effect::RequestUnsubscribe => result.unsubscribe.push((
                ChannelName::from(POSTS_CHANNEL_NAME.to_vec()),
                context.effect_id.chain_id,
            )),
            Effect::Posts { count, posts } => self.execute_posts_effect(context, count, posts)?,
        }
        Ok(result)
    }

    async fn handle_application_call(
        &mut self,
        _context: &CalleeContext,
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error> {
        Err(Error::ApplicationCallsNotSupported)
    }

    async fn handle_session_call(
        &mut self,
        _context: &CalleeContext,
        _session: &[u8],
        _argument: &[u8],
        _forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, Self::Error> {
        Err(Error::SessionsNotSupported)
    }
}

impl Social {
    async fn execute_post_operation(&mut self, text: String) -> Result<ExecutionResult, Error> {
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
        Ok(ExecutionResult::default().with_effect(dest, &Effect::Posts { count, posts }))
    }

    fn execute_posts_effect(
        &mut self,
        context: &EffectContext,
        count: u64,
        posts: Vec<OwnPost>,
    ) -> Result<(), Error> {
        for (index, post) in (0..count).rev().zip(posts) {
            let key = Key {
                timestamp: post.timestamp,
                author: context.effect_id.chain_id,
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

    /// Invalid serialized operation.
    #[error("Invalid operation")]
    InvalidOperation(bcs::Error),

    /// Invalid serialized effect.
    #[error("Invalid effect")]
    InvalidEffect(bcs::Error),

    /// View error.
    #[error(transparent)]
    View(#[from] ViewError),
}

#[cfg(test)]
mod tests {
    use linera_sdk::base::{ChainId, Timestamp};
    use linera_views::{common::CustomSerialize, memory::MemoryContext};
    use social::Key;
    use webassembly_test::webassembly_test;

    #[webassembly_test]
    fn key_custom_serialize() {
        let key = Key {
            timestamp: Timestamp::from(0x123456789ABCDEF),
            author: ChainId([0x12345, 0x6789A, 0xBCDEF, 0x0248A].into()),
            index: 0xFEDCBA9876543210,
        };
        let ser_key = key
            .to_custom_bytes::<MemoryContext<()>>()
            .expect("serialize");
        let deser_key = Key::from_custom_bytes::<MemoryContext<()>>(&ser_key).expect("deserialize");
        assert_eq!(key, deser_key);
    }
}
