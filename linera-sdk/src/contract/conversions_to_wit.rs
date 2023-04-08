// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Conversions from types declared in [`linera-sdk`] to types generated by [`wit-bindgen-guest-rust`].

use super::{wit_types, writable_system as system};
use crate::{ApplicationCallResult, ExecutionResult, Session, SessionCallResult};
use linera_base::{
    crypto::CryptoHash,
    identifiers::{ApplicationId, ChannelName, Destination, EffectId, SessionId},
};
use std::task::Poll;

impl From<CryptoHash> for system::CryptoHash {
    fn from(hash_value: CryptoHash) -> Self {
        let parts = <[u64; 4]>::from(hash_value);

        system::CryptoHash {
            part1: parts[0],
            part2: parts[1],
            part3: parts[2],
            part4: parts[3],
        }
    }
}

impl From<CryptoHash> for wit_types::CryptoHash {
    fn from(crypto_hash: CryptoHash) -> Self {
        let parts = <[u64; 4]>::from(crypto_hash);

        wit_types::CryptoHash {
            part1: parts[0],
            part2: parts[1],
            part3: parts[2],
            part4: parts[3],
        }
    }
}

impl From<ApplicationId> for system::ApplicationId {
    fn from(application_id: ApplicationId) -> system::ApplicationId {
        system::ApplicationId {
            bytecode_id: application_id.bytecode_id.0.into(),
            creation: application_id.creation.into(),
        }
    }
}

impl From<SessionId> for system::SessionId {
    fn from(session_id: SessionId) -> Self {
        system::SessionId {
            application_id: session_id.application_id.into(),
            kind: session_id.kind,
            index: session_id.index,
        }
    }
}

impl From<EffectId> for system::EffectId {
    fn from(effect_id: EffectId) -> Self {
        system::EffectId {
            chain_id: effect_id.chain_id.0.into(),
            height: effect_id.height.0,
            index: effect_id.index,
        }
    }
}

impl From<log::Level> for system::LogLevel {
    fn from(level: log::Level) -> Self {
        match level {
            log::Level::Trace => system::LogLevel::Trace,
            log::Level::Debug => system::LogLevel::Debug,
            log::Level::Info => system::LogLevel::Info,
            log::Level::Warn => system::LogLevel::Warn,
            log::Level::Error => system::LogLevel::Error,
        }
    }
}

impl From<ApplicationCallResult> for wit_types::ApplicationCallResult {
    fn from(result: ApplicationCallResult) -> Self {
        let create_sessions = result
            .create_sessions
            .into_iter()
            .map(wit_types::Session::from)
            .collect();

        wit_types::ApplicationCallResult {
            create_sessions,
            execution_result: result.execution_result.into(),
            value: result.value,
        }
    }
}

impl From<Session> for wit_types::Session {
    fn from(new_session: Session) -> Self {
        wit_types::Session {
            kind: new_session.kind,
            data: new_session.data,
        }
    }
}

impl From<SessionCallResult> for wit_types::SessionCallResult {
    fn from(result: SessionCallResult) -> Self {
        wit_types::SessionCallResult {
            inner: result.inner.into(),
            data: result.data,
        }
    }
}

impl From<ExecutionResult> for wit_types::ExecutionResult {
    fn from(result: ExecutionResult) -> Self {
        let effects = result
            .effects
            .into_iter()
            .map(|(destination, authenticated, effect)| (destination.into(), authenticated, effect))
            .collect();

        let subscribe = result
            .subscribe
            .into_iter()
            .map(|(subscription, chain_id)| (subscription.into(), chain_id.0.into()))
            .collect();

        let unsubscribe = result
            .unsubscribe
            .into_iter()
            .map(|(subscription, chain_id)| (subscription.into(), chain_id.0.into()))
            .collect();

        wit_types::ExecutionResult {
            effects,
            subscribe,
            unsubscribe,
        }
    }
}

impl From<Destination> for wit_types::Destination {
    fn from(destination: Destination) -> Self {
        match destination {
            Destination::Recipient(chain_id) => {
                wit_types::Destination::Recipient(chain_id.0.into())
            }
            Destination::Subscribers(subscription) => {
                wit_types::Destination::Subscribers(subscription.into())
            }
        }
    }
}

impl From<ChannelName> for wit_types::ChannelName {
    fn from(name: ChannelName) -> Self {
        wit_types::ChannelName {
            name: name.into_bytes(),
        }
    }
}

impl From<Poll<Result<ExecutionResult, String>>> for wit_types::PollExecutionResult {
    fn from(poll: Poll<Result<ExecutionResult, String>>) -> Self {
        use wit_types::PollExecutionResult;
        match poll {
            Poll::Pending => PollExecutionResult::Pending,
            Poll::Ready(Ok(result)) => PollExecutionResult::Ready(Ok(result.into())),
            Poll::Ready(Err(message)) => PollExecutionResult::Ready(Err(message)),
        }
    }
}

impl From<Poll<Result<ApplicationCallResult, String>>> for wit_types::PollCallApplication {
    fn from(poll: Poll<Result<ApplicationCallResult, String>>) -> Self {
        use wit_types::PollCallApplication;
        match poll {
            Poll::Pending => PollCallApplication::Pending,
            Poll::Ready(Ok(result)) => PollCallApplication::Ready(Ok(result.into())),
            Poll::Ready(Err(message)) => PollCallApplication::Ready(Err(message)),
        }
    }
}

impl From<Poll<Result<SessionCallResult, String>>> for wit_types::PollCallSession {
    fn from(poll: Poll<Result<SessionCallResult, String>>) -> Self {
        use wit_types::PollCallSession;
        match poll {
            Poll::Pending => PollCallSession::Pending,
            Poll::Ready(Ok(result)) => PollCallSession::Ready(Ok(result.into())),
            Poll::Ready(Err(message)) => PollCallSession::Ready(Err(message)),
        }
    }
}
