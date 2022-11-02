// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod exported_future;

use async_trait::async_trait;
use std::error::Error;

pub use crate::exported_future::ExportedFuture;

/// The public entry points provided by an application.
#[async_trait]
pub trait Application {
    /// Message reports for application execution errors.
    type Error: Error;

    /// Apply an operation from the current block.
    async fn execute_operation(
        &mut self,
        context: &OperationContext,
        operation: &[u8],
    ) -> Result<ExecutionResult, Self::Error>;

    /// Apply an effect originating from a cross-chain message.
    async fn execute_effect(
        &mut self,
        context: &EffectContext,
        effect: &[u8],
    ) -> Result<ExecutionResult, Self::Error>;

    /// Allow an operation or an effect of other applications to call into this
    /// application.
    async fn call_application(
        &mut self,
        context: &CalleeContext,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error>;

    /// Allow an operation or an effect of other applications to call into a session that
    /// we previously created.
    async fn call_session(
        &mut self,
        context: &CalleeContext,
        session: Session,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<SessionCallResult, Self::Error>;

    /// Allow an end user to execute read-only queries on the state of this application.
    /// NOTE: This is not meant to be metered and may not be exposed by validators.
    async fn query_application(
        &self,
        context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error>;
}

#[derive(Debug, Clone)]
pub struct OperationContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The current block height.
    pub height: BlockHeight,
    /// The current index of the operation.
    pub index: u64,
}

#[derive(Debug, Clone)]
pub struct EffectContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The current block height.
    pub height: BlockHeight,
    /// The id of the effect (based on the operation height and index in the remote
    /// chain that created the effect).
    pub effect_id: EffectId,
}

#[derive(Debug, Clone)]
pub struct CalleeContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// `None` if the caller doesn't want this particular call to be authenticated (e.g.
    /// for safety reasons).
    pub authenticated_caller_id: Option<ApplicationId>,
}

#[derive(Debug, Clone)]
pub struct QueryContext {
    /// The current chain id.
    pub chain_id: ChainId,
}

/// Externally visible results of an execution. These results are meant in the context of
/// the application that created them.
#[derive(Debug, Default)]
pub struct ExecutionResult {
    /// Send messages to the given destinations.
    pub effects: Vec<(Destination, Vec<u8>)>,
    /// Subscribe chains to channels.
    pub subscribe: Vec<(String, ChainId)>,
    /// Unsubscribe chains to channels.
    pub unsubscribe: Vec<(String, ChainId)>,
}

/// The index of an effect in a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug)]
pub struct EffectId {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub index: u64,
}

/// The unique identifier (UID) of a chain. This is the hash value of a ChainDescription.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash)]
pub struct ChainId(pub HashValue);

/// A block height to identify blocks in a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug)]
pub struct BlockHeight(pub u64);

/// A Sha512 value.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct HashValue([u8; 64]);

impl From<[u64; 8]> for HashValue {
    fn from(eight_u64s: [u64; 8]) -> Self {
        let mut bytes = [0u8; 64];

        bytes[0..8].copy_from_slice(&eight_u64s[0].to_le_bytes());
        bytes[8..16].copy_from_slice(&eight_u64s[1].to_le_bytes());
        bytes[16..24].copy_from_slice(&eight_u64s[2].to_le_bytes());
        bytes[24..32].copy_from_slice(&eight_u64s[3].to_le_bytes());
        bytes[32..40].copy_from_slice(&eight_u64s[4].to_le_bytes());
        bytes[40..48].copy_from_slice(&eight_u64s[5].to_le_bytes());
        bytes[48..56].copy_from_slice(&eight_u64s[6].to_le_bytes());
        bytes[56..64].copy_from_slice(&eight_u64s[7].to_le_bytes());

        HashValue(bytes)
    }
}

impl From<HashValue> for [u64; 8] {
    fn from(hash_value: HashValue) -> Self {
        let bytes = hash_value.0;
        let mut eight_u64s = [0u64; 8];

        eight_u64s[0] = u64::from_le_bytes(bytes[0..8].try_into().expect("incorrect indices"));
        eight_u64s[1] = u64::from_le_bytes(bytes[8..16].try_into().expect("incorrect indices"));
        eight_u64s[2] = u64::from_le_bytes(bytes[16..24].try_into().expect("incorrect indices"));
        eight_u64s[3] = u64::from_le_bytes(bytes[24..32].try_into().expect("incorrect indices"));
        eight_u64s[4] = u64::from_le_bytes(bytes[32..40].try_into().expect("incorrect indices"));
        eight_u64s[5] = u64::from_le_bytes(bytes[40..48].try_into().expect("incorrect indices"));
        eight_u64s[6] = u64::from_le_bytes(bytes[48..56].try_into().expect("incorrect indices"));
        eight_u64s[7] = u64::from_le_bytes(bytes[56..64].try_into().expect("incorrect indices"));

        eight_u64s
    }
}

/// The destination of a message, relative to a particular application.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Destination {
    /// Direct message to a chain.
    Recipient(ChainId),
    /// Broadcast to the current subscribers of our channel.
    Subscribers(String),
}

/// A unique identifier for an application.
// FIXME: placeholder
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug)]
pub struct ApplicationId(pub u64);

/// The identifier of a session.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct SessionId {
    /// The application that runs the session.
    pub application_id: ApplicationId,
    /// User-defined tag.
    pub kind: u64,
    /// Unique index set by the runtime.
    pub index: u64,
}

/// Syscall to request creating a new session.
#[derive(Default)]
pub struct Session {
    /// A kind provided by the creator (meant to be visible to other applications).
    pub kind: u64,
    /// The data associated to the session.
    pub data: Vec<u8>,
}

/// The result of calling into a user application.
#[derive(Default)]
pub struct ApplicationCallResult {
    /// The return value.
    pub value: Vec<u8>,
    /// The externally-visible result.
    pub execution_result: ExecutionResult,
    /// The new sessions that were just created by the callee for us.
    pub create_sessions: Vec<Session>,
}

/// The result of calling into a session.
#[derive(Default)]
pub struct SessionCallResult {
    /// The application result.
    pub inner: ApplicationCallResult,
    /// If `call_session` was called, this tells the system to clean up the session.
    pub data: Option<Vec<u8>>,
}
