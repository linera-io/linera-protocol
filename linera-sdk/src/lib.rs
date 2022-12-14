// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod exported_future;

use async_trait::async_trait;
use std::error::Error;

#[cfg(feature = "serde")]
use {
    serde::{Deserialize, Serialize},
    serde_with::serde_as,
};

pub use crate::exported_future::ExportedFuture;

/// The public entry points provided by a contract.
#[async_trait]
pub trait Contract {
    /// Message reports for application execution errors.
    type Error: Error;

    /// Initialize the application on the chain that created it.
    async fn initialize(
        &mut self,
        context: &OperationContext,
        argument: &[u8],
    ) -> Result<ExecutionResult, Self::Error>;

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
}

/// The public entry points provided by a service.
#[async_trait]
pub trait Service {
    /// Message reports for service execution errors.
    type Error: Error;

    /// Allow an end user to execute read-only queries on the state of this application.
    /// NOTE: This is not meant to be metered and may not be exposed by validators.
    async fn query_application(
        &self,
        context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error>;
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct OperationContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The current block height.
    pub height: BlockHeight,
    /// The current index of the operation.
    pub index: u64,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
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
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct CalleeContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// `None` if the caller doesn't want this particular call to be authenticated (e.g.
    /// for safety reasons).
    pub authenticated_caller_id: Option<ApplicationId>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct QueryContext {
    /// The current chain id.
    pub chain_id: ChainId,
}

/// Externally visible results of an execution. These results are meant in the context of
/// the application that created them.
#[derive(Debug, Default)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct ExecutionResult {
    /// Send messages to the given destinations.
    pub effects: Vec<(Destination, Vec<u8>)>,
    /// Subscribe chains to channels.
    pub subscribe: Vec<(String, ChainId)>,
    /// Unsubscribe chains to channels.
    pub unsubscribe: Vec<(String, ChainId)>,
}

impl ExecutionResult {
    /// Add an effect to the execution result.
    #[cfg(feature = "serde")]
    pub fn with_effect(
        mut self,
        destination: impl Into<Destination>,
        effect: &impl Serialize,
    ) -> Self {
        let effect_bytes = bcs::to_bytes(effect).expect("Effect should be serializable");
        self.effects.push((destination.into(), effect_bytes));
        self
    }
}

/// The index of an effect in a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct EffectId {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub index: u64,
}

/// The unique identifier (UID) of a chain. This is the hash value of a ChainDescription.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct ChainId(pub HashValue);

/// A block height to identify blocks in a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct BlockHeight(pub u64);

/// A Sha512 value.
#[cfg(not(feature = "serde"))]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct HashValue([u8; 64]);

/// A Sha512 value.
#[cfg(feature = "serde")]
#[serde_as]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct HashValue(#[serde_as(as = "[_; 64]")] [u8; 64]);

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
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub enum Destination {
    /// Direct message to a chain.
    Recipient(ChainId),
    /// Broadcast to the current subscribers of our channel.
    Subscribers(String),
}

impl From<ChainId> for Destination {
    fn from(chain_id: ChainId) -> Self {
        Destination::Recipient(chain_id)
    }
}

/// A unique identifier for an application.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct ApplicationId {
    /// The bytecode to use for the application.
    pub bytecode: BytecodeId,
    /// The unique ID of the application's creation.
    pub creation: EffectId,
}

/// A unique identifier for an application bytecode.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct BytecodeId(pub EffectId);

/// The identifier of a session.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
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
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct Session {
    /// A kind provided by the creator (meant to be visible to other applications).
    pub kind: u64,
    /// The data associated to the session.
    pub data: Vec<u8>,
}

/// The result of calling into a user application.
#[derive(Default)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
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
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct SessionCallResult {
    /// The application result.
    pub inner: ApplicationCallResult,
    /// If `call_session` was called, this tells the system to clean up the session.
    pub data: Option<Vec<u8>>,
}

/// The balance of a chain.
#[derive(Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
pub struct SystemBalance(pub u128);
