// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod contract;
#[cfg(feature = "crypto")]
pub mod crypto;
mod ensure;
mod exported_future;
mod extensions;
mod log;
pub mod service;

use async_trait::async_trait;
use custom_debug_derive::Debug;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::{error::Error, fmt, sync::Arc};

pub use self::{
    exported_future::ExportedFuture,
    extensions::FromBcsBytes,
    log::{ContractLogger, ServiceLogger},
};
#[doc(hidden)]
pub use wit_bindgen_guest_rust;

#[cfg(not(target_arch = "wasm32"))]
pub use linera_base::crypto::BcsSignable;

/// Activate the blanket implementation of `Signable` based on serde and BCS.
/// * We use `serde_name` to extract a seed from the name of structs and enums.
/// * We use `BCS` to generate canonical bytes suitable for hashing and signing.
#[cfg(target_arch = "wasm32")]
pub trait BcsSignable: Serialize + serde::de::DeserializeOwned {}

/// A simple state management runtime using a single byte array.
pub struct SimpleStateStorage<A>(std::marker::PhantomData<A>);

/// A state management runtime based on `linera-views`.
pub struct ViewStateStorage<A>(std::marker::PhantomData<A>);

/// The public entry points provided by a contract.
#[async_trait]
pub trait Contract: Sized {
    /// Message reports for application execution errors.
    type Error: Error;
    /// Tag the contract with the desired state management runtime.
    type Storage;

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
    async fn handle_application_call(
        &mut self,
        context: &CalleeContext,
        argument: &[u8],
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<ApplicationCallResult, Self::Error>;

    /// Allow an operation or an effect of other applications to call into a session that
    /// we previously created.
    async fn handle_session_call(
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
    /// Mark the contract with the desired state management runtime.
    type Storage;

    /// Allow an end user to execute read-only queries on the state of this application.
    /// NOTE: This is not meant to be metered and may not be exposed by validators.
    async fn query_application(
        self: Arc<Self>,
        context: &QueryContext,
        argument: &[u8],
    ) -> Result<Vec<u8>, Self::Error>;
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OperationContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The current block height.
    pub height: BlockHeight,
    /// The current index of the operation.
    pub index: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EffectContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// The current block height.
    pub height: BlockHeight,
    /// The id of the effect (based on the operation height and index in the remote
    /// chain that created the effect).
    pub effect_id: EffectId,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CalleeContext {
    /// The current chain id.
    pub chain_id: ChainId,
    /// `None` if the caller doesn't want this particular call to be authenticated (e.g.
    /// for safety reasons).
    pub authenticated_caller_id: Option<ApplicationId>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueryContext {
    /// The current chain id.
    pub chain_id: ChainId,
}

/// Externally visible results of an execution. These results are meant in the context of
/// the application that created them.
#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ExecutionResult {
    /// Send messages to the given destinations, possibly forwarding the authenticated
    /// signer.
    pub effects: Vec<(Destination, bool, Vec<u8>)>,
    /// Subscribe chains to channels.
    pub subscribe: Vec<(ChannelName, ChainId)>,
    /// Unsubscribe chains to channels.
    pub unsubscribe: Vec<(ChannelName, ChainId)>,
}

impl ExecutionResult {
    /// Add an effect to the execution result.
    pub fn with_effect(
        mut self,
        destination: impl Into<Destination>,
        effect: &impl Serialize,
    ) -> Self {
        let effect_bytes = bcs::to_bytes(effect).expect("Effect should be serializable");
        self.effects.push((destination.into(), false, effect_bytes));
        self
    }

    /// Add an authenticated effect to the execution result.
    pub fn with_authenticated_effect(
        mut self,
        destination: impl Into<Destination>,
        effect: &impl Serialize,
    ) -> Self {
        let effect_bytes = bcs::to_bytes(effect).expect("Effect should be serializable");
        self.effects.push((destination.into(), true, effect_bytes));
        self
    }
}

/// The index of an effect in a chain.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Deserialize, Serialize)]
pub struct EffectId {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub index: u64,
}

/// The unique identifier (UID) of a chain. This is the hash value of a ChainDescription.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Deserialize, Serialize)]
pub struct ChainId(pub CryptoHash);

/// The name of a subscription channel.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct ChannelName(pub Vec<u8>);

/// A block height to identify blocks in a chain.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Deserialize, Serialize,
)]
pub struct BlockHeight(pub u64);

/// A Sha3-256 value.
#[serde_as]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct CryptoHash(#[serde_as(as = "[_; 32]")] [u8; 32]);

impl From<[u64; 4]> for CryptoHash {
    fn from(integers: [u64; 4]) -> Self {
        let mut bytes = [0u8; 32];

        bytes[0..8].copy_from_slice(&integers[0].to_le_bytes());
        bytes[8..16].copy_from_slice(&integers[1].to_le_bytes());
        bytes[16..24].copy_from_slice(&integers[2].to_le_bytes());
        bytes[24..32].copy_from_slice(&integers[3].to_le_bytes());

        CryptoHash(bytes)
    }
}

impl From<CryptoHash> for [u64; 4] {
    fn from(crypto_hash: CryptoHash) -> Self {
        let bytes = crypto_hash.0;
        let mut integers = [0u64; 4];

        integers[0] = u64::from_le_bytes(bytes[0..8].try_into().expect("incorrect indices"));
        integers[1] = u64::from_le_bytes(bytes[8..16].try_into().expect("incorrect indices"));
        integers[2] = u64::from_le_bytes(bytes[16..24].try_into().expect("incorrect indices"));
        integers[3] = u64::from_le_bytes(bytes[24..32].try_into().expect("incorrect indices"));

        integers
    }
}

/// The destination of a message, relative to a particular application.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub enum Destination {
    /// Direct message to a chain.
    Recipient(ChainId),
    /// Broadcast to the current subscribers of our channel.
    Subscribers(ChannelName),
}

impl From<ChainId> for Destination {
    fn from(chain_id: ChainId) -> Self {
        Destination::Recipient(chain_id)
    }
}

/// A unique identifier for an application.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, Deserialize, Serialize)]
pub struct ApplicationId {
    /// The bytecode to use for the application.
    pub bytecode: BytecodeId,
    /// The unique ID of the application's creation.
    pub creation: EffectId,
}

/// A unique identifier for an application bytecode.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct BytecodeId(pub EffectId);

/// The identifier of a session.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub struct SessionId {
    /// The application that runs the session.
    pub application_id: ApplicationId,
    /// User-defined tag.
    pub kind: u64,
    /// Unique index set by the runtime.
    pub index: u64,
}

/// Syscall to request creating a new session.
#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct Session {
    /// A kind provided by the creator (meant to be visible to other applications).
    pub kind: u64,
    /// The data associated to the session.
    #[debug(with = "hex_debug")]
    pub data: Vec<u8>,
}

/// The result of calling into a user application.
#[derive(Debug, Default, Deserialize, Serialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ApplicationCallResult {
    /// The return value.
    #[debug(with = "hex_debug")]
    pub value: Vec<u8>,
    /// The externally-visible result.
    pub execution_result: ExecutionResult,
    /// The new sessions that were just created by the callee for us.
    pub create_sessions: Vec<Session>,
}

/// The result of calling into a session.
#[derive(Default, Deserialize, Serialize)]
pub struct SessionCallResult {
    /// The application result.
    pub inner: ApplicationCallResult,
    /// If `call_session` was called, this tells the system to clean up the session.
    pub data: Option<Vec<u8>>,
}

/// The balance of a chain.
#[derive(Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Deserialize, Serialize)]
pub struct SystemBalance(pub u128);

/// A timestamp, in microseconds since the Unix epoch.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Deserialize, Serialize,
)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Returns the number of microseconds since the Unix epoch.
    pub fn micros(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Timestamp {
    fn from(t: u64) -> Timestamp {
        Timestamp(t)
    }
}

/// Prints a vector of bytes in hexadecimal.
pub fn hex_debug<T: AsRef<[u8]>>(bytes: &T, f: &mut fmt::Formatter) -> fmt::Result {
    for byte in bytes.as_ref() {
        write!(f, "{:02x}", byte)?;
    }
    Ok(())
}
