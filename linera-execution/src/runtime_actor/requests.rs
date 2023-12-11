// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Different request types for different runtimes.

use super::sync_response::SyncSender;
use crate::{CallResult, UserApplicationId};
use linera_base::{
    data_types::{Amount, Timestamp},
    identifiers::{ChainId, SessionId},
};
use linera_views::batch::Batch;
use std::fmt::{self, Debug, Formatter};

/// Requests shared by contracts and services.
pub enum BaseRequest {
    /// Requests the current chain id.
    ChainId {
        response_sender: oneshot::Sender<ChainId>,
    },

    /// Requests the current application id.
    ApplicationId {
        response_sender: oneshot::Sender<UserApplicationId>,
    },

    /// Requests the current application parameters.
    ApplicationParameters {
        response_sender: oneshot::Sender<Vec<u8>>,
    },

    /// Requests to read the system balance.
    ReadSystemBalance {
        response_sender: oneshot::Sender<Amount>,
    },

    /// Requests to read the system timestamp.
    ReadSystemTimestamp {
        response_sender: oneshot::Sender<Timestamp>,
    },

    /// Requests to read the application state.
    TryReadMyState {
        response_sender: oneshot::Sender<Vec<u8>>,
    },

    /// Requests to lock the view user state and prevent further reading/loading.
    LockViewUserState {
        response_sender: oneshot::Sender<()>,
    },

    /// Requests to unlocks the view user state and allow reading/loading again.
    UnlockViewUserState {
        response_sender: oneshot::Sender<()>,
    },

    /// Requests to read an entry from the key-value store.
    ContainsKey {
        key: Vec<u8>,
        response_sender: oneshot::Sender<bool>,
    },

    /// Requests to read an entry from the key-value store.
    ReadValueBytes {
        key: Vec<u8>,
        response_sender: oneshot::Sender<Option<Vec<u8>>>,
    },

    /// Requests to read the keys that have a specific prefix.
    FindKeysByPrefix {
        key_prefix: Vec<u8>,
        response_sender: oneshot::Sender<Vec<Vec<u8>>>,
    },

    /// Requests to read the entries whose keys have a specific prefix.
    FindKeyValuesByPrefix {
        key_prefix: Vec<u8>,
        response_sender: oneshot::Sender<Vec<(Vec<u8>, Vec<u8>)>>,
    },
}

impl Debug for BaseRequest {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        match self {
            BaseRequest::ChainId { .. } => formatter
                .debug_struct("BaseRequest::ChainId")
                .finish_non_exhaustive(),
            BaseRequest::ApplicationId { .. } => formatter
                .debug_struct("BaseRequest::ApplicationId")
                .finish_non_exhaustive(),
            BaseRequest::ApplicationParameters { .. } => formatter
                .debug_struct("BaseRequest::ApplicationParameters")
                .finish_non_exhaustive(),
            BaseRequest::ReadSystemBalance { .. } => formatter
                .debug_struct("BaseRequest::ReadSystemBalance")
                .finish_non_exhaustive(),
            BaseRequest::ReadSystemTimestamp { .. } => formatter
                .debug_struct("BaseRequest::ReadSystemTimestamp")
                .finish_non_exhaustive(),
            BaseRequest::TryReadMyState { .. } => formatter
                .debug_struct("BaseRequest::TryReadMyState")
                .finish_non_exhaustive(),
            BaseRequest::LockViewUserState { .. } => formatter
                .debug_struct("BaseRequest::LockViewUserState")
                .finish_non_exhaustive(),
            BaseRequest::UnlockViewUserState { .. } => formatter
                .debug_struct("BaseRequest::UnlockViewUserState")
                .finish_non_exhaustive(),
            BaseRequest::ContainsKey { key, .. } => formatter
                .debug_struct("BaseRequest::ContainsKey")
                .field("key", key)
                .finish_non_exhaustive(),
            BaseRequest::ReadValueBytes { key, .. } => formatter
                .debug_struct("BaseRequest::ReadValueBytes")
                .field("key", key)
                .finish_non_exhaustive(),
            BaseRequest::FindKeysByPrefix { key_prefix, .. } => formatter
                .debug_struct("BaseRequest::FindKeysByPrefix")
                .field("key_prefix", key_prefix)
                .finish_non_exhaustive(),
            BaseRequest::FindKeyValuesByPrefix { key_prefix, .. } => formatter
                .debug_struct("BaseRequest::FindKeyValuesByPrefix")
                .field("key_prefix", key_prefix)
                .finish_non_exhaustive(),
        }
    }
}

/// Requests from application contracts.
///
/// Most of the requests use [`SyncSender`]s to force the respective system APIs to be blocking.
/// This is needed to enforce determinism, otherwise it's possible to queue two attempts to acquire
/// the write lock, and that forces any attempts to acquire the read lock between the two writes to
/// be pushed to the back of the queue. That would change the order of execution depending on the
/// order the locks are acquired and released.
///
/// Consider for example in one validator the operation using the first write lock completes before
/// the second operation that acquires the write lock starts. Any reads between the two operations
/// will get changes from the first operation and no changes from the second operation. However, if
/// on a different validator the second operation starts and attempts to acquire the lock while the
/// first operation is still executing, the read operations will get pushed back and will read
/// changes from both operations. This is due to the write-preferring behavior of
/// [`async_lock::RwLock`].
pub enum ContractRequest {
    /// Requests that are valid for both contracts and services.
    Base(BaseRequest),

    /// Requests the amount of execution fuel remaining before execution is aborted.
    RemainingFuel {
        response_sender: oneshot::Sender<u64>,
    },

    /// Requests to set the amount of execution fuel remaining before execution is aborted.
    SetRemainingFuel {
        remaining_fuel: u64,
        response_sender: SyncSender<()>,
    },

    /// Requests to read the application state and prevent further reading/loading until the state
    /// is saved or unlocked.
    TryReadAndLockMyState {
        response_sender: SyncSender<Option<Vec<u8>>>,
    },

    /// Requests to save the application state and allow reading/loading the state again.
    SaveAndUnlockMyState {
        state: Vec<u8>,
        response_sender: SyncSender<bool>,
    },

    /// Requests to unlock the application state without saving anything and allow reading/loading
    /// it again.
    UnlockMyState { response_sender: SyncSender<()> },

    /// Requests to write the batch and unlock the application state to allow further
    /// reading/loading it.
    WriteBatchAndUnlock {
        batch: Batch,
        response_sender: SyncSender<()>,
    },

    /// Requests to call another application.
    TryCallApplication {
        authenticated: bool,
        callee_id: UserApplicationId,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
        response_sender: SyncSender<CallResult>,
    },

    /// Calls into a session that is in our scope.
    TryCallSession {
        authenticated: bool,
        session_id: SessionId,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
        response_sender: SyncSender<CallResult>,
    },
}

impl Debug for ContractRequest {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        match self {
            ContractRequest::Base(base_request) => formatter
                .debug_tuple("ContractRequest::Base")
                .field(base_request)
                .finish(),

            ContractRequest::RemainingFuel { .. } => formatter
                .debug_struct("ContractRequest::RemainingFuel")
                .finish_non_exhaustive(),

            ContractRequest::SetRemainingFuel { remaining_fuel, .. } => formatter
                .debug_struct("ContractRequest::SetRemainingFuel")
                .field("remaining_fuel", remaining_fuel)
                .finish_non_exhaustive(),

            ContractRequest::TryReadAndLockMyState { .. } => formatter
                .debug_struct("ContractRequest::TryReadAndLockMyState")
                .finish_non_exhaustive(),

            ContractRequest::SaveAndUnlockMyState { state, .. } => formatter
                .debug_struct("ContractRequest::SaveAndUnlockMyState")
                .field("state", state)
                .finish_non_exhaustive(),

            ContractRequest::UnlockMyState { .. } => formatter
                .debug_struct("ContractRequest::UnlockMyState")
                .finish_non_exhaustive(),

            ContractRequest::WriteBatchAndUnlock { .. } => formatter
                .debug_struct("ContractRequest::WriteBatchAndUnlock")
                .field("batch", &"Batch")
                .finish_non_exhaustive(),

            ContractRequest::TryCallApplication {
                authenticated,
                callee_id,
                argument,
                forwarded_sessions,
                ..
            } => formatter
                .debug_struct("ContractRequest::TryCallApplication")
                .field("authenticated", authenticated)
                .field("callee_id", callee_id)
                .field("argument", argument)
                .field("forwarded_sessions", forwarded_sessions)
                .finish_non_exhaustive(),

            ContractRequest::TryCallSession {
                authenticated,
                session_id,
                argument,
                forwarded_sessions,
                ..
            } => formatter
                .debug_struct("ContractRequest::TryCallSession")
                .field("authenticated", authenticated)
                .field("session_id", session_id)
                .field("argument", argument)
                .field("forwarded_sessions", forwarded_sessions)
                .finish_non_exhaustive(),
        }
    }
}

/// Requests from application services.
pub enum ServiceRequest {
    /// Requests that are valid for both contracts and services.
    Base(BaseRequest),

    /// Requests to query another application.
    TryQueryApplication {
        queried_id: UserApplicationId,
        argument: Vec<u8>,
        response_sender: oneshot::Sender<Vec<u8>>,
    },
}

impl Debug for ServiceRequest {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        match self {
            ServiceRequest::Base(base_request) => formatter
                .debug_tuple("ServiceRequest::Base")
                .field(base_request)
                .finish(),

            ServiceRequest::TryQueryApplication {
                queried_id,
                argument,
                ..
            } => formatter
                .debug_struct("ServiceRequest::TryQueryApplication")
                .field("queried_id", queried_id)
                .field("argument", argument)
                .finish_non_exhaustive(),
        }
    }
}
