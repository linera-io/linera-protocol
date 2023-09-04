// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Different request types for different runtimes.

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
    ReadKeyBytes {
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
            BaseRequest::ReadKeyBytes { key, .. } => formatter
                .debug_struct("BaseRequest::ReadKeyBytes")
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
