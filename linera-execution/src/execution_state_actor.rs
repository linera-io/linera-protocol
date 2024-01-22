// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Handle requests from the synchronous execution thread of user applications.

use crate::{
    util::RespondExt, ExecutionError, ExecutionRuntimeContext, ExecutionStateView,
    UserApplicationDescription, UserApplicationId, UserContractCode, UserServiceCode,
};
use futures::channel::mpsc;
use linera_base::data_types::{Amount, Timestamp};
use linera_views::{
    batch::Batch,
    common::Context,
    views::{View, ViewError},
};
use oneshot::Sender;
use std::fmt::{self, Debug, Formatter};

pub(crate) type ExecutionStateSender = mpsc::UnboundedSender<Request>;

impl<C> ExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    // TODO(#1416): Support concurrent I/O.
    pub(crate) async fn handle_request(&mut self, request: Request) -> Result<(), ExecutionError> {
        use Request::*;
        match request {
            LoadContract { id, callback } => {
                let description = self.system.registry.describe_application(id).await?;
                let code = self
                    .context()
                    .extra()
                    .get_user_contract(&description)
                    .await?;
                callback.respond((code, description));
            }

            LoadService { id, callback } => {
                let description = self.system.registry.describe_application(id).await?;
                let code = self
                    .context()
                    .extra()
                    .get_user_service(&description)
                    .await?;
                callback.respond((code, description));
            }

            SystemBalance { callback } => {
                let balance = *self.system.balance.get();
                callback.respond(balance);
            }

            SystemTimestamp { callback } => {
                let timestamp = *self.system.timestamp.get();
                callback.respond(timestamp);
            }

            ContainsKey { id, key, callback } => {
                let view = self.users.try_load_entry_or_insert(&id).await?;
                let result = view.contains_key(&key).await?;
                callback.respond(result);
            }

            ReadMultiValuesBytes { id, keys, callback } => {
                let view = self.users.try_load_entry_or_insert(&id).await?;
                let values = view.multi_get(keys).await?;
                callback.respond(values);
            }

            ReadValueBytes { id, key, callback } => {
                let view = self.users.try_load_entry_or_insert(&id).await?;
                let result = view.get(&key).await?;
                callback.respond(result);
            }

            FindKeysByPrefix {
                id,
                key_prefix,
                callback,
            } => {
                let view = self.users.try_load_entry_or_insert(&id).await?;
                let result = view.find_keys_by_prefix(&key_prefix).await?;
                callback.respond(result);
            }

            FindKeyValuesByPrefix {
                id,
                key_prefix,
                callback,
            } => {
                let view = self.users.try_load_entry_or_insert(&id).await?;
                let result = view.find_key_values_by_prefix(&key_prefix).await?;
                callback.respond(result);
            }

            WriteBatch {
                id,
                batch,
                callback,
            } => {
                let mut view = self.users.try_load_entry_mut(&id).await?;
                view.write_batch(batch).await?;
                callback.respond(());
            }
        }

        Ok(())
    }
}

/// Requests to the execution state.
pub enum Request {
    LoadContract {
        id: UserApplicationId,
        callback: Sender<(UserContractCode, UserApplicationDescription)>,
    },

    LoadService {
        id: UserApplicationId,
        callback: Sender<(UserServiceCode, UserApplicationDescription)>,
    },

    SystemBalance {
        callback: Sender<Amount>,
    },

    SystemTimestamp {
        callback: Sender<Timestamp>,
    },

    ReadValueBytes {
        id: UserApplicationId,
        key: Vec<u8>,
        callback: Sender<Option<Vec<u8>>>,
    },

    ContainsKey {
        id: UserApplicationId,
        key: Vec<u8>,
        callback: Sender<bool>,
    },

    ReadMultiValuesBytes {
        id: UserApplicationId,
        keys: Vec<Vec<u8>>,
        callback: Sender<Vec<Option<Vec<u8>>>>,
    },

    FindKeysByPrefix {
        id: UserApplicationId,
        key_prefix: Vec<u8>,
        callback: Sender<Vec<Vec<u8>>>,
    },

    FindKeyValuesByPrefix {
        id: UserApplicationId,
        key_prefix: Vec<u8>,
        callback: Sender<Vec<(Vec<u8>, Vec<u8>)>>,
    },

    WriteBatch {
        id: UserApplicationId,
        batch: Batch,
        callback: Sender<()>,
    },
}

impl Debug for Request {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        match self {
            Request::LoadContract { id, .. } => formatter
                .debug_struct("Request::LoadContract")
                .field("id", id)
                .finish_non_exhaustive(),

            Request::LoadService { id, .. } => formatter
                .debug_struct("Request::LoadService")
                .field("id", id)
                .finish_non_exhaustive(),

            Request::SystemBalance { .. } => formatter
                .debug_struct("Request::SystemBalance")
                .finish_non_exhaustive(),

            Request::SystemTimestamp { .. } => formatter
                .debug_struct("Request::SystemTimestamp")
                .finish_non_exhaustive(),

            Request::ReadValueBytes { id, key, .. } => formatter
                .debug_struct("Request::ReadValueBytes")
                .field("id", id)
                .field("key", key)
                .finish_non_exhaustive(),

            Request::ContainsKey { id, key, .. } => formatter
                .debug_struct("Request::ContainsKey")
                .field("id", id)
                .field("key", key)
                .finish_non_exhaustive(),

            Request::ReadMultiValuesBytes { id, keys, .. } => formatter
                .debug_struct("Request::ReadMultiValuesBytes")
                .field("id", id)
                .field("keys", keys)
                .finish_non_exhaustive(),

            Request::FindKeysByPrefix { id, key_prefix, .. } => formatter
                .debug_struct("Request::FindKeysByPrefix")
                .field("id", id)
                .field("key_prefix", key_prefix)
                .finish_non_exhaustive(),

            Request::FindKeyValuesByPrefix { id, key_prefix, .. } => formatter
                .debug_struct("Request::FindKeyValuesByPrefix")
                .field("id", id)
                .field("key_prefix", key_prefix)
                .finish_non_exhaustive(),

            Request::WriteBatch { id, batch, .. } => formatter
                .debug_struct("Request::WriteBatch")
                .field("id", id)
                .field("batch", batch)
                .finish_non_exhaustive(),
        }
    }
}
