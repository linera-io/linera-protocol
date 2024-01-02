// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of how requests should be handled inside a [`RuntimeActor`].

use super::{
    requests::{BaseRequest, ContractRequest, ServiceRequest},
    RespondExt,
};
use crate::{runtime::ExecutionRuntime, ExecutionError, ExecutionRuntimeContext};
use async_lock::RwLock;
use async_trait::async_trait;
use linera_views::{common::Context, views::ViewError};

/// A type that is able to handle incoming `Request`s.
#[async_trait]
pub trait RequestHandler<Request> {
    /// Handles a `Request`.
    ///
    /// Returns an error if the request could not be handled and no further requests should be sent
    /// to this handler.
    async fn handle_request(&self, request: Request) -> Result<(), ExecutionError>;
}

#[async_trait]
impl<'a, C, const W: bool> RequestHandler<BaseRequest> for &ExecutionRuntime<'a, C, W>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    async fn handle_request(&self, request: BaseRequest) -> Result<(), ExecutionError> {
        match request {
            BaseRequest::ChainId { response_sender } => response_sender.respond(self.chain_id()),
            BaseRequest::ApplicationId { response_sender } => {
                response_sender.respond(self.application_id())
            }
            BaseRequest::ApplicationParameters { response_sender } => {
                response_sender.respond(self.application_parameters())
            }
            BaseRequest::ReadSystemBalance { response_sender } => {
                response_sender.respond(self.read_system_balance())
            }
            BaseRequest::ReadSystemTimestamp { response_sender } => {
                response_sender.respond(self.read_system_timestamp())
            }
            BaseRequest::TryReadMyState { response_sender } => {
                response_sender.respond(self.try_read_my_state().await?)
            }
            BaseRequest::LockViewUserState { response_sender } => {
                response_sender.respond(self.lock_view_user_state().await?)
            }
            BaseRequest::UnlockViewUserState { response_sender } => {
                response_sender.respond(self.unlock_view_user_state().await?)
            }
            BaseRequest::ContainsKey {
                key,
                response_sender,
            } => response_sender.respond(self.contains_key(key).await?),
            BaseRequest::ReadMultiValuesBytes {
                keys,
                response_sender,
            } => response_sender.respond(self.read_multi_values_bytes(keys).await?),
            BaseRequest::ReadValueBytes {
                key,
                response_sender,
            } => response_sender.respond(self.read_value_bytes(key).await?),
            BaseRequest::FindKeysByPrefix {
                key_prefix,
                response_sender,
            } => response_sender.respond(self.find_keys_by_prefix(key_prefix).await?),
            BaseRequest::FindKeyValuesByPrefix {
                key_prefix,
                response_sender,
            } => response_sender.respond(self.find_key_values_by_prefix(key_prefix).await?),
        }

        Ok(())
    }
}

#[async_trait]
impl<'a, C> RequestHandler<ContractRequest> for RwLock<&ExecutionRuntime<'a, C, true>>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    async fn handle_request(&self, request: ContractRequest) -> Result<(), ExecutionError> {
        // Use unit arguments in calls to `respond` in order to have compile errors if the return
        // value of the called function changes.
        #[allow(clippy::unit_arg)]
        match request {
            ContractRequest::Base(base_request) => {
                self.read().await.handle_request(base_request).await?
            }
            ContractRequest::RemainingFuel { response_sender } => {
                response_sender.respond(self.read().await.remaining_fuel().await)
            }
            ContractRequest::SetRemainingFuel {
                remaining_fuel,
                response_sender,
            } => {
                response_sender.respond(self.write().await.set_remaining_fuel(remaining_fuel).await)
            }
            ContractRequest::TryReadAndLockMyState { response_sender } => response_sender.respond(
                match self.write().await.try_read_and_lock_my_state().await {
                    Ok(bytes) => Some(bytes),
                    Err(ExecutionError::ViewError(ViewError::NotFound(_))) => None,
                    Err(error) => return Err(error),
                },
            ),
            ContractRequest::SaveAndUnlockMyState {
                state,
                response_sender,
            } => {
                response_sender.respond(self.write().await.save_and_unlock_my_state(state).is_ok())
            }
            ContractRequest::UnlockMyState { response_sender } => {
                response_sender.respond(self.write().await.unlock_my_state())
            }
            ContractRequest::WriteBatchAndUnlock {
                batch,
                response_sender,
            } => response_sender.respond(self.write().await.write_batch_and_unlock(batch).await?),
            ContractRequest::TryCallApplication {
                authenticated,
                callee_id,
                argument,
                forwarded_sessions,
                response_sender,
            } => response_sender.respond(
                self.write()
                    .await
                    .try_call_application(authenticated, callee_id, argument, forwarded_sessions)
                    .await?,
            ),
            ContractRequest::TryCallSession {
                authenticated,
                session_id,
                argument,
                forwarded_sessions,
                response_sender,
            } => response_sender.respond(
                self.write()
                    .await
                    .try_call_session(authenticated, session_id, argument, forwarded_sessions)
                    .await?,
            ),
        }

        Ok(())
    }
}

#[async_trait]
impl<'a, C> RequestHandler<ServiceRequest> for &ExecutionRuntime<'a, C, false>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    async fn handle_request(&self, request: ServiceRequest) -> Result<(), ExecutionError> {
        match request {
            ServiceRequest::Base(base_request) => self.handle_request(base_request).await?,
            ServiceRequest::TryQueryApplication {
                queried_id,
                argument,
                response_sender,
            } => response_sender.respond(self.try_query_application(queried_id, argument).await?),
        }

        Ok(())
    }
}
