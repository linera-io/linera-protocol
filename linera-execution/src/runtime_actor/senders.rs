// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    runtime_actor::{
        BaseRequest, ContractRequest, ReceiverExt, ServiceRequest, UnboundedSenderExt,
    },
    CallResult, ExecutionError,
};
use futures::channel::mpsc;
use linera_base::{
    data_types::{Amount, Timestamp},
    identifiers::{ApplicationId, ChainId, SessionId},
};
use linera_views::batch::Batch;
use std::sync::Mutex;

pub type Load = Mutex<Option<oneshot::Receiver<Vec<u8>>>>;
pub type Lock = Mutex<Option<oneshot::Receiver<()>>>;
pub type Unlock = Mutex<Option<oneshot::Receiver<()>>>;
pub type TryQueryApplication = Mutex<Option<oneshot::Receiver<Vec<u8>>>>;

pub type ReadValueBytes = Mutex<Option<oneshot::Receiver<Option<Vec<u8>>>>>;
pub type FindKeys = Mutex<Option<oneshot::Receiver<Vec<Vec<u8>>>>>;
pub type FindKeyValues = Mutex<Option<oneshot::Receiver<Vec<(Vec<u8>, Vec<u8>)>>>>;

/// Wrapper around a sender to a runtime actor.
pub struct RuntimeSender<Request> {
    inner: mpsc::UnboundedSender<Request>,
}

pub type ContractRuntimeSender = RuntimeSender<ContractRequest>;
pub type ServiceRuntimeSender = RuntimeSender<ServiceRequest>;

impl<Request> RuntimeSender<Request> {
    /// Creates a new [`ContractRuntimeSender`] instance.
    pub(crate) fn new(inner: mpsc::UnboundedSender<Request>) -> Self {
        Self { inner }
    }
}

impl<Request> Clone for RuntimeSender<Request> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl ContractRuntimeSender {
    pub fn chain_id(&mut self) -> Result<ChainId, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ContractRequest::Base(BaseRequest::ChainId { response_sender })
            })?
            .recv_response()
    }

    pub fn application_id(&mut self) -> Result<ApplicationId, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ContractRequest::Base(BaseRequest::ApplicationId { response_sender })
            })?
            .recv_response()
    }

    pub fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ContractRequest::Base(BaseRequest::ApplicationParameters { response_sender })
            })?
            .recv_response()
    }

    pub fn read_system_balance(&mut self) -> Result<Amount, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ContractRequest::Base(BaseRequest::ReadSystemBalance { response_sender })
            })?
            .recv_response()
    }

    pub fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ContractRequest::Base(BaseRequest::ReadSystemTimestamp { response_sender })
            })?
            .recv_response()
    }

    pub fn try_read_my_state(&mut self) -> Result<Vec<u8>, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ContractRequest::Base(BaseRequest::TryReadMyState { response_sender })
            })?
            .recv_response()
    }

    pub fn try_read_and_lock_my_state(&mut self) -> Result<Option<Vec<u8>>, ExecutionError> {
        self.inner
            .send_sync_request(|response_sender| ContractRequest::TryReadAndLockMyState {
                response_sender,
            })
    }

    pub fn save_and_unlock_my_state(&mut self, state: Vec<u8>) -> Result<bool, ExecutionError> {
        self.inner
            .send_sync_request(|response_sender| ContractRequest::SaveAndUnlockMyState {
                state,
                response_sender,
            })
    }

    #[cfg(feature = "test")]
    pub fn lock(&mut self) -> Result<(), ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ContractRequest::Base(BaseRequest::LockViewUserState { response_sender })
            })?
            .recv_response()
    }

    pub fn lock_new(&mut self) -> Result<Lock, ExecutionError> {
        Ok(std::sync::Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ContractRequest::Base(BaseRequest::LockViewUserState { response_sender })
            },
        )?)))
    }

    pub fn lock_wait(&mut self, promise: &Lock) -> Result<(), ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    pub fn try_call_application(
        &mut self,
        authenticated: bool,
        callee_id: ApplicationId,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, ExecutionError> {
        self.inner
            .send_sync_request(|response_sender| ContractRequest::TryCallApplication {
                authenticated,
                callee_id,
                argument,
                forwarded_sessions,
                response_sender,
            })
    }

    pub fn try_call_session(
        &mut self,
        authenticated: bool,
        session_id: SessionId,
        argument: Vec<u8>,
        forwarded_sessions: Vec<SessionId>,
    ) -> Result<CallResult, ExecutionError> {
        self.inner
            .send_sync_request(|response_sender| ContractRequest::TryCallSession {
                authenticated,
                session_id,
                argument,
                forwarded_sessions,
                response_sender,
            })
    }

    #[cfg(feature = "test")]
    pub fn read_value_bytes(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ContractRequest::Base(BaseRequest::ReadValueBytes {
                    key,
                    response_sender,
                })
            })?
            .recv_response()
    }

    pub fn read_value_bytes_new(&mut self, key: Vec<u8>) -> Result<ReadValueBytes, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ContractRequest::Base(BaseRequest::ReadValueBytes {
                    key,
                    response_sender,
                })
            },
        )?)))
    }

    pub fn read_value_bytes_wait(
        &mut self,
        promise: &ReadValueBytes,
    ) -> Result<Option<Vec<u8>>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    pub fn find_keys_new(&mut self, key_prefix: Vec<u8>) -> Result<FindKeys, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ContractRequest::Base(BaseRequest::FindKeysByPrefix {
                    key_prefix,
                    response_sender,
                })
            },
        )?)))
    }

    pub fn find_keys_wait(&mut self, promise: &FindKeys) -> Result<Vec<Vec<u8>>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    pub fn find_key_values_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<FindKeyValues, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ContractRequest::Base(BaseRequest::FindKeyValuesByPrefix {
                    key_prefix,
                    response_sender,
                })
            },
        )?)))
    }

    #[allow(clippy::type_complexity)]
    pub fn find_key_values_wait(
        &mut self,
        promise: &FindKeyValues,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    pub fn write_batch_and_unlock(&mut self, batch: Batch) -> Result<(), ExecutionError> {
        self.inner
            .send_sync_request(|response_sender| ContractRequest::WriteBatchAndUnlock {
                batch,
                response_sender,
            })
    }

    pub fn remaining_fuel(&self) -> Result<u64, ExecutionError> {
        self.inner
            .send_request(|response_sender| ContractRequest::RemainingFuel { response_sender })?
            .recv_response()
    }

    pub fn set_remaining_fuel(&self, remaining_fuel: u64) -> Result<(), ExecutionError> {
        self.inner
            .send_sync_request(|response_sender| ContractRequest::SetRemainingFuel {
                remaining_fuel,
                response_sender,
            })
    }
}

impl ServiceRuntimeSender {
    pub fn chain_id(&mut self) -> Result<ChainId, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::ChainId { response_sender })
            })?
            .recv_response()
    }

    pub fn application_id(&mut self) -> Result<ApplicationId, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::ApplicationId { response_sender })
            })?
            .recv_response()
    }

    pub fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::ApplicationParameters { response_sender })
            })?
            .recv_response()
    }

    pub fn read_system_balance(&mut self) -> Result<Amount, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::ReadSystemBalance { response_sender })
            })?
            .recv_response()
    }

    pub fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::ReadSystemTimestamp { response_sender })
            })?
            .recv_response()
    }

    pub fn try_read_my_state_new(&mut self) -> Result<Load, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| ServiceRequest::Base(BaseRequest::TryReadMyState { response_sender }),
        )?)))
    }

    pub fn try_read_my_state_wait(
        &mut self,
        promise: &Load,
    ) -> Result<Result<Vec<u8>, String>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver
            .recv_response()
            // TODO(#1153): remove
            .map(Ok)
    }

    #[cfg(feature = "test")]
    pub fn lock(&mut self) -> Result<Result<(), String>, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::LockViewUserState { response_sender })
            })?
            .recv_response()
            // TODO(#1153): remove
            .map(Ok)
    }

    pub fn lock_new(&mut self) -> Result<Lock, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ServiceRequest::Base(BaseRequest::LockViewUserState { response_sender })
            },
        )?)))
    }

    pub fn lock_wait(&mut self, promise: &Lock) -> Result<Result<(), String>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver
            .recv_response()
            // TODO(#1153): remove
            .map(Ok)
    }

    #[cfg(feature = "test")]
    pub fn unlock(&mut self) -> Result<(), ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::UnlockViewUserState { response_sender })
            })?
            .recv_response()
    }

    pub fn unlock_new(&mut self) -> Result<Unlock, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ServiceRequest::Base(BaseRequest::UnlockViewUserState { response_sender })
            },
        )?)))
    }

    pub fn unlock_wait(&mut self, promise: &Lock) -> Result<Result<(), String>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver
            .recv_response()
            // TODO(#1153): remove
            .map(Ok)
    }

    pub fn try_query_application_new(
        &mut self,
        queried_id: ApplicationId,
        argument: Vec<u8>,
    ) -> Result<TryQueryApplication, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            move |response_sender| ServiceRequest::TryQueryApplication {
                queried_id,
                argument,
                response_sender,
            },
        )?)))
    }

    pub fn try_query_application_wait(
        &mut self,
        promise: &TryQueryApplication,
    ) -> Result<Result<Vec<u8>, String>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver
            .recv_response()
            // TODO(#1153): remove
            .map(Ok)
    }

    #[cfg(feature = "test")]
    pub fn read_value_bytes(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::ReadValueBytes {
                    key,
                    response_sender,
                })
            })?
            .recv_response()
    }

    pub fn read_value_bytes_new(&mut self, key: Vec<u8>) -> Result<ReadValueBytes, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ServiceRequest::Base(BaseRequest::ReadValueBytes {
                    key,
                    response_sender,
                })
            },
        )?)))
    }

    pub fn read_value_bytes_wait(
        &mut self,
        promise: &ReadValueBytes,
    ) -> Result<Option<Vec<u8>>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    pub fn find_keys_new(&mut self, key_prefix: Vec<u8>) -> Result<FindKeys, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ServiceRequest::Base(BaseRequest::FindKeysByPrefix {
                    key_prefix,
                    response_sender,
                })
            },
        )?)))
    }

    pub fn find_keys_wait(&mut self, promise: &FindKeys) -> Result<Vec<Vec<u8>>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    pub fn find_key_values_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<FindKeyValues, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ServiceRequest::Base(BaseRequest::FindKeyValuesByPrefix {
                    key_prefix,
                    response_sender,
                })
            },
        )?)))
    }

    #[allow(clippy::type_complexity)]
    pub fn find_key_values_wait(
        &mut self,
        promise: &FindKeyValues,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    pub fn write_batch_and_unlock(&mut self, _batch: Batch) -> Result<(), ExecutionError> {
        Err(ExecutionError::WriteAttemptToReadOnlyStorage)
    }
}
