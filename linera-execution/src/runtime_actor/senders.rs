// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper implementations to proxy requests through to [`RuntimeActor`][`super::RuntimeActor`]s.

use crate::{
    runtime_actor::{
        BaseRequest, ContractRequest, ReceiverExt, ServiceRequest, UnboundedSenderExt,
    },
    BaseRuntime, CallResult, ContractRuntime, ExecutionError, ServiceRuntime,
};
use futures::channel::mpsc;
use linera_base::{
    data_types::{Amount, Timestamp},
    identifiers::{ApplicationId, ChainId, SessionId},
};
use linera_views::batch::Batch;
use std::sync::Mutex;

/// Wrapper around a sender to a runtime actor.
pub struct RuntimeSender<Request> {
    inner: mpsc::UnboundedSender<Request>,
}

pub type ContractActorRuntime = RuntimeSender<ContractRequest>;
pub type ServiceActorRuntime = RuntimeSender<ServiceRequest>;

impl<Request> RuntimeSender<Request> {
    /// Creates a new [`ContractActorRuntime`] instance.
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

impl BaseRuntime for ContractActorRuntime {
    type Read = Mutex<Option<oneshot::Receiver<Vec<u8>>>>;
    type Lock = Mutex<Option<oneshot::Receiver<()>>>;
    type Unlock = Mutex<Option<oneshot::Receiver<()>>>;
    type ContainsKey = Mutex<Option<oneshot::Receiver<bool>>>;
    type ReadMultiValuesBytes = Mutex<Option<oneshot::Receiver<Vec<Option<Vec<u8>>>>>>;
    type ReadValueBytes = Mutex<Option<oneshot::Receiver<Option<Vec<u8>>>>>;
    type FindKeysByPrefix = Mutex<Option<oneshot::Receiver<Vec<Vec<u8>>>>>;
    type FindKeyValuesByPrefix = Mutex<Option<oneshot::Receiver<Vec<(Vec<u8>, Vec<u8>)>>>>;

    fn chain_id(&mut self) -> Result<ChainId, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ContractRequest::Base(BaseRequest::ChainId { response_sender })
            })?
            .recv_response()
    }

    fn application_id(&mut self) -> Result<ApplicationId, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ContractRequest::Base(BaseRequest::ApplicationId { response_sender })
            })?
            .recv_response()
    }

    fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ContractRequest::Base(BaseRequest::ApplicationParameters { response_sender })
            })?
            .recv_response()
    }

    fn read_system_balance(&mut self) -> Result<Amount, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ContractRequest::Base(BaseRequest::ReadSystemBalance { response_sender })
            })?
            .recv_response()
    }

    fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ContractRequest::Base(BaseRequest::ReadSystemTimestamp { response_sender })
            })?
            .recv_response()
    }

    fn try_read_my_state_new(&mut self) -> Result<Self::Read, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ContractRequest::Base(BaseRequest::TryReadMyState { response_sender })
            },
        )?)))
    }

    fn try_read_my_state_wait(&mut self, promise: &Self::Read) -> Result<Vec<u8>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    fn lock_new(&mut self) -> Result<Self::Lock, ExecutionError> {
        Ok(std::sync::Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ContractRequest::Base(BaseRequest::LockViewUserState { response_sender })
            },
        )?)))
    }

    fn lock_wait(&mut self, promise: &Self::Lock) -> Result<(), ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    fn unlock_new(&mut self) -> Result<Self::Unlock, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ContractRequest::Base(BaseRequest::UnlockViewUserState { response_sender })
            },
        )?)))
    }

    fn unlock_wait(&mut self, promise: &Self::Unlock) -> Result<(), ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    fn contains_key_new(&mut self, key: Vec<u8>) -> Result<Self::ContainsKey, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ContractRequest::Base(BaseRequest::ContainsKey {
                    key,
                    response_sender,
                })
            },
        )?)))
    }

    fn contains_key_wait(&mut self, promise: &Self::ContainsKey) -> Result<bool, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    fn read_multi_values_bytes_new(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Self::ReadMultiValuesBytes, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ContractRequest::Base(BaseRequest::ReadMultiValuesBytes {
                    keys,
                    response_sender,
                })
            },
        )?)))
    }

    fn read_multi_values_bytes_wait(
        &mut self,
        promise: &Self::ReadMultiValuesBytes,
    ) -> Result<Vec<Option<Vec<u8>>>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    fn write_batch_and_unlock(&mut self, batch: Batch) -> Result<(), ExecutionError> {
        self.inner
            .send_sync_request(|response_sender| ContractRequest::WriteBatchAndUnlock {
                batch,
                response_sender,
            })
    }

    fn read_value_bytes_new(
        &mut self,
        key: Vec<u8>,
    ) -> Result<Self::ReadValueBytes, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ContractRequest::Base(BaseRequest::ReadValueBytes {
                    key,
                    response_sender,
                })
            },
        )?)))
    }

    fn read_value_bytes_wait(
        &mut self,
        promise: &Self::ReadValueBytes,
    ) -> Result<Option<Vec<u8>>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    fn find_keys_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeysByPrefix, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ContractRequest::Base(BaseRequest::FindKeysByPrefix {
                    key_prefix,
                    response_sender,
                })
            },
        )?)))
    }

    fn find_keys_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeysByPrefix,
    ) -> Result<Vec<Vec<u8>>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    fn find_key_values_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeyValuesByPrefix, ExecutionError> {
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
    fn find_key_values_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeyValuesByPrefix,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }
}

impl ContractRuntime for ContractActorRuntime {
    fn try_read_and_lock_my_state(&mut self) -> Result<Option<Vec<u8>>, ExecutionError> {
        self.inner
            .send_sync_request(|response_sender| ContractRequest::TryReadAndLockMyState {
                response_sender,
            })
    }

    fn save_and_unlock_my_state(&mut self, state: Vec<u8>) -> Result<bool, ExecutionError> {
        self.inner
            .send_sync_request(|response_sender| ContractRequest::SaveAndUnlockMyState {
                state,
                response_sender,
            })
    }

    fn try_call_application(
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

    fn try_call_session(
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

    fn remaining_fuel(&mut self) -> Result<u64, ExecutionError> {
        self.inner
            .send_request(|response_sender| ContractRequest::RemainingFuel { response_sender })?
            .recv_response()
    }

    fn set_remaining_fuel(&mut self, remaining_fuel: u64) -> Result<(), ExecutionError> {
        self.inner
            .send_sync_request(|response_sender| ContractRequest::SetRemainingFuel {
                remaining_fuel,
                response_sender,
            })
    }
}

impl BaseRuntime for ServiceActorRuntime {
    type Read = Mutex<Option<oneshot::Receiver<Vec<u8>>>>;
    type Lock = Mutex<Option<oneshot::Receiver<()>>>;
    type Unlock = Mutex<Option<oneshot::Receiver<()>>>;
    type ContainsKey = Mutex<Option<oneshot::Receiver<bool>>>;
    type ReadMultiValuesBytes = Mutex<Option<oneshot::Receiver<Vec<Option<Vec<u8>>>>>>;
    type ReadValueBytes = Mutex<Option<oneshot::Receiver<Option<Vec<u8>>>>>;
    type FindKeysByPrefix = Mutex<Option<oneshot::Receiver<Vec<Vec<u8>>>>>;
    type FindKeyValuesByPrefix = Mutex<Option<oneshot::Receiver<Vec<(Vec<u8>, Vec<u8>)>>>>;

    fn chain_id(&mut self) -> Result<ChainId, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::ChainId { response_sender })
            })?
            .recv_response()
    }

    fn application_id(&mut self) -> Result<ApplicationId, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::ApplicationId { response_sender })
            })?
            .recv_response()
    }

    fn application_parameters(&mut self) -> Result<Vec<u8>, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::ApplicationParameters { response_sender })
            })?
            .recv_response()
    }

    fn read_system_balance(&mut self) -> Result<Amount, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::ReadSystemBalance { response_sender })
            })?
            .recv_response()
    }

    fn read_system_timestamp(&mut self) -> Result<Timestamp, ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::ReadSystemTimestamp { response_sender })
            })?
            .recv_response()
    }

    fn try_read_my_state_new(&mut self) -> Result<Self::Read, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| ServiceRequest::Base(BaseRequest::TryReadMyState { response_sender }),
        )?)))
    }

    fn try_read_my_state_wait(&mut self, promise: &Self::Read) -> Result<Vec<u8>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    #[cfg(feature = "test")]
    fn lock(&mut self) -> Result<(), ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::LockViewUserState { response_sender })
            })?
            .recv_response()
    }

    fn lock_new(&mut self) -> Result<Self::Lock, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ServiceRequest::Base(BaseRequest::LockViewUserState { response_sender })
            },
        )?)))
    }

    fn lock_wait(&mut self, promise: &Self::Lock) -> Result<(), ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    #[cfg(feature = "test")]
    fn unlock(&mut self) -> Result<(), ExecutionError> {
        self.inner
            .send_request(|response_sender| {
                ServiceRequest::Base(BaseRequest::UnlockViewUserState { response_sender })
            })?
            .recv_response()
    }

    fn unlock_new(&mut self) -> Result<Self::Unlock, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ServiceRequest::Base(BaseRequest::UnlockViewUserState { response_sender })
            },
        )?)))
    }

    fn unlock_wait(&mut self, promise: &Self::Unlock) -> Result<(), ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    fn contains_key_new(&mut self, key: Vec<u8>) -> Result<Self::ContainsKey, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ServiceRequest::Base(BaseRequest::ContainsKey {
                    key,
                    response_sender,
                })
            },
        )?)))
    }

    fn contains_key_wait(&mut self, promise: &Self::ContainsKey) -> Result<bool, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    fn read_multi_values_bytes_new(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Self::ReadMultiValuesBytes, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ServiceRequest::Base(BaseRequest::ReadMultiValuesBytes {
                    keys,
                    response_sender,
                })
            },
        )?)))
    }

    fn read_multi_values_bytes_wait(
        &mut self,
        promise: &Self::ReadMultiValuesBytes,
    ) -> Result<Vec<Option<Vec<u8>>>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    fn write_batch_and_unlock(&mut self, _batch: Batch) -> Result<(), ExecutionError> {
        Err(ExecutionError::WriteAttemptToReadOnlyStorage)
    }

    fn read_value_bytes_new(
        &mut self,
        key: Vec<u8>,
    ) -> Result<Self::ReadValueBytes, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ServiceRequest::Base(BaseRequest::ReadValueBytes {
                    key,
                    response_sender,
                })
            },
        )?)))
    }

    fn read_value_bytes_wait(
        &mut self,
        promise: &Self::ReadValueBytes,
    ) -> Result<Option<Vec<u8>>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    fn find_keys_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeysByPrefix, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            |response_sender| {
                ServiceRequest::Base(BaseRequest::FindKeysByPrefix {
                    key_prefix,
                    response_sender,
                })
            },
        )?)))
    }

    fn find_keys_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeysByPrefix,
    ) -> Result<Vec<Vec<u8>>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }

    fn find_key_values_by_prefix_new(
        &mut self,
        key_prefix: Vec<u8>,
    ) -> Result<Self::FindKeyValuesByPrefix, ExecutionError> {
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
    fn find_key_values_by_prefix_wait(
        &mut self,
        promise: &Self::FindKeyValuesByPrefix,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }
}

impl ServiceRuntime for ServiceActorRuntime {
    type TryQueryApplication = Mutex<Option<oneshot::Receiver<Vec<u8>>>>;

    fn try_query_application_new(
        &mut self,
        queried_id: ApplicationId,
        argument: Vec<u8>,
    ) -> Result<Self::TryQueryApplication, ExecutionError> {
        Ok(Mutex::new(Some(self.inner.send_request(
            move |response_sender| ServiceRequest::TryQueryApplication {
                queried_id,
                argument,
                response_sender,
            },
        )?)))
    }

    fn try_query_application_wait(
        &mut self,
        promise: &Self::TryQueryApplication,
    ) -> Result<Vec<u8>, ExecutionError> {
        let receiver = promise
            .try_lock()
            .expect("Unexpected reentrant locking of `oneshot::Receiver`")
            .take()
            .ok_or_else(|| ExecutionError::PolledTwice)?;
        receiver.recv_response()
    }
}
