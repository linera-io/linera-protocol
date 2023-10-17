// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Generates an implementation of `ContractSystemApi` for the provided `contract_system_api` type.
///
/// Generates the common code for contract system API types for all Wasm runtimes.
macro_rules! impl_contract_system_api {
    ($contract_system_api:ident, $trap:ty) => {
        impl contract_system_api::ContractSystemApi for $contract_system_api {
            type Error = ExecutionError;

            type Lock = Mutex<futures::channel::oneshot::Receiver<Result<(), ExecutionError>>>;

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn chain_id(&mut self) -> Result<contract_system_api::ChainId, Self::Error> {
                self.runtime
                    .sync_request(|response_sender| {
                        ContractRequest::Base(BaseRequest::ChainId { response_sender })
                    })
                    .map(|chain_id| chain_id.into())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn application_id(
                &mut self,
            ) -> Result<contract_system_api::ApplicationId, Self::Error> {
                self.runtime
                    .sync_request(|response_sender| {
                        ContractRequest::Base(BaseRequest::ApplicationId { response_sender })
                    })
                    .map(|application_id| application_id.into())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn application_parameters(&mut self) -> Result<Vec<u8>, Self::Error> {
                self.runtime
                    .sync_request(|response_sender| {
                        ContractRequest::Base(BaseRequest::ApplicationParameters {
                            response_sender,
                        })
                    })
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn read_system_balance(&mut self) -> Result<contract_system_api::Amount, Self::Error> {
                self.runtime
                    .sync_request(|response_sender| {
                        ContractRequest::Base(BaseRequest::ReadSystemBalance { response_sender })
                    })
                    .map(|balance| balance.into())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn read_system_timestamp(
                &mut self,
            ) -> Result<contract_system_api::Timestamp, Self::Error> {
                self.runtime
                    .sync_request(|response_sender| {
                        ContractRequest::Base(BaseRequest::ReadSystemTimestamp { response_sender })
                    })
                    .map(|timestamp| timestamp.micros())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn load(&mut self) -> Result<Vec<u8>, Self::Error> {
                self.runtime
                    .sync_request(|response_sender| {
                        ContractRequest::Base(BaseRequest::TryReadMyState { response_sender })
                    })
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn load_and_lock(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
                self.runtime
                    .sync_request(|response_sender| ContractRequest::TryReadAndLockMyState {
                        response_sender,
                    })
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn store_and_unlock(&mut self, state: &[u8]) -> Result<bool, Self::Error> {
                self.runtime
                    .sync_request(|response_sender| ContractRequest::SaveAndUnlockMyState {
                        state: state.to_owned(),
                        response_sender,
                    })
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn lock_new(&mut self) -> Result<Self::Lock, Self::Error> {
                Ok(Mutex::new(
                    self.queued_future_factory.enqueue(
                        self.runtime
                            .send_request(|response_sender| {
                                ContractRequest::Base(BaseRequest::LockViewUserState {
                                    response_sender,
                                })
                            })
                            .map_err(|_| WasmExecutionError::MissingRuntimeResponse.into()),
                    ),
                ))
            }

            fn lock_poll(
                &mut self,
                future: &Self::Lock,
            ) -> Result<contract_system_api::PollLock, Self::Error> {
                use contract_system_api::PollLock;
                let mut receiver = future
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`");
                match receiver.try_recv() {
                    Ok(None) => Ok(PollLock::Pending),
                    Ok(Some(Ok(()))) => Ok(PollLock::ReadyLocked),
                    Ok(Some(Err(ExecutionError::ViewError(ViewError::TryLockError(_))))) => {
                        Ok(PollLock::ReadyNotLocked)
                    }
                    Ok(Some(Err(error))) => Err(error),
                    Err(futures::channel::oneshot::Canceled) => {
                        Err(WasmExecutionError::MissingRuntimeResponse.into())
                    }
                }
            }

            fn try_call_application(
                &mut self,
                authenticated: bool,
                application: contract_system_api::ApplicationId,
                argument: &[u8],
                forwarded_sessions: &[Le<contract_system_api::SessionId>],
            ) -> Result<contract_system_api::CallResult, Self::Error> {
                let forwarded_sessions = forwarded_sessions
                    .iter()
                    .map(Le::get)
                    .map(SessionId::from)
                    .collect();

                self.runtime
                    .sync_request(|response_sender| ContractRequest::TryCallApplication {
                        authenticated,
                        callee_id: application.into(),
                        argument: argument.to_owned(),
                        forwarded_sessions,
                        response_sender,
                    })
                    .map(|call_result| call_result.into())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn try_call_session(
                &mut self,
                authenticated: bool,
                session: contract_system_api::SessionId,
                argument: &[u8],
                forwarded_sessions: &[Le<contract_system_api::SessionId>],
            ) -> Result<contract_system_api::CallResult, Self::Error> {
                let forwarded_sessions = forwarded_sessions
                    .iter()
                    .map(Le::get)
                    .map(SessionId::from)
                    .collect();

                self.runtime
                    .sync_request(|response_sender| ContractRequest::TryCallSession {
                        authenticated,
                        session_id: session.into(),
                        argument: argument.to_owned(),
                        forwarded_sessions,
                        response_sender,
                    })
                    .map(|call_result| call_result.into())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn log(
                &mut self,
                message: &str,
                level: contract_system_api::LogLevel,
            ) -> Result<(), Self::Error> {
                match level {
                    contract_system_api::LogLevel::Trace => tracing::trace!("{message}"),
                    contract_system_api::LogLevel::Debug => tracing::debug!("{message}"),
                    contract_system_api::LogLevel::Info => tracing::info!("{message}"),
                    contract_system_api::LogLevel::Warn => tracing::warn!("{message}"),
                    contract_system_api::LogLevel::Error => tracing::error!("{message}"),
                }
                Ok(())
            }
        }
    };
}

/// Generates an implementation of `ServiceSystemApi` for the provided `service_system_api` type.
///
/// Generates the common code for service system API types for all Wasm runtimes.
macro_rules! impl_service_system_api {
    ($service_system_api:ident, $trap:ty) => {
        impl service_system_api::ServiceSystemApi for $service_system_api {
            type Error = ExecutionError;

            type Load = Mutex<oneshot::Receiver<Vec<u8>>>;
            type Lock = Mutex<oneshot::Receiver<()>>;
            type Unlock = Mutex<oneshot::Receiver<()>>;
            type TryQueryApplication = Mutex<oneshot::Receiver<Vec<u8>>>;

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn chain_id(&mut self) -> Result<service_system_api::ChainId, Self::Error> {
                self.runtime
                    .sync_request(|response_sender| {
                        ServiceRequest::Base(BaseRequest::ChainId { response_sender })
                    })
                    .map(|chain_id| chain_id.into())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn application_id(&mut self) -> Result<service_system_api::ApplicationId, Self::Error> {
                self.runtime
                    .sync_request(|response_sender| {
                        ServiceRequest::Base(BaseRequest::ApplicationId { response_sender })
                    })
                    .map(|application_id| application_id.into())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn application_parameters(&mut self) -> Result<Vec<u8>, Self::Error> {
                self.runtime
                    .sync_request(|response_sender| {
                        ServiceRequest::Base(BaseRequest::ApplicationParameters { response_sender })
                    })
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn read_system_balance(&mut self) -> Result<service_system_api::Amount, Self::Error> {
                self.runtime
                    .sync_request(|response_sender| {
                        ServiceRequest::Base(BaseRequest::ReadSystemBalance { response_sender })
                    })
                    .map(|balance| balance.into())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn read_system_timestamp(
                &mut self,
            ) -> Result<service_system_api::Timestamp, Self::Error> {
                self.runtime
                    .sync_request(|response_sender| {
                        ServiceRequest::Base(BaseRequest::ReadSystemTimestamp { response_sender })
                    })
                    .map(|timestamp| timestamp.micros())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn load_new(&mut self) -> Result<Self::Load, Self::Error> {
                Ok(Mutex::new(self.runtime.send_request(|response_sender| {
                    ServiceRequest::Base(BaseRequest::TryReadMyState { response_sender })
                })))
            }

            fn load_poll(
                &mut self,
                future: &Self::Load,
            ) -> Result<service_system_api::PollLoad, Self::Error> {
                use service_system_api::PollLoad;
                let receiver = future
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`");
                match receiver.try_recv() {
                    Ok(bytes) => Ok(PollLoad::Ready(Ok(bytes))),
                    Err(oneshot::TryRecvError::Empty) => Ok(PollLoad::Pending),
                    Err(oneshot::TryRecvError::Disconnected) => {
                        Err(WasmExecutionError::MissingRuntimeResponse.into())
                    }
                }
            }

            fn lock_new(&mut self) -> Result<Self::Lock, Self::Error> {
                Ok(Mutex::new(self.runtime.send_request(|response_sender| {
                    ServiceRequest::Base(BaseRequest::LockViewUserState { response_sender })
                })))
            }

            fn lock_poll(
                &mut self,
                future: &Self::Lock,
            ) -> Result<service_system_api::PollLock, Self::Error> {
                use service_system_api::PollLock;
                let receiver = future
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`");
                match receiver.try_recv() {
                    Ok(()) => Ok(PollLock::Ready(Ok(()))),
                    Err(oneshot::TryRecvError::Empty) => Ok(PollLock::Pending),
                    Err(oneshot::TryRecvError::Disconnected) => {
                        Err(WasmExecutionError::MissingRuntimeResponse.into())
                    }
                }
            }

            fn unlock_new(&mut self) -> Result<Self::Unlock, Self::Error> {
                Ok(Mutex::new(self.runtime.send_request(|response_sender| {
                    ServiceRequest::Base(BaseRequest::UnlockViewUserState { response_sender })
                })))
            }

            fn unlock_poll(
                &mut self,
                future: &Self::Lock,
            ) -> Result<service_system_api::PollUnlock, Self::Error> {
                use service_system_api::PollUnlock;
                let receiver = future
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`");
                match receiver.try_recv() {
                    Ok(()) => Ok(PollUnlock::Ready(Ok(()))),
                    Err(oneshot::TryRecvError::Empty) => Ok(PollUnlock::Pending),
                    Err(oneshot::TryRecvError::Disconnected) => {
                        Err(WasmExecutionError::MissingRuntimeResponse.into())
                    }
                }
            }

            fn try_query_application_new(
                &mut self,
                application: service_system_api::ApplicationId,
                argument: &[u8],
            ) -> Result<Self::TryQueryApplication, Self::Error> {
                let argument = Vec::from(argument);

                Ok(Mutex::new(self.runtime.send_request(|response_sender| {
                    ServiceRequest::TryQueryApplication {
                        queried_id: application.into(),
                        argument: argument.to_owned(),
                        response_sender,
                    }
                })))
            }

            fn try_query_application_poll(
                &mut self,
                future: &Self::TryQueryApplication,
            ) -> Result<service_system_api::PollLoad, Self::Error> {
                use service_system_api::PollLoad;
                let receiver = future
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`");
                match receiver.try_recv() {
                    Ok(result) => Ok(PollLoad::Ready(Ok(result))),
                    Err(oneshot::TryRecvError::Empty) => Ok(PollLoad::Pending),
                    Err(oneshot::TryRecvError::Disconnected) => {
                        Err(WasmExecutionError::MissingRuntimeResponse.into())
                    }
                }
            }

            fn log(
                &mut self,
                message: &str,
                level: service_system_api::LogLevel,
            ) -> Result<(), Self::Error> {
                match level {
                    service_system_api::LogLevel::Trace => tracing::trace!("{message}"),
                    service_system_api::LogLevel::Debug => tracing::debug!("{message}"),
                    service_system_api::LogLevel::Info => tracing::info!("{message}"),
                    service_system_api::LogLevel::Warn => tracing::warn!("{message}"),
                    service_system_api::LogLevel::Error => tracing::error!("{message}"),
                }

                Ok(())
            }
        }
    };
}

/// Generates an implementation of `ViewSystem` for the provided `view_system_api` type for
/// application services.
///
/// Generates the common code for view system API types for all Wasm runtimes.
macro_rules! impl_view_system_api_for_service {
    ($view_system_api:ty, $trap:ty) => {
        impl view_system_api::ViewSystemApi for $view_system_api {
            type Error = ExecutionError;

            type ReadKeyBytes = Mutex<oneshot::Receiver<Option<Vec<u8>>>>;
            type FindKeys = Mutex<oneshot::Receiver<Vec<Vec<u8>>>>;
            type FindKeyValues = Mutex<oneshot::Receiver<Vec<(Vec<u8>, Vec<u8>)>>>;
            type WriteBatch = ();

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn read_key_bytes_new(
                &mut self,
                key: &[u8],
            ) -> Result<Self::ReadKeyBytes, Self::Error> {
                Ok(Mutex::new(self.runtime.send_request(|response_sender| {
                    ServiceRequest::Base(BaseRequest::ReadKeyBytes {
                        key: key.to_owned(),
                        response_sender,
                    })
                })))
            }

            fn read_key_bytes_poll(
                &mut self,
                future: &Self::ReadKeyBytes,
            ) -> Result<view_system_api::PollReadKeyBytes, Self::Error> {
                use view_system_api::PollReadKeyBytes;
                let receiver = future
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`");
                match receiver.try_recv() {
                    Ok(opt_list) => Ok(PollReadKeyBytes::Ready(opt_list)),
                    Err(oneshot::TryRecvError::Empty) => Ok(PollReadKeyBytes::Pending),
                    Err(oneshot::TryRecvError::Disconnected) => {
                        Err(WasmExecutionError::MissingRuntimeResponse.into())
                    }
                }
            }

            fn find_keys_new(&mut self, key_prefix: &[u8]) -> Result<Self::FindKeys, Self::Error> {
                Ok(Mutex::new(self.runtime.send_request(|response_sender| {
                    ServiceRequest::Base(BaseRequest::FindKeysByPrefix {
                        key_prefix: key_prefix.to_owned(),
                        response_sender,
                    })
                })))
            }

            fn find_keys_poll(
                &mut self,
                future: &Self::FindKeys,
            ) -> Result<view_system_api::PollFindKeys, Self::Error> {
                use view_system_api::PollFindKeys;
                let receiver = future
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`");
                match receiver.try_recv() {
                    Ok(keys) => Ok(PollFindKeys::Ready(keys)),
                    Err(oneshot::TryRecvError::Empty) => Ok(PollFindKeys::Pending),
                    Err(oneshot::TryRecvError::Disconnected) => {
                        Err(WasmExecutionError::MissingRuntimeResponse.into())
                    }
                }
            }

            fn find_key_values_new(
                &mut self,
                key_prefix: &[u8],
            ) -> Result<Self::FindKeyValues, Self::Error> {
                Ok(Mutex::new(self.runtime.send_request(|response_sender| {
                    ServiceRequest::Base(BaseRequest::FindKeyValuesByPrefix {
                        key_prefix: key_prefix.to_owned(),
                        response_sender,
                    })
                })))
            }

            fn find_key_values_poll(
                &mut self,
                future: &Self::FindKeyValues,
            ) -> Result<view_system_api::PollFindKeyValues, Self::Error> {
                use view_system_api::PollFindKeyValues;
                let receiver = future
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`");
                match receiver.try_recv() {
                    Ok(key_values) => Ok(PollFindKeyValues::Ready(key_values)),
                    Err(oneshot::TryRecvError::Empty) => Ok(PollFindKeyValues::Pending),
                    Err(oneshot::TryRecvError::Disconnected) => {
                        Err(WasmExecutionError::MissingRuntimeResponse.into())
                    }
                }
            }

            fn write_batch_new(
                &mut self,
                _list_oper: Vec<view_system_api::WriteOperation>,
            ) -> Result<Self::WriteBatch, Self::Error> {
                Err(WasmExecutionError::WriteAttemptToReadOnlyStorage.into())
            }

            fn write_batch_poll(
                &mut self,
                _future: &Self::WriteBatch,
            ) -> Result<view_system_api::PollUnit, Self::Error> {
                Err(WasmExecutionError::WriteAttemptToReadOnlyStorage.into())
            }
        }
    };
}

/// Generates an implementation of `ViewSystem` for the provided `view_system_api` type for
/// application contracts.
///
/// Generates the common code for view system API types for all WASM runtimes.
macro_rules! impl_view_system_api_for_contract {
    ($view_system_api:ty, $trap:ty) => {
        impl view_system_api::ViewSystemApi for $view_system_api {
            type Error = ExecutionError;

            type ReadKeyBytes =
                Mutex<futures::channel::oneshot::Receiver<Result<Option<Vec<u8>>, ExecutionError>>>;
            type FindKeys =
                Mutex<futures::channel::oneshot::Receiver<Result<Vec<Vec<u8>>, ExecutionError>>>;
            type FindKeyValues = Mutex<
                futures::channel::oneshot::Receiver<
                    Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>,
                >,
            >;
            type WriteBatch =
                Mutex<futures::channel::oneshot::Receiver<Result<(), ExecutionError>>>;

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn read_key_bytes_new(
                &mut self,
                key: &[u8],
            ) -> Result<Self::ReadKeyBytes, Self::Error> {
                Ok(Mutex::new(
                    self.queued_future_factory.enqueue(
                        self.runtime
                            .send_request(|response_sender| {
                                ContractRequest::Base(BaseRequest::ReadKeyBytes {
                                    key: key.to_owned(),
                                    response_sender,
                                })
                            })
                            .map_err(|_| WasmExecutionError::MissingRuntimeResponse.into()),
                    ),
                ))
            }

            fn read_key_bytes_poll(
                &mut self,
                future: &Self::ReadKeyBytes,
            ) -> Result<view_system_api::PollReadKeyBytes, Self::Error> {
                use view_system_api::PollReadKeyBytes;
                let mut receiver = future
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`");
                match receiver.try_recv() {
                    Ok(None) => Ok(PollReadKeyBytes::Pending),
                    Ok(Some(Ok(opt_list))) => Ok(PollReadKeyBytes::Ready(opt_list)),
                    Ok(Some(Err(error))) => Err(error),
                    Err(futures::channel::oneshot::Canceled) => {
                        Err(WasmExecutionError::MissingRuntimeResponse.into())
                    }
                }
            }

            fn find_keys_new(&mut self, key_prefix: &[u8]) -> Result<Self::FindKeys, Self::Error> {
                Ok(Mutex::new(
                    self.queued_future_factory.enqueue(
                        self.runtime
                            .send_request(|response_sender| {
                                ContractRequest::Base(BaseRequest::FindKeysByPrefix {
                                    key_prefix: key_prefix.to_owned(),
                                    response_sender,
                                })
                            })
                            .map_err(|_| WasmExecutionError::MissingRuntimeResponse.into()),
                    ),
                ))
            }

            fn find_keys_poll(
                &mut self,
                future: &Self::FindKeys,
            ) -> Result<view_system_api::PollFindKeys, Self::Error> {
                use view_system_api::PollFindKeys;
                let mut receiver = future
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`");
                match receiver.try_recv() {
                    Ok(None) => Ok(PollFindKeys::Pending),
                    Ok(Some(Ok(keys))) => Ok(PollFindKeys::Ready(keys)),
                    Ok(Some(Err(error))) => Err(error),
                    Err(futures::channel::oneshot::Canceled) => {
                        Err(WasmExecutionError::MissingRuntimeResponse.into())
                    }
                }
            }

            fn find_key_values_new(
                &mut self,
                key_prefix: &[u8],
            ) -> Result<Self::FindKeyValues, Self::Error> {
                Ok(Mutex::new(
                    self.queued_future_factory.enqueue(
                        self.runtime
                            .send_request(|response_sender| {
                                ContractRequest::Base(BaseRequest::FindKeyValuesByPrefix {
                                    key_prefix: key_prefix.to_owned(),
                                    response_sender,
                                })
                            })
                            .map_err(|_| WasmExecutionError::MissingRuntimeResponse.into()),
                    ),
                ))
            }

            fn find_key_values_poll(
                &mut self,
                future: &Self::FindKeyValues,
            ) -> Result<view_system_api::PollFindKeyValues, Self::Error> {
                use view_system_api::PollFindKeyValues;
                let mut receiver = future
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`");
                match receiver.try_recv() {
                    Ok(None) => Ok(PollFindKeyValues::Pending),
                    Ok(Some(Ok(key_values))) => Ok(PollFindKeyValues::Ready(key_values)),
                    Ok(Some(Err(error))) => Err(error),
                    Err(futures::channel::oneshot::Canceled) => {
                        Err(WasmExecutionError::MissingRuntimeResponse.into())
                    }
                }
            }

            fn write_batch_new(
                &mut self,
                list_oper: Vec<view_system_api::WriteOperation>,
            ) -> Result<Self::WriteBatch, Self::Error> {
                let mut batch = Batch::new();
                for x in list_oper {
                    match x {
                        view_system_api::WriteOperation::Delete(key) => {
                            batch.delete_key(key.to_vec())
                        }
                        view_system_api::WriteOperation::Deleteprefix(key_prefix) => {
                            batch.delete_key_prefix(key_prefix.to_vec())
                        }
                        view_system_api::WriteOperation::Put(key_value) => {
                            batch.put_key_value_bytes(key_value.0.to_vec(), key_value.1.to_vec())
                        }
                    }
                }
                Ok(Mutex::new(
                    self.queued_future_factory.enqueue(
                        self.runtime
                            .send_request(|response_sender| ContractRequest::WriteBatchAndUnlock {
                                batch,
                                response_sender,
                            })
                            .map_err(|_| WasmExecutionError::MissingRuntimeResponse.into()),
                    ),
                ))
            }

            fn write_batch_poll(
                &mut self,
                future: &Self::WriteBatch,
            ) -> Result<view_system_api::PollUnit, Self::Error> {
                use view_system_api::PollUnit;
                let mut receiver = future
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`");
                match receiver.try_recv() {
                    Ok(None) => Ok(PollUnit::Pending),
                    Ok(Some(Ok(()))) => Ok(PollUnit::Ready),
                    Ok(Some(Err(error))) => Err(error),
                    Err(futures::channel::oneshot::Canceled) => {
                        Err(WasmExecutionError::MissingRuntimeResponse.into())
                    }
                }
            }
        }
    };
}
