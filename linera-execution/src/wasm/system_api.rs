// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Generates an implementation of `ContractSystemApi` for the provided `contract_system_api` type.
///
/// Generates the common code for contract system API types for all Wasm runtimes.
macro_rules! impl_contract_system_api {
    ($contract_system_api:ident<$runtime:lifetime>) => {
        impl_contract_system_api!(
            @generate $contract_system_api<$runtime>, wasmtime::Trap, $runtime, <$runtime>
        );
    };

    ($contract_system_api:ident) => {
        impl_contract_system_api!(@generate $contract_system_api, wasmer::RuntimeError, 'static);
    };

    (@generate $contract_system_api:ty, $trap:ty, $runtime:lifetime $(, <$param:lifetime> )?) => {
        impl$(<$param>)? contract_system_api::ContractSystemApi for $contract_system_api {
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
                        ContractRequest::Base(BaseRequest::ApplicationParameters { response_sender })
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
                        ContractRequest::Base(BaseRequest::TryReadMyState {
                            response_sender,
                        })
                    })
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn load_and_lock(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
                self.runtime
                    .sync_request(|response_sender| ContractRequest::TryReadAndLockMyState { response_sender })
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
                                ContractRequest::Base(BaseRequest::LockViewUserState { response_sender })
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
                match self.waker().with_context(|context| receiver.poll_unpin(context)) {
                    Poll::Pending => Ok(PollLock::Pending),
                    Poll::Ready(Ok(Ok(()))) => Ok(PollLock::ReadyLocked),
                    Poll::Ready(Ok(Err(ExecutionError::ViewError(ViewError::TryLockError(_))))) => {
                        Ok(PollLock::ReadyNotLocked)
                    }
                    Poll::Ready(Ok(Err(error))) => Err(error),
                    Poll::Ready(Err(_)) => panic!(
                        "`HostFutureQueue` dropped while guest Wasm instance is still executing",
                    ),
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
    ($service_system_api:ident<$runtime:lifetime>) => {
        impl_service_system_api!(
            @generate $service_system_api<$runtime>, wasmtime::Trap, $runtime, <$runtime>
        );
    };

    ($service_system_api:ident) => {
        impl_service_system_api!(@generate $service_system_api, wasmer::RuntimeError, 'static);
    };

    (@generate $service_system_api:ty, $trap:ty, $runtime:lifetime $(, <$param:lifetime> )?) => {
        impl$(<$param>)? service_system_api::ServiceSystemApi for $service_system_api {
            type Error = ExecutionError;

            type Load = HostFuture<$runtime, Result<Vec<u8>, ExecutionError>>;
            type Lock = HostFuture<$runtime, Result<(), ExecutionError>>;
            type Unlock = HostFuture<$runtime, Result<(), ExecutionError>>;
            type TryQueryApplication = HostFuture<$runtime, Result<Vec<u8>, ExecutionError>>;

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn chain_id(&mut self) -> Result<service_system_api::ChainId, Self::Error> {
                Ok(self.runtime().chain_id().into())
            }

            fn application_id(&mut self) -> Result<service_system_api::ApplicationId, Self::Error> {
                Ok(self.runtime().application_id().into())
            }

            fn application_parameters(&mut self) -> Result<Vec<u8>, Self::Error> {
                Ok(self.runtime().application_parameters())
            }

            fn read_system_balance(&mut self) -> Result<service_system_api::Amount, Self::Error> {
                Ok(self.runtime().read_system_balance().into())
            }

            fn read_system_timestamp(
                &mut self,
            ) -> Result<service_system_api::Timestamp, Self::Error> {
                Ok(self.runtime().read_system_timestamp().micros())
            }

            fn load_new(&mut self) -> Result<Self::Load, Self::Error> {
                Ok(HostFuture::new(self.runtime().try_read_my_state()))
            }

            fn load_poll(
                &mut self,
                future: &Self::Load,
            ) -> Result<service_system_api::PollLoad, Self::Error> {
                use service_system_api::PollLoad;
                Ok(match future.poll(&mut *self.waker()) {
                    Poll::Pending => PollLoad::Pending,
                    Poll::Ready(Ok(bytes)) => PollLoad::Ready(Ok(bytes)),
                    Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
                })
            }

            fn lock_new(&mut self) -> Result<Self::Lock, Self::Error> {
                Ok(HostFuture::new(self.runtime().lock_view_user_state()))
            }

            fn lock_poll(
                &mut self,
                future: &Self::Lock,
            ) -> Result<service_system_api::PollLock, Self::Error> {
                use service_system_api::PollLock;
                Ok(match future.poll(&mut *self.waker()) {
                    Poll::Pending => PollLock::Pending,
                    Poll::Ready(Ok(())) => PollLock::Ready(Ok(())),
                    Poll::Ready(Err(error)) => PollLock::Ready(Err(error.to_string())),
                })
            }

            fn unlock_new(&mut self) -> Result<Self::Unlock, Self::Error> {
                Ok(HostFuture::new(self.runtime().unlock_view_user_state()))
            }

            fn unlock_poll(
                &mut self,
                future: &Self::Lock,
            ) -> Result<service_system_api::PollUnlock, Self::Error> {
                use service_system_api::PollUnlock;
                Ok(match future.poll(&mut *self.waker()) {
                    Poll::Pending => PollUnlock::Pending,
                    Poll::Ready(Ok(())) => PollUnlock::Ready(Ok(())),
                    Poll::Ready(Err(error)) => PollUnlock::Ready(Err(error.to_string())),
                })
            }

            fn try_query_application_new(
                &mut self,
                application: service_system_api::ApplicationId,
                argument: &[u8],
            ) -> Result<Self::TryQueryApplication, Self::Error> {
                let runtime = self.runtime();
                let argument = Vec::from(argument);

                Ok(HostFuture::new(async move {
                    runtime
                        .try_query_application(application.into(), &argument)
                        .await
                }))
            }

            fn try_query_application_poll(
                &mut self,
                future: &Self::TryQueryApplication,
            ) -> Result<service_system_api::PollLoad, Self::Error> {
                use service_system_api::PollLoad;
                Ok(match future.poll(&mut *self.waker()) {
                    Poll::Pending => PollLoad::Pending,
                    Poll::Ready(Ok(result)) => PollLoad::Ready(Ok(result)),
                    Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
                })
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
    ($view_system_api:ident<$runtime:lifetime>) => {
        impl_view_system_api_for_service!(
            @generate $view_system_api<$runtime>, wasmtime::Trap, $runtime, <$runtime>
        );
    };

    ($view_system_api:ty) => {
        impl_view_system_api_for_service!(@generate $view_system_api, wasmer::RuntimeError, 'static);
    };

    (@generate $view_system_api:ty, $trap:ty, $runtime:lifetime $(, <$param:lifetime> )?) => {
        impl$(<$param>)? view_system_api::ViewSystemApi for $view_system_api {
            type Error = ExecutionError;

            type ReadKeyBytes = HostFuture<$runtime, Result<Option<Vec<u8>>, ExecutionError>>;
            type FindKeys = HostFuture<$runtime, Result<Vec<Vec<u8>>, ExecutionError>>;
            type FindKeyValues =
                HostFuture<$runtime, Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>>;
            type WriteBatch = ();

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn read_key_bytes_new(
                &mut self,
                key: &[u8],
            ) -> Result<Self::ReadKeyBytes, Self::Error> {
                Ok(HostFuture::new(self.runtime().read_key_bytes(key.to_owned())))
            }

            fn read_key_bytes_poll(
                &mut self,
                future: &Self::ReadKeyBytes,
            ) -> Result<view_system_api::PollReadKeyBytes, Self::Error> {
                use view_system_api::PollReadKeyBytes;
                match future.poll(&mut *self.waker()) {
                    Poll::Pending => Ok(PollReadKeyBytes::Pending),
                    Poll::Ready(Ok(opt_list)) => Ok(PollReadKeyBytes::Ready(opt_list)),
                    Poll::Ready(Err(error)) => Err(error),
                }
            }

            fn find_keys_new(&mut self, key_prefix: &[u8]) -> Result<Self::FindKeys, Self::Error> {
                Ok(HostFuture::new(self.runtime().find_keys_by_prefix(key_prefix.to_owned())))
            }

            fn find_keys_poll(
                &mut self,
                future: &Self::FindKeys,
            ) -> Result<view_system_api::PollFindKeys, Self::Error> {
                use view_system_api::PollFindKeys;
                match future.poll(&mut *self.waker()) {
                    Poll::Pending => Ok(PollFindKeys::Pending),
                    Poll::Ready(Ok(keys)) => Ok(PollFindKeys::Ready(keys)),
                    Poll::Ready(Err(error)) => Err(error),
                }
            }

            fn find_key_values_new(
                &mut self,
                key_prefix: &[u8],
            ) -> Result<Self::FindKeyValues, Self::Error> {
                Ok(HostFuture::new(self.runtime().find_key_values_by_prefix(key_prefix.to_owned())))
            }

            fn find_key_values_poll(
                &mut self,
                future: &Self::FindKeyValues,
            ) -> Result<view_system_api::PollFindKeyValues, Self::Error> {
                use view_system_api::PollFindKeyValues;
                match future.poll(&mut *self.waker()) {
                    Poll::Pending => Ok(PollFindKeyValues::Pending),
                    Poll::Ready(Ok(key_values)) => Ok(PollFindKeyValues::Ready(key_values)),
                    Poll::Ready(Err(error)) => Err(error),
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
    ($view_system_api:ident<$runtime:lifetime>) => {
        impl_view_system_api_for_contract!(
            @generate $view_system_api<$runtime>, wasmtime::Trap, <$runtime>
        );
    };

    ($view_system_api:ty) => {
        impl_view_system_api_for_contract!(
            @generate $view_system_api, wasmer::RuntimeError
        );
    };

    (@generate $view_system_api:ty, $trap:ty $(, <$param:lifetime> )?) => {
        impl$(<$param>)? view_system_api::ViewSystemApi for $view_system_api {
            type Error = ExecutionError;

            type ReadKeyBytes =
                Mutex<futures::channel::oneshot::Receiver<Result<Option<Vec<u8>>, ExecutionError>>>;
            type FindKeys =
                Mutex<futures::channel::oneshot::Receiver<Result<Vec<Vec<u8>>, ExecutionError>>>;
            type FindKeyValues = Mutex<
                futures::channel::oneshot::Receiver<Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>>
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
                        self.runtime.send_request(|response_sender| {
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
                match self.waker().with_context(|context| receiver.poll_unpin(context)) {
                    Poll::Pending => Ok(PollReadKeyBytes::Pending),
                    Poll::Ready(Ok(Ok(opt_list))) => Ok(PollReadKeyBytes::Ready(opt_list)),
                    Poll::Ready(Ok(Err(error))) => Err(error),
                    Poll::Ready(Err(_)) => panic!(
                        "`HostFutureQueue` dropped while guest Wasm instance is still executing",
                    ),
                }
            }

            fn find_keys_new(&mut self, key_prefix: &[u8]) -> Result<Self::FindKeys, Self::Error> {
                Ok(Mutex::new(
                    self.queued_future_factory.enqueue(
                        self.runtime.send_request(|response_sender| {
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
                match self.waker().with_context(|context| receiver.poll_unpin(context)) {
                    Poll::Pending => Ok(PollFindKeys::Pending),
                    Poll::Ready(Ok(Ok(keys))) => Ok(PollFindKeys::Ready(keys)),
                    Poll::Ready(Ok(Err(error))) => Err(error),
                    Poll::Ready(Err(_)) => panic!(
                        "`HostFutureQueue` dropped while guest Wasm instance is still executing",
                    ),
                }
            }

            fn find_key_values_new(
                &mut self,
                key_prefix: &[u8],
            ) -> Result<Self::FindKeyValues, Self::Error> {
                Ok(Mutex::new(
                    self.queued_future_factory.enqueue(
                        self.runtime.send_request(|response_sender| {
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
                match self.waker().with_context(|context| receiver.poll_unpin(context)) {
                    Poll::Pending => Ok(PollFindKeyValues::Pending),
                    Poll::Ready(Ok(Ok(key_values))) => Ok(PollFindKeyValues::Ready(key_values)),
                    Poll::Ready(Ok(Err(error))) => Err(error),
                    Poll::Ready(Err(_)) => panic!(
                        "`HostFutureQueue` dropped while guest Wasm instance is still executing",
                    ),
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
                        self.runtime.send_request(|response_sender| {
                            ContractRequest::WriteBatchAndUnlock { batch, response_sender }
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
                match self.waker().with_context(|context| receiver.poll_unpin(context)) {
                    Poll::Pending => Ok(PollUnit::Pending),
                    Poll::Ready(Ok(Ok(()))) => Ok(PollUnit::Ready),
                    Poll::Ready(Ok(Err(error))) => Err(error),
                    Poll::Ready(Err(_)) => panic!(
                        "`HostFutureQueue` dropped while guest Wasm instance is still executing",
                    ),
                }
            }
        }
    };
}
