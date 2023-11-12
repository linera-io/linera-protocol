// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Generates an implementation of `ContractSystemApi` for the provided `contract_system_api` type.
///
/// Generates the common code for contract system API types for all Wasm runtimes.
macro_rules! impl_contract_system_api {
    ($contract_system_api:ident, $trap:ty) => {
        impl contract_system_api::ContractSystemApi for $contract_system_api {
            type Error = ExecutionError;

            type Lock = Mutex<Option<oneshot::Receiver<()>>>;

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn chain_id(&mut self) -> Result<contract_system_api::ChainId, Self::Error> {
                self.runtime
                    .send_request(|response_sender| {
                        ContractRequest::Base(BaseRequest::ChainId { response_sender })
                    })?
                    .recv()
                    .map(|chain_id| chain_id.into())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn application_id(
                &mut self,
            ) -> Result<contract_system_api::ApplicationId, Self::Error> {
                self.runtime
                    .send_request(|response_sender| {
                        ContractRequest::Base(BaseRequest::ApplicationId { response_sender })
                    })?
                    .recv()
                    .map(|application_id| application_id.into())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn application_parameters(&mut self) -> Result<Vec<u8>, Self::Error> {
                self.runtime
                    .send_request(|response_sender| {
                        ContractRequest::Base(BaseRequest::ApplicationParameters {
                            response_sender,
                        })
                    })?
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn read_system_balance(&mut self) -> Result<contract_system_api::Amount, Self::Error> {
                self.runtime
                    .send_request(|response_sender| {
                        ContractRequest::Base(BaseRequest::ReadSystemBalance { response_sender })
                    })?
                    .recv()
                    .map(|balance| balance.into())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn read_system_timestamp(
                &mut self,
            ) -> Result<contract_system_api::Timestamp, Self::Error> {
                self.runtime
                    .send_request(|response_sender| {
                        ContractRequest::Base(BaseRequest::ReadSystemTimestamp { response_sender })
                    })?
                    .recv()
                    .map(|timestamp| timestamp.micros())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn load(&mut self) -> Result<Vec<u8>, Self::Error> {
                self.runtime
                    .send_request(|response_sender| {
                        ContractRequest::Base(BaseRequest::TryReadMyState { response_sender })
                    })?
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn load_and_lock(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
                self.runtime
                    .send_request(|response_sender| ContractRequest::TryReadAndLockMyState {
                        response_sender,
                    })?
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn store_and_unlock(&mut self, state: &[u8]) -> Result<bool, Self::Error> {
                self.runtime
                    .send_request(|response_sender| ContractRequest::SaveAndUnlockMyState {
                        state: state.to_owned(),
                        response_sender,
                    })?
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn lock_new(&mut self) -> Result<Self::Lock, Self::Error> {
                Ok(Mutex::new(Some(self.runtime.send_request(
                    |response_sender| {
                        ContractRequest::Base(BaseRequest::LockViewUserState { response_sender })
                    },
                )?)))
            }

            fn lock_wait(&mut self, promise: &Self::Lock) -> Result<(), Self::Error> {
                let receiver = promise
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`")
                    .take()
                    .ok_or_else(|| WasmExecutionError::PolledTwice)?;
                receiver
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
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
                    .send_request(|response_sender| ContractRequest::TryCallApplication {
                        authenticated,
                        callee_id: application.into(),
                        argument: argument.to_owned(),
                        forwarded_sessions,
                        response_sender,
                    })?
                    .recv()
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
                    .send_request(|response_sender| ContractRequest::TryCallSession {
                        authenticated,
                        session_id: session.into(),
                        argument: argument.to_owned(),
                        forwarded_sessions,
                        response_sender,
                    })?
                    .recv()
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

            type Load = Mutex<Option<oneshot::Receiver<Vec<u8>>>>;
            type Lock = Mutex<Option<oneshot::Receiver<()>>>;
            type Unlock = Mutex<Option<oneshot::Receiver<()>>>;
            type TryQueryApplication = Mutex<Option<oneshot::Receiver<Vec<u8>>>>;

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn chain_id(&mut self) -> Result<service_system_api::ChainId, Self::Error> {
                self.runtime
                    .send_request(|response_sender| {
                        ServiceRequest::Base(BaseRequest::ChainId { response_sender })
                    })?
                    .recv()
                    .map(|chain_id| chain_id.into())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn application_id(&mut self) -> Result<service_system_api::ApplicationId, Self::Error> {
                self.runtime
                    .send_request(|response_sender| {
                        ServiceRequest::Base(BaseRequest::ApplicationId { response_sender })
                    })?
                    .recv()
                    .map(|application_id| application_id.into())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn application_parameters(&mut self) -> Result<Vec<u8>, Self::Error> {
                self.runtime
                    .send_request(|response_sender| {
                        ServiceRequest::Base(BaseRequest::ApplicationParameters { response_sender })
                    })?
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn read_system_balance(&mut self) -> Result<service_system_api::Amount, Self::Error> {
                self.runtime
                    .send_request(|response_sender| {
                        ServiceRequest::Base(BaseRequest::ReadSystemBalance { response_sender })
                    })?
                    .recv()
                    .map(|balance| balance.into())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn read_system_timestamp(
                &mut self,
            ) -> Result<service_system_api::Timestamp, Self::Error> {
                self.runtime
                    .send_request(|response_sender| {
                        ServiceRequest::Base(BaseRequest::ReadSystemTimestamp { response_sender })
                    })?
                    .recv()
                    .map(|timestamp| timestamp.micros())
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn load_new(&mut self) -> Result<Self::Load, Self::Error> {
                Ok(Mutex::new(Some(self.runtime.send_request(
                    |response_sender| {
                        ServiceRequest::Base(BaseRequest::TryReadMyState { response_sender })
                    },
                )?)))
            }

            fn load_wait(
                &mut self,
                promise: &Self::Load,
            ) -> Result<Result<Vec<u8>, String>, Self::Error> {
                let receiver = promise
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`")
                    .take()
                    .ok_or_else(|| WasmExecutionError::PolledTwice)?;
                receiver
                    .recv()
                    .map(Ok)
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn lock_new(&mut self) -> Result<Self::Lock, Self::Error> {
                Ok(Mutex::new(Some(self.runtime.send_request(
                    |response_sender| {
                        ServiceRequest::Base(BaseRequest::LockViewUserState { response_sender })
                    },
                )?)))
            }

            fn lock_wait(
                &mut self,
                promise: &Self::Lock,
            ) -> Result<Result<(), String>, Self::Error> {
                let receiver = promise
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`")
                    .take()
                    .ok_or_else(|| WasmExecutionError::PolledTwice)?;
                receiver
                    .recv()
                    .map(Ok)
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn unlock_new(&mut self) -> Result<Self::Unlock, Self::Error> {
                Ok(Mutex::new(Some(self.runtime.send_request(
                    |response_sender| {
                        ServiceRequest::Base(BaseRequest::UnlockViewUserState { response_sender })
                    },
                )?)))
            }

            fn unlock_wait(
                &mut self,
                promise: &Self::Lock,
            ) -> Result<Result<(), String>, Self::Error> {
                let receiver = promise
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`")
                    .take()
                    .ok_or_else(|| WasmExecutionError::PolledTwice)?;
                receiver
                    .recv()
                    .map(Ok)
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn try_query_application_new(
                &mut self,
                application: service_system_api::ApplicationId,
                argument: &[u8],
            ) -> Result<Self::TryQueryApplication, Self::Error> {
                let argument = Vec::from(argument);

                Ok(Mutex::new(Some(self.runtime.send_request(
                    |response_sender| ServiceRequest::TryQueryApplication {
                        queried_id: application.into(),
                        argument: argument.to_owned(),
                        response_sender,
                    },
                )?)))
            }

            fn try_query_application_wait(
                &mut self,
                promise: &Self::TryQueryApplication,
            ) -> Result<Result<Vec<u8>, String>, Self::Error> {
                let receiver = promise
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`")
                    .take()
                    .ok_or_else(|| WasmExecutionError::PolledTwice)?;
                receiver
                    .recv()
                    .map(Ok)
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
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

            type ReadKeyBytes = Mutex<Option<oneshot::Receiver<Option<Vec<u8>>>>>;
            type FindKeys = Mutex<Option<oneshot::Receiver<Vec<Vec<u8>>>>>;
            type FindKeyValues = Mutex<Option<oneshot::Receiver<Vec<(Vec<u8>, Vec<u8>)>>>>;

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn read_key_bytes_new(
                &mut self,
                key: &[u8],
            ) -> Result<Self::ReadKeyBytes, Self::Error> {
                Ok(Mutex::new(Some(self.runtime.send_request(
                    |response_sender| {
                        ServiceRequest::Base(BaseRequest::ReadKeyBytes {
                            key: key.to_owned(),
                            response_sender,
                        })
                    },
                )?)))
            }

            fn read_key_bytes_wait(
                &mut self,
                promise: &Self::ReadKeyBytes,
            ) -> Result<Option<Vec<u8>>, Self::Error> {
                let receiver = promise
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`")
                    .take()
                    .ok_or_else(|| WasmExecutionError::PolledTwice)?;
                receiver
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn find_keys_new(&mut self, key_prefix: &[u8]) -> Result<Self::FindKeys, Self::Error> {
                Ok(Mutex::new(Some(self.runtime.send_request(
                    |response_sender| {
                        ServiceRequest::Base(BaseRequest::FindKeysByPrefix {
                            key_prefix: key_prefix.to_owned(),
                            response_sender,
                        })
                    },
                )?)))
            }

            fn find_keys_wait(
                &mut self,
                promise: &Self::FindKeys,
            ) -> Result<Vec<Vec<u8>>, Self::Error> {
                let receiver = promise
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`")
                    .take()
                    .ok_or_else(|| WasmExecutionError::PolledTwice)?;
                receiver
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn find_key_values_new(
                &mut self,
                key_prefix: &[u8],
            ) -> Result<Self::FindKeyValues, Self::Error> {
                Ok(Mutex::new(Some(self.runtime.send_request(
                    |response_sender| {
                        ServiceRequest::Base(BaseRequest::FindKeyValuesByPrefix {
                            key_prefix: key_prefix.to_owned(),
                            response_sender,
                        })
                    },
                )?)))
            }

            fn find_key_values_wait(
                &mut self,
                promise: &Self::FindKeyValues,
            ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
                let receiver = promise
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`")
                    .take()
                    .ok_or_else(|| WasmExecutionError::PolledTwice)?;
                receiver
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn write_batch(
                &mut self,
                _list_oper: Vec<view_system_api::WriteOperation>,
            ) -> Result<(), Self::Error> {
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

            type ReadKeyBytes = Mutex<Option<oneshot::Receiver<Option<Vec<u8>>>>>;
            type FindKeys = Mutex<Option<oneshot::Receiver<Vec<Vec<u8>>>>>;
            type FindKeyValues = Mutex<Option<oneshot::Receiver<Vec<(Vec<u8>, Vec<u8>)>>>>;

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn read_key_bytes_new(
                &mut self,
                key: &[u8],
            ) -> Result<Self::ReadKeyBytes, Self::Error> {
                Ok(Mutex::new(Some(self.runtime.send_request(
                    |response_sender| {
                        ContractRequest::Base(BaseRequest::ReadKeyBytes {
                            key: key.to_owned(),
                            response_sender,
                        })
                    },
                )?)))
            }

            fn read_key_bytes_wait(
                &mut self,
                promise: &Self::ReadKeyBytes,
            ) -> Result<Option<Vec<u8>>, Self::Error> {
                let receiver = promise
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`")
                    .take()
                    .ok_or_else(|| WasmExecutionError::PolledTwice)?;
                receiver
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn find_keys_new(&mut self, key_prefix: &[u8]) -> Result<Self::FindKeys, Self::Error> {
                Ok(Mutex::new(Some(self.runtime.send_request(
                    |response_sender| {
                        ContractRequest::Base(BaseRequest::FindKeysByPrefix {
                            key_prefix: key_prefix.to_owned(),
                            response_sender,
                        })
                    },
                )?)))
            }

            fn find_keys_wait(
                &mut self,
                promise: &Self::FindKeys,
            ) -> Result<Vec<Vec<u8>>, Self::Error> {
                let receiver = promise
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`")
                    .take()
                    .ok_or_else(|| WasmExecutionError::PolledTwice)?;
                receiver
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn find_key_values_new(
                &mut self,
                key_prefix: &[u8],
            ) -> Result<Self::FindKeyValues, Self::Error> {
                Ok(Mutex::new(Some(self.runtime.send_request(
                    |response_sender| {
                        ContractRequest::Base(BaseRequest::FindKeyValuesByPrefix {
                            key_prefix: key_prefix.to_owned(),
                            response_sender,
                        })
                    },
                )?)))
            }

            fn find_key_values_wait(
                &mut self,
                promise: &Self::FindKeyValues,
            ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
                let receiver = promise
                    .try_lock()
                    .expect("Unexpected reentrant locking of `oneshot::Receiver`")
                    .take()
                    .ok_or_else(|| WasmExecutionError::PolledTwice)?;
                receiver
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }

            fn write_batch(
                &mut self,
                list_oper: Vec<view_system_api::WriteOperation>,
            ) -> Result<(), Self::Error> {
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
                self.runtime
                    .send_request(|response_sender| ContractRequest::WriteBatchAndUnlock {
                        batch,
                        response_sender,
                    })?
                    .recv()
                    .map_err(|oneshot::RecvError| WasmExecutionError::MissingRuntimeResponse.into())
            }
        }
    };
}
