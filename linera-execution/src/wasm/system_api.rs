// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Generates an implementation of `ContractSystemApi` for the provided `contract_system_api` type.
///
/// Generates the common code for contract system API types for all WASM runtimes.
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

            type Lock = HostFuture<$runtime, Result<(), ExecutionError>>;

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn chain_id(&mut self) -> Result<contract_system_api::ChainId, Self::Error> {
                Ok(self.runtime().chain_id().into())
            }

            fn application_id(&mut self) -> Result<contract_system_api::ApplicationId, Self::Error> {
                Ok(self.runtime().application_id().into())
            }

            fn application_parameters(&mut self) -> Result<Vec<u8>, Self::Error> {
                Ok(self.runtime().application_parameters())
            }

            fn read_system_balance(
                &mut self,
            ) -> Result<contract_system_api::Amount, Self::Error> {
                Ok(self.runtime().read_system_balance().into())
            }

            fn read_system_timestamp(&mut self) -> Result<contract_system_api::Timestamp, Self::Error> {
                Ok(self.runtime().read_system_timestamp().micros())
            }

            fn load(&mut self) -> Result<Vec<u8>, Self::Error> {
                Self::block_on(self.runtime().try_read_my_state())
            }

            fn load_and_lock(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
                match Self::block_on(self.runtime().try_read_and_lock_my_state()) {
                    Ok(bytes) => Ok(Some(bytes)),
                    Err(ExecutionError::ViewError(ViewError::NotFound(_))) => Ok(None),
                    Err(error) => Err(error),
                }
            }

            fn store_and_unlock(&mut self, state: &[u8]) -> Result<bool, Self::Error> {
                Ok(self
                    .runtime()
                    .save_and_unlock_my_state(state.to_owned())
                    .is_ok())
            }

            fn lock_new(&mut self) -> Result<Self::Lock, Self::Error> {
                Ok(self
                    .queued_future_factory
                    .enqueue(self.runtime().lock_view_user_state()))
            }

            fn lock_poll(
                &mut self,
                future: &Self::Lock,
            ) -> Result<contract_system_api::PollLock, Self::Error> {
                use contract_system_api::PollLock;
                match future.poll(&mut *self.waker()) {
                    Poll::Pending => Ok(PollLock::Pending),
                    Poll::Ready(Ok(())) => Ok(PollLock::ReadyLocked),
                    Poll::Ready(Err(ExecutionError::ViewError(ViewError::TryLockError(_)))) => {
                        Ok(PollLock::ReadyNotLocked)
                    }
                    Poll::Ready(Err(error)) => Err(error),
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
                let argument = Vec::from(argument);

                Self::block_on(self.runtime().try_call_application(
                    authenticated,
                    application.into(),
                    &argument,
                    forwarded_sessions,
                ))
                .map(contract_system_api::CallResult::from)
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
                let argument = Vec::from(argument);

                Self::block_on(self.runtime().try_call_session(
                    authenticated,
                    session.into(),
                    &argument,
                    forwarded_sessions,
                ))
                .map(contract_system_api::CallResult::from)
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

        impl$(<$param>)? $contract_system_api {
            /// Calls a `future` in a blocking manner.
            fn block_on<F>(future: F) -> F::Output
            where
                F: std::future::Future + Send,
                F::Output: Send,
            {
                let runtime = tokio::runtime::Handle::current();

                std::thread::scope(|scope| {
                    scope
                        .spawn(|| runtime.block_on(future))
                        .join()
                        .expect("Panic when running a future in a blocking manner")
                })
            }
        }
    };
}

/// Generates an implementation of `ServiceSystemApi` for the provided `service_system_api` type.
///
/// Generates the common code for service system API types for all WASM runtimes.
macro_rules! impl_service_system_api {
    ($service_system_api:ident<$runtime:lifetime>) => {
        impl_service_system_api!(@generate $service_system_api<$runtime>, $runtime, <$runtime>);
    };

    ($service_system_api:ident) => {
        impl_service_system_api!(@generate $service_system_api, 'static);
    };

    (@generate $service_system_api:ty, $runtime:lifetime $(, <$param:lifetime> )?) => {
        impl$(<$param>)? service_system_api::ServiceSystemApi for $service_system_api {
            type Load = HostFuture<$runtime, Result<Vec<u8>, ExecutionError>>;
            type Lock = HostFuture<$runtime, Result<(), ExecutionError>>;
            type Unlock = HostFuture<$runtime, Result<(), ExecutionError>>;
            type TryQueryApplication = HostFuture<$runtime, Result<Vec<u8>, ExecutionError>>;

            fn chain_id(&mut self) -> service_system_api::ChainId {
                self.runtime().chain_id().into()
            }

            fn application_id(&mut self) -> service_system_api::ApplicationId {
                self.runtime().application_id().into()
            }

            fn application_parameters(&mut self) -> Vec<u8> {
                self.runtime().application_parameters()
            }

            fn read_system_balance(&mut self) -> service_system_api::Amount {
                self.runtime().read_system_balance().into()
            }

            fn read_system_timestamp(&mut self) -> service_system_api::Timestamp {
                self.runtime().read_system_timestamp().micros()
            }

            fn load_new(&mut self) -> Self::Load {
                HostFuture::new(self.runtime().try_read_my_state())
            }

            fn load_poll(&mut self, future: &Self::Load) -> service_system_api::PollLoad {
                use service_system_api::PollLoad;
                match future.poll(&mut *self.waker()) {
                    Poll::Pending => PollLoad::Pending,
                    Poll::Ready(Ok(bytes)) => PollLoad::Ready(Ok(bytes)),
                    Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
                }
            }

            fn lock_new(&mut self) -> Self::Lock {
                HostFuture::new(self.runtime().lock_view_user_state())
            }

            fn lock_poll(&mut self, future: &Self::Lock) -> service_system_api::PollLock {
                use service_system_api::PollLock;
                match future.poll(&mut *self.waker()) {
                    Poll::Pending => PollLock::Pending,
                    Poll::Ready(Ok(())) => PollLock::Ready(Ok(())),
                    Poll::Ready(Err(error)) => PollLock::Ready(Err(error.to_string())),
                }
            }

            fn unlock_new(&mut self) -> Self::Unlock {
                HostFuture::new(self.runtime().unlock_view_user_state())
            }

            fn unlock_poll(&mut self, future: &Self::Lock) -> service_system_api::PollUnlock {
                use service_system_api::PollUnlock;
                match future.poll(&mut *self.waker()) {
                    Poll::Pending => PollUnlock::Pending,
                    Poll::Ready(Ok(())) => PollUnlock::Ready(Ok(())),
                    Poll::Ready(Err(error)) => PollUnlock::Ready(Err(error.to_string())),
                }
            }

            fn try_query_application_new(
                &mut self,
                application: service_system_api::ApplicationId,
                argument: &[u8],
            ) -> Self::TryQueryApplication {
                let runtime = self.runtime();
                let argument = Vec::from(argument);

                HostFuture::new(async move {
                    runtime
                        .try_query_application(application.into(), &argument)
                        .await
                })
            }

            fn try_query_application_poll(
                &mut self,
                future: &Self::TryQueryApplication,
            ) -> service_system_api::PollLoad {
                use service_system_api::PollLoad;
                match future.poll(&mut *self.waker()) {
                    Poll::Pending => PollLoad::Pending,
                    Poll::Ready(Ok(result)) => PollLoad::Ready(Ok(result)),
                    Poll::Ready(Err(error)) => PollLoad::Ready(Err(error.to_string())),
                }
            }

            fn log(&mut self, message: &str, level: service_system_api::LogLevel) {
                match level {
                    service_system_api::LogLevel::Trace => tracing::trace!("{message}"),
                    service_system_api::LogLevel::Debug => tracing::debug!("{message}"),
                    service_system_api::LogLevel::Info => tracing::info!("{message}"),
                    service_system_api::LogLevel::Warn => tracing::warn!("{message}"),
                    service_system_api::LogLevel::Error => tracing::error!("{message}"),
                }
            }
        }
    };
}

/// Generates an implementation of `ViewSystem` for the provided `view_system_api` type.
///
/// Generates the common code for view system API types for all WASM runtimes.
macro_rules! impl_view_system_api {
    ($view_system_api:ident<$runtime:lifetime>) => {
        impl_view_system_api!(
            @generate $view_system_api<$runtime>, wasmtime::Trap, $runtime, <$runtime>
        );
    };

    ($view_system_api:ty) => {
        impl_view_system_api!(@generate $view_system_api, wasmer::RuntimeError, 'static);
    };

    (@generate $view_system_api:ty, $trap:ty, $runtime:lifetime $(, <$param:lifetime> )?) => {
        impl$(<$param>)? view_system_api::ViewSystemApi for $view_system_api {
            type Error = ExecutionError;

            type ReadKeyBytes = HostFuture<$runtime, Result<Option<Vec<u8>>, ExecutionError>>;
            type FindKeys = HostFuture<$runtime, Result<Vec<Vec<u8>>, ExecutionError>>;
            type FindKeyValues =
                HostFuture<$runtime, Result<Vec<(Vec<u8>, Vec<u8>)>, ExecutionError>>;
            type WriteBatch = HostFuture<$runtime, Result<(), ExecutionError>>;

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn read_key_bytes_new(
                &mut self,
                key: &[u8],
            ) -> Result<Self::ReadKeyBytes, Self::Error> {
                Ok(self.new_host_future(self.runtime().read_key_bytes(key.to_owned())))
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
                Ok(self.new_host_future(self.runtime().find_keys_by_prefix(key_prefix.to_owned())))
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
                Ok(HostFuture::new(
                    self.runtime()
                        .find_key_values_by_prefix(key_prefix.to_owned()),
                ))
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
                Ok(self.new_host_future(
                    self.runtime_with_writable_storage()?
                        .write_batch_and_unlock(batch),
                ))
            }

            fn write_batch_poll(
                &mut self,
                future: &Self::WriteBatch,
            ) -> Result<view_system_api::PollUnit, Self::Error> {
                use view_system_api::PollUnit;
                match future.poll(&mut *self.waker()) {
                    Poll::Pending => Ok(PollUnit::Pending),
                    Poll::Ready(Ok(())) => Ok(PollUnit::Ready),
                    Poll::Ready(Err(error)) => Err(error),
                }
            }
        }
    };
}
