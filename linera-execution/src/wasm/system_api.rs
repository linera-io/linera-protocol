// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Generates an implementation of `ContractSystemApi` for the provided `contract_system_api` type.
///
/// Generates the common code for contract system API types for all Wasm runtimes.
macro_rules! impl_contract_system_api {
    ($trap:ty) => {
        impl<T: crate::ContractRuntime + Send + Sync + 'static>
            contract_system_api::ContractSystemApi for T
        {
            type Error = ExecutionError;

            type Lock = <Self as BaseRuntime>::Lock;

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn chain_id(&mut self) -> Result<contract_system_api::ChainId, Self::Error> {
                BaseRuntime::chain_id(self).map(|chain_id| chain_id.into())
            }

            fn application_id(
                &mut self,
            ) -> Result<contract_system_api::ApplicationId, Self::Error> {
                BaseRuntime::application_id(self).map(|application_id| application_id.into())
            }

            fn application_parameters(&mut self) -> Result<Vec<u8>, Self::Error> {
                BaseRuntime::application_parameters(self)
            }

            fn read_system_balance(&mut self) -> Result<contract_system_api::Amount, Self::Error> {
                BaseRuntime::read_system_balance(self).map(|balance| balance.into())
            }

            fn read_system_timestamp(
                &mut self,
            ) -> Result<contract_system_api::Timestamp, Self::Error> {
                BaseRuntime::read_system_timestamp(self).map(|timestamp| timestamp.micros())
            }

            // TODO(#1152): remove
            fn load(&mut self) -> Result<Vec<u8>, Self::Error> {
                self.try_read_my_state()
            }

            // TODO(#1152): remove
            fn load_and_lock(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
                self.try_read_and_lock_my_state()
            }

            // TODO(#1152): remove
            fn store_and_unlock(&mut self, state: &[u8]) -> Result<bool, Self::Error> {
                self.save_and_unlock_my_state(state.to_vec())
            }

            fn lock_new(&mut self) -> Result<Self::Lock, Self::Error> {
                BaseRuntime::lock_new(self)
            }

            fn lock_wait(&mut self, promise: &Self::Lock) -> Result<(), Self::Error> {
                BaseRuntime::lock_wait(self, promise)
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

                ContractRuntime::try_call_application(
                    self,
                    authenticated,
                    application.into(),
                    argument.to_vec(),
                    forwarded_sessions,
                )
                .map(|call_result| call_result.into())
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

                ContractRuntime::try_call_session(
                    self,
                    authenticated,
                    session.into(),
                    argument.to_vec(),
                    forwarded_sessions,
                )
                .map(|call_result| call_result.into())
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
    ($trap:ty) => {
        impl<T: crate::ServiceRuntime + Send + Sync + 'static> service_system_api::ServiceSystemApi
            for T
        {
            type Error = ExecutionError;

            type Load = <Self as BaseRuntime>::Read;
            type Lock = <Self as BaseRuntime>::Lock;
            type Unlock = <Self as BaseRuntime>::Unlock;

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn chain_id(&mut self) -> Result<service_system_api::ChainId, Self::Error> {
                BaseRuntime::chain_id(self).map(|chain_id| chain_id.into())
            }

            fn application_id(&mut self) -> Result<service_system_api::ApplicationId, Self::Error> {
                BaseRuntime::application_id(self).map(|application_id| application_id.into())
            }

            fn application_parameters(&mut self) -> Result<Vec<u8>, Self::Error> {
                BaseRuntime::application_parameters(self)
            }

            fn read_system_balance(&mut self) -> Result<service_system_api::Amount, Self::Error> {
                BaseRuntime::read_system_balance(self).map(|balance| balance.into())
            }

            fn read_system_timestamp(
                &mut self,
            ) -> Result<service_system_api::Timestamp, Self::Error> {
                BaseRuntime::read_system_timestamp(self).map(|timestamp| timestamp.micros())
            }

            // TODO(#1152): remove
            fn load_new(&mut self) -> Result<Self::Load, Self::Error> {
                self.try_read_my_state_new()
            }

            // TODO(#1152): remove
            fn load_wait(
                &mut self,
                promise: &Self::Load,
            ) -> Result<Result<Vec<u8>, String>, Self::Error> {
                self.try_read_my_state_wait(promise)
                    // TODO(#1153): remove
                    .map(Ok)
            }

            fn lock_new(&mut self) -> Result<Self::Lock, Self::Error> {
                BaseRuntime::lock_new(self)
            }

            fn lock_wait(
                &mut self,
                promise: &Self::Lock,
            ) -> Result<Result<(), String>, Self::Error> {
                BaseRuntime::lock_wait(self, promise)
                    // TODO(#1153): remove
                    .map(Ok)
            }

            fn unlock_new(&mut self) -> Result<Self::Unlock, Self::Error> {
                BaseRuntime::unlock_new(self)
            }

            fn unlock_wait(
                &mut self,
                promise: &Self::Unlock,
            ) -> Result<Result<(), String>, Self::Error> {
                BaseRuntime::unlock_wait(self, promise)
                    // TODO(#1153): remove
                    .map(Ok)
            }

            fn try_query_application(
                &mut self,
                application: service_system_api::ApplicationId,
                argument: &[u8],
            ) -> Result<Result<Vec<u8>, String>, Self::Error> {
                ServiceRuntime::try_query_application(self, application.into(), argument.to_vec())
                    // TODO(#1153): remove
                    .map(Ok)
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
/// applications.
///
/// Generates the common code for view system API types for all WASM runtimes.
macro_rules! impl_view_system_api {
    ($trap:ty) => {
        impl<T: crate::BaseRuntime + Send + Sync + 'static> view_system_api::ViewSystemApi for T {
            type Error = ExecutionError;

            type ContainsKey = <Self as BaseRuntime>::ContainsKey;
            type ReadMultiValuesBytes = <Self as BaseRuntime>::ReadMultiValuesBytes;
            type ReadValueBytes = <Self as BaseRuntime>::ReadValueBytes;
            type FindKeys = <Self as BaseRuntime>::FindKeysByPrefix;
            type FindKeyValues = <Self as BaseRuntime>::FindKeyValuesByPrefix;

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn contains_key_new(&mut self, key: &[u8]) -> Result<Self::ContainsKey, Self::Error> {
                self.contains_key_new(key.to_vec())
            }

            fn contains_key_wait(
                &mut self,
                promise: &Self::ContainsKey,
            ) -> Result<bool, Self::Error> {
                self.contains_key_wait(promise)
            }

            fn read_multi_values_bytes_new(
                &mut self,
                keys: Vec<&[u8]>,
            ) -> Result<Self::ReadMultiValuesBytes, Self::Error> {
                let keys = keys.into_iter().map(Vec::from).collect();
                self.read_multi_values_bytes_new(keys)
            }

            fn read_multi_values_bytes_wait(
                &mut self,
                promise: &Self::ReadMultiValuesBytes,
            ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
                self.read_multi_values_bytes_wait(promise)
            }

            fn read_value_bytes_new(
                &mut self,
                key: &[u8],
            ) -> Result<Self::ReadValueBytes, Self::Error> {
                self.read_value_bytes_new(key.to_vec())
            }

            fn read_value_bytes_wait(
                &mut self,
                promise: &Self::ReadValueBytes,
            ) -> Result<Option<Vec<u8>>, Self::Error> {
                self.read_value_bytes_wait(promise)
            }

            fn find_keys_new(&mut self, key_prefix: &[u8]) -> Result<Self::FindKeys, Self::Error> {
                self.find_keys_by_prefix_new(key_prefix.to_vec())
            }

            fn find_keys_wait(
                &mut self,
                promise: &Self::FindKeys,
            ) -> Result<Vec<Vec<u8>>, Self::Error> {
                self.find_keys_by_prefix_wait(promise)
            }

            fn find_key_values_new(
                &mut self,
                key_prefix: &[u8],
            ) -> Result<Self::FindKeyValues, Self::Error> {
                self.find_key_values_by_prefix_new(key_prefix.to_vec())
            }

            fn find_key_values_wait(
                &mut self,
                promise: &Self::FindKeyValues,
            ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error> {
                self.find_key_values_by_prefix_wait(promise)
            }

            // TODO(#1153): the wit name is wrong
            fn write_batch(
                &mut self,
                operations: Vec<view_system_api::WriteOperation>,
            ) -> Result<(), Self::Error> {
                let mut batch = linera_views::batch::Batch::new();
                for operation in operations {
                    match operation {
                        view_system_api::WriteOperation::Delete(key) => {
                            batch.delete_key(key.to_vec())
                        }
                        view_system_api::WriteOperation::Deleteprefix(key_prefix) => {
                            batch.delete_key_prefix(key_prefix.to_vec())
                        }
                        view_system_api::WriteOperation::Put((key, value)) => {
                            batch.put_key_value_bytes(key.to_vec(), value.to_vec())
                        }
                    }
                }
                // Hack: The following is a no-op for services.
                self.write_batch_and_unlock(batch)
            }
        }
    };
}
