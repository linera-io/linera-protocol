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

            fn error_to_trap(&mut self, error: Self::Error) -> $trap {
                error.into()
            }

            fn chain_id(&mut self) -> Result<contract_system_api::ChainId, Self::Error> {
                BaseRuntime::chain_id(self).map(|chain_id| chain_id.into())
            }

            fn block_height(&mut self) -> Result<contract_system_api::BlockHeight, Self::Error> {
                BaseRuntime::block_height(self).map(|height| height.into())
            }

            fn application_id(
                &mut self,
            ) -> Result<contract_system_api::ApplicationId, Self::Error> {
                BaseRuntime::application_id(self).map(|application_id| application_id.into())
            }

            fn application_parameters(&mut self) -> Result<Vec<u8>, Self::Error> {
                BaseRuntime::application_parameters(self)
            }

            fn authenticated_signer(
                &mut self,
            ) -> Result<Option<contract_system_api::Owner>, Self::Error> {
                let maybe_owner = ContractRuntime::authenticated_signer(self)?;
                Ok(maybe_owner.map(|owner| owner.into()))
            }

            fn read_system_timestamp(
                &mut self,
            ) -> Result<contract_system_api::Timestamp, Self::Error> {
                BaseRuntime::read_system_timestamp(self).map(|timestamp| timestamp.micros())
            }

            fn message_id(
                &mut self,
            ) -> Result<Option<contract_system_api::MessageId>, Self::Error> {
                let maybe_message_id = ContractRuntime::message_id(self)?;
                Ok(maybe_message_id.map(|message_id| message_id.into()))
            }

            fn message_is_bouncing(&mut self) -> Result<Option<bool>, Self::Error> {
                ContractRuntime::message_is_bouncing(self)
            }

            fn authenticated_caller_id(
                &mut self,
            ) -> Result<Option<contract_system_api::ApplicationId>, Self::Error> {
                let maybe_caller_id = ContractRuntime::authenticated_caller_id(self)?;
                Ok(maybe_caller_id.map(|caller_id| caller_id.into()))
            }

            fn read_chain_balance(&mut self) -> Result<contract_system_api::Amount, Self::Error> {
                BaseRuntime::read_chain_balance(self).map(|balance| balance.into())
            }

            fn read_owner_balance(
                &mut self,
                owner: contract_system_api::Owner,
            ) -> Result<contract_system_api::Amount, Self::Error> {
                BaseRuntime::read_owner_balance(self, owner.into()).map(|balance| balance.into())
            }

            fn transfer(
                &mut self,
                source: Option<contract_system_api::Owner>,
                destination: contract_system_api::Account,
                amount: contract_system_api::Amount,
            ) -> Result<(), Self::Error> {
                ContractRuntime::transfer(
                    self,
                    source.map(|source| source.into()),
                    destination.into(),
                    amount.into(),
                )
            }

            fn claim(
                &mut self,
                source: contract_system_api::Account,
                destination: contract_system_api::Account,
                amount: contract_system_api::Amount,
            ) -> Result<(), Self::Error> {
                ContractRuntime::claim(self, source.into(), destination.into(), amount.into())
            }

            fn chain_ownership(
                &mut self,
            ) -> Result<contract_system_api::ChainOwnershipResult, Self::Error> {
                BaseRuntime::chain_ownership(self).map(Into::into)
            }

            fn open_chain(
                &mut self,
                chain_ownership: contract_system_api::ChainOwnershipParam,
                balance: contract_system_api::Amount,
            ) -> Result<contract_system_api::ChainId, Self::Error> {
                ContractRuntime::open_chain(self, chain_ownership.into(), balance.into())
                    .map(Into::into)
            }

            fn close_chain(&mut self) -> Result<(), Self::Error> {
                ContractRuntime::close_chain(self)
            }

            fn error_to_closechainerror(
                &mut self,
                error: Self::Error,
            ) -> Result<contract_system_api::Closechainerror, $trap> {
                match error {
                    ExecutionError::UnauthorizedApplication(_) => {
                        Ok(contract_system_api::Closechainerror::NotPermitted)
                    }
                    error => Err(error.into()),
                }
            }

            fn try_call_application(
                &mut self,
                authenticated: bool,
                application: contract_system_api::ApplicationId,
                argument: &[u8],
                forwarded_sessions: &[Le<contract_system_api::SessionId>],
            ) -> Result<contract_system_api::CallOutcome, Self::Error> {
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
                .map(|call_outcome| call_outcome.into())
            }

            fn try_call_session(
                &mut self,
                authenticated: bool,
                session: contract_system_api::SessionId,
                argument: &[u8],
                forwarded_sessions: &[Le<contract_system_api::SessionId>],
            ) -> Result<contract_system_api::CallOutcome, Self::Error> {
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
                .map(|call_outcome| call_outcome.into())
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

            fn read_chain_balance(&mut self) -> Result<service_system_api::Amount, Self::Error> {
                BaseRuntime::read_chain_balance(self).map(|balance| balance.into())
            }

            fn read_owner_balance(
                &mut self,
                owner: service_system_api::Owner,
            ) -> Result<service_system_api::Amount, Self::Error> {
                BaseRuntime::read_owner_balance(self, owner.into()).map(|balance| balance.into())
            }

            fn read_system_timestamp(
                &mut self,
            ) -> Result<service_system_api::Timestamp, Self::Error> {
                BaseRuntime::read_system_timestamp(self).map(|timestamp| timestamp.micros())
            }

            fn try_query_application(
                &mut self,
                application: service_system_api::ApplicationId,
                argument: &[u8],
            ) -> Result<Vec<u8>, Self::Error> {
                ServiceRuntime::try_query_application(self, application.into(), argument.to_vec())
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
                self.write_batch(batch)
            }
        }
    };
}
