// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{any::Any, collections::HashMap, marker::PhantomData};

use linera_base::{
    data_types::{Amount, BlockHeight, SendMessageRequest, Timestamp},
    identifiers::{Account, ApplicationId, ChainId, ChannelName, MessageId, Owner},
    ownership::{ChainOwnership, CloseChainError},
};
use linera_views::batch::{Batch, WriteOperation};
use linera_witty::{wit_export, Instance, RuntimeError};
use tracing::log;

use super::WasmExecutionError;
use crate::{BaseRuntime, ContractRuntime, ExecutionError, ServiceRuntime};

/// Common host data used as the `UserData` of the system API implementations.
pub struct SystemApiData<Runtime> {
    runtime: Runtime,
    active_promises: HashMap<u32, Box<dyn Any + Send + Sync>>,
    promise_counter: u32,
}

#[allow(dead_code)]
impl<Runtime> SystemApiData<Runtime> {
    /// Creates a new [`SystemApiData`] using the provided `runtime` to execute the system APIs.
    pub fn new(runtime: Runtime) -> Self {
        SystemApiData {
            runtime,
            active_promises: HashMap::new(),
            promise_counter: 0,
        }
    }

    /// Returns a mutable reference the system API `Runtime`.
    pub fn runtime_mut(&mut self) -> &mut Runtime {
        &mut self.runtime
    }

    /// Registers a `promise` internally, returning an ID that is unique for the lifetime of this
    /// [`SystemApiData`].
    fn register_promise<Promise>(&mut self, promise: Promise) -> Result<u32, RuntimeError>
    where
        Promise: Send + Sync + 'static,
    {
        let id = self.promise_counter;

        self.active_promises.insert(id, Box::new(promise));
        self.promise_counter += 1;

        Ok(id)
    }

    /// Returns a `Promise` registered to the provided `promise_id`.
    fn take_promise<Promise>(&mut self, promise_id: u32) -> Result<Promise, RuntimeError>
    where
        Promise: Send + Sync + 'static,
    {
        let type_erased_promise = self
            .active_promises
            .remove(&promise_id)
            .ok_or_else(|| RuntimeError::Custom(WasmExecutionError::UnknownPromise.into()))?;

        type_erased_promise
            .downcast()
            .map(|boxed_promise| *boxed_promise)
            .map_err(|_| RuntimeError::Custom(WasmExecutionError::IncorrectPromise.into()))
    }
}

/// An implementation of the system API made available to contracts.
#[derive(Default)]
pub struct ContractSystemApi<Caller>(PhantomData<Caller>);

#[wit_export(package = "linera:app")]
impl<Caller, Runtime> ContractSystemApi<Caller>
where
    Caller: Instance<UserData = SystemApiData<Runtime>>,
    Runtime: ContractRuntime + Send + 'static,
{
    /// Returns the ID of the current chain.
    fn get_chain_id(caller: &mut Caller) -> Result<ChainId, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .chain_id()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the height of the current block that is executing.
    fn get_block_height(caller: &mut Caller) -> Result<BlockHeight, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .block_height()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the ID of the current application.
    fn get_application_id(caller: &mut Caller) -> Result<ApplicationId, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .application_id()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the application parameters provided when the application was created.
    fn application_parameters(caller: &mut Caller) -> Result<Vec<u8>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .application_parameters()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the authenticated signer for this execution, if there is one.
    fn authenticated_signer(caller: &mut Caller) -> Result<Option<Owner>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .authenticated_signer()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Retrieves the current system time, i.e. the timestamp of the block in which this is called.
    fn read_system_timestamp(caller: &mut Caller) -> Result<Timestamp, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .read_system_timestamp()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the ID of the incoming message that is being handled, or [`None`] if not executing
    /// an incoming message.
    fn get_message_id(caller: &mut Caller) -> Result<Option<MessageId>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .message_id()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns `Some(true)` if the incoming message was rejected from the original destination and
    /// is now bouncing back, `Some(false)` if the message is being currently being delivered to
    /// its original destination, or [`None`] if not executing an incoming message.
    fn message_is_bouncing(caller: &mut Caller) -> Result<Option<bool>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .message_is_bouncing()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the authenticated caller ID, if the caller configured it and if the current context.
    fn authenticated_caller_id(caller: &mut Caller) -> Result<Option<ApplicationId>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .authenticated_caller_id()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the current chain balance.
    fn read_chain_balance(caller: &mut Caller) -> Result<Amount, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .read_chain_balance()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the balance of one of the accounts on this chain.
    fn read_owner_balance(caller: &mut Caller, owner: Owner) -> Result<Amount, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .read_owner_balance(owner)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Schedules a message to be sent to this application on another chain.
    fn send_message(
        caller: &mut Caller,
        message: SendMessageRequest<Vec<u8>>,
    ) -> Result<(), RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .send_message(message)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Subscribes to a message channel from another chain.
    fn subscribe(
        caller: &mut Caller,
        chain: ChainId,
        channel: ChannelName,
    ) -> Result<(), RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .subscribe(chain, channel)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Unsubscribes to a message channel from another chain.
    fn unsubscribe(
        caller: &mut Caller,
        chain: ChainId,
        channel: ChannelName,
    ) -> Result<(), RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .unsubscribe(chain, channel)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Transfers an `amount` of native tokens from `source` owner account (or the current chain's
    /// balance) to `destination`.
    fn transfer(
        caller: &mut Caller,
        source: Option<Owner>,
        destination: Account,
        amount: Amount,
    ) -> Result<(), RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .transfer(source, destination, amount)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Claims an `amount` of native tokens from a `source` account to a `destination` account.
    fn claim(
        caller: &mut Caller,
        source: Account,
        destination: Account,
        amount: Amount,
    ) -> Result<(), RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .claim(source, destination, amount)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Retrieves the owner configuration for the current chain.
    fn get_chain_ownership(caller: &mut Caller) -> Result<ChainOwnership, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .chain_ownership()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Opens a new chain, configuring it with the provided `chain_ownership` and initial `balance`
    /// (debited from the current chain).
    fn open_chain(
        caller: &mut Caller,
        chain_ownership: ChainOwnership,
        balance: Amount,
    ) -> Result<ChainId, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .open_chain(chain_ownership, balance)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Closes the current chain. Returns an error if the application doesn't have
    /// permission to do so.
    fn close_chain(caller: &mut Caller) -> Result<Result<(), CloseChainError>, RuntimeError> {
        match caller.user_data_mut().runtime.close_chain() {
            Ok(()) => Ok(Ok(())),
            Err(ExecutionError::UnauthorizedApplication(_)) => {
                Ok(Err(CloseChainError::NotPermitted))
            }
            Err(error) => Err(RuntimeError::Custom(error.into())),
        }
    }

    /// Calls another application.
    fn try_call_application(
        caller: &mut Caller,
        authenticated: bool,
        callee_id: ApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .try_call_application(authenticated, callee_id, argument)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Logs a `message` with the provided information `level`.
    fn log(_caller: &mut Caller, message: String, level: log::Level) -> Result<(), RuntimeError> {
        match level {
            log::Level::Trace => tracing::trace!("{message}"),
            log::Level::Debug => tracing::debug!("{message}"),
            log::Level::Info => tracing::info!("{message}"),
            log::Level::Warn => tracing::warn!("{message}"),
            log::Level::Error => tracing::error!("{message}"),
        }
        Ok(())
    }
}

/// An implementation of the system API made available to services.
#[derive(Default)]
pub struct ServiceSystemApi<Caller>(PhantomData<Caller>);

#[linera_witty::wit_export(package = "linera:app")]
impl<Caller, Runtime> ServiceSystemApi<Caller>
where
    Caller: Instance<UserData = SystemApiData<Runtime>>,
    Runtime: ServiceRuntime + Send + 'static,
{
    /// Returns the ID of the current chain.
    fn get_chain_id(caller: &mut Caller) -> Result<ChainId, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .chain_id()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the height of the next block that can be added to the current chain.
    fn get_next_block_height(caller: &mut Caller) -> Result<BlockHeight, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .block_height()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the ID of the current application.
    fn get_application_id(caller: &mut Caller) -> Result<ApplicationId, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .application_id()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the application parameters provided when the application was created.
    fn get_application_parameters(caller: &mut Caller) -> Result<Vec<u8>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .application_parameters()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the current chain balance.
    fn read_chain_balance(caller: &mut Caller) -> Result<Amount, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .read_chain_balance()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the balance of one of the accounts on this chain.
    fn read_owner_balance(caller: &mut Caller, owner: Owner) -> Result<Amount, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .read_owner_balance(owner)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Retrieves the current system time, i.e. the timestamp of the block in which this is called.
    fn read_system_timestamp(caller: &mut Caller) -> Result<Timestamp, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .read_system_timestamp()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the balances of all accounts on the chain.
    fn read_owner_balances(caller: &mut Caller) -> Result<Vec<(Owner, Amount)>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .read_owner_balances()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Returns the owners of accounts on this chain.
    fn read_balance_owners(caller: &mut Caller) -> Result<Vec<Owner>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .read_balance_owners()
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Queries another application.
    fn try_query_application(
        caller: &mut Caller,
        application: ApplicationId,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .try_query_application(application, argument)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Logs a `message` with the provided information `level`.
    fn log(_caller: &mut Caller, message: String, level: log::Level) -> Result<(), RuntimeError> {
        match level {
            log::Level::Trace => tracing::trace!("{message}"),
            log::Level::Debug => tracing::debug!("{message}"),
            log::Level::Info => tracing::info!("{message}"),
            log::Level::Warn => tracing::warn!("{message}"),
            log::Level::Error => tracing::error!("{message}"),
        }
        Ok(())
    }
}

/// An implementation of the system API used to access the view storage for both contracts and
/// services.
#[derive(Default)]
pub struct ViewSystemApi<Caller>(PhantomData<Caller>);

#[linera_witty::wit_export(package = "linera:app")]
impl<Caller, Runtime> ViewSystemApi<Caller>
where
    Caller: Instance<UserData = SystemApiData<Runtime>>,
    Runtime: BaseRuntime + Send + 'static,
{
    /// Creates a new promise to check if the `key` is in storage.
    fn contains_key_new(caller: &mut Caller, key: Vec<u8>) -> Result<u32, RuntimeError> {
        let mut data = caller.user_data_mut();
        let promise = data
            .runtime
            .contains_key_new(key)
            .map_err(|error| RuntimeError::Custom(error.into()))?;

        data.register_promise(promise)
    }

    /// Waits for the promise to check if the `key` is in storage.
    fn contains_key_wait(caller: &mut Caller, promise_id: u32) -> Result<bool, RuntimeError> {
        let mut data = caller.user_data_mut();
        let promise = data.take_promise(promise_id)?;

        data.runtime
            .contains_key_wait(&promise)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Creates a new promise to read multiple entries from storage.
    fn read_multi_values_bytes_new(
        caller: &mut Caller,
        keys: Vec<Vec<u8>>,
    ) -> Result<u32, RuntimeError> {
        let mut data = caller.user_data_mut();
        let promise = data
            .runtime
            .read_multi_values_bytes_new(keys)
            .map_err(|error| RuntimeError::Custom(error.into()))?;

        data.register_promise(promise)
    }

    /// Waits for the promise to read multiple entries from storage.
    fn read_multi_values_bytes_wait(
        caller: &mut Caller,
        promise_id: u32,
    ) -> Result<Vec<Option<Vec<u8>>>, RuntimeError> {
        let mut data = caller.user_data_mut();
        let promise = data.take_promise(promise_id)?;

        data.runtime
            .read_multi_values_bytes_wait(&promise)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Creates a new promise to read a single entry from storage.
    fn read_value_bytes_new(caller: &mut Caller, key: Vec<u8>) -> Result<u32, RuntimeError> {
        let mut data = caller.user_data_mut();
        let promise = data
            .runtime
            .read_value_bytes_new(key)
            .map_err(|error| RuntimeError::Custom(error.into()))?;

        data.register_promise(promise)
    }

    /// Waits for the promise to read a single entry from storage.
    fn read_value_bytes_wait(
        caller: &mut Caller,
        promise_id: u32,
    ) -> Result<Option<Vec<u8>>, RuntimeError> {
        let mut data = caller.user_data_mut();
        let promise = data.take_promise(promise_id)?;

        data.runtime
            .read_value_bytes_wait(&promise)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Creates a new promise to search for keys that start with the `key_prefix`.
    fn find_keys_new(caller: &mut Caller, key_prefix: Vec<u8>) -> Result<u32, RuntimeError> {
        let mut data = caller.user_data_mut();
        let promise = data
            .runtime
            .find_keys_by_prefix_new(key_prefix)
            .map_err(|error| RuntimeError::Custom(error.into()))?;

        data.register_promise(promise)
    }

    /// Waits for the promise to search for keys that start with the `key_prefix`.
    fn find_keys_wait(caller: &mut Caller, promise_id: u32) -> Result<Vec<Vec<u8>>, RuntimeError> {
        let mut data = caller.user_data_mut();
        let promise = data.take_promise(promise_id)?;

        data.runtime
            .find_keys_by_prefix_wait(&promise)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Creates a new promise to search for entries whose keys that start with the `key_prefix`.
    fn find_key_values_new(caller: &mut Caller, key_prefix: Vec<u8>) -> Result<u32, RuntimeError> {
        let mut data = caller.user_data_mut();
        let promise = data
            .runtime
            .find_key_values_by_prefix_new(key_prefix)
            .map_err(|error| RuntimeError::Custom(error.into()))?;

        data.register_promise(promise)
    }

    /// Waits for the promise to search for entries whose keys that start with the `key_prefix`.
    #[allow(clippy::type_complexity)]
    fn find_key_values_wait(
        caller: &mut Caller,
        promise_id: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RuntimeError> {
        let mut data = caller.user_data_mut();
        let promise = data.take_promise(promise_id)?;

        data.runtime
            .find_key_values_by_prefix_wait(&promise)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Writes a batch of `operations` to storage.
    fn write_batch(
        caller: &mut Caller,
        operations: Vec<WriteOperation>,
    ) -> Result<(), RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .write_batch(Batch { operations })
            .map_err(|error| RuntimeError::Custom(error.into()))
    }
}

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

            fn send_message(
                &mut self,
                message: contract_system_api::SendMessageRequest,
            ) -> Result<(), Self::Error> {
                ContractRuntime::send_message(self, message.into())
            }

            fn subscribe(
                &mut self,
                chain: contract_system_api::ChainId,
                channel: contract_system_api::ChannelName,
            ) -> Result<(), Self::Error> {
                ContractRuntime::subscribe(self, chain.into(), channel.into())
            }

            fn unsubscribe(
                &mut self,
                chain: contract_system_api::ChainId,
                channel: contract_system_api::ChannelName,
            ) -> Result<(), Self::Error> {
                ContractRuntime::unsubscribe(self, chain.into(), channel.into())
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
            ) -> Result<Vec<u8>, Self::Error> {
                ContractRuntime::try_call_application(
                    self,
                    authenticated,
                    application.into(),
                    argument.to_vec(),
                )
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

            fn next_block_height(
                &mut self,
            ) -> Result<service_system_api::BlockHeight, Self::Error> {
                BaseRuntime::block_height(self).map(|height| height.into())
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

            fn read_owner_balances(
                &mut self,
            ) -> Result<Vec<(service_system_api::Owner, service_system_api::Amount)>, Self::Error>
            {
                BaseRuntime::read_owner_balances(self).map(|balances| {
                    balances
                        .into_iter()
                        .map(|(owner, amount)| (owner.into(), amount.into()))
                        .collect()
                })
            }

            fn read_balance_owners(
                &mut self,
            ) -> Result<Vec<service_system_api::Owner>, Self::Error> {
                BaseRuntime::read_balance_owners(self)
                    .map(|owners| owners.into_iter().map(|owner| owner.into()).collect())
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

            /// Fetches a blob of bytes from an arbitrary URL.
            fn fetch_url(&mut self, url: &str) -> Result<Vec<u8>, Self::Error> {
                ServiceRuntime::fetch_url(self, url)
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
