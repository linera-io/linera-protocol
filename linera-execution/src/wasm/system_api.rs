// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{any::Any, collections::HashMap, marker::PhantomData};

use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, ApplicationPermissions, BlockHeight, SendMessageRequest, Timestamp},
    identifiers::{
        Account, AccountOwner, ApplicationId, ChainId, ChannelName, MessageId, Owner, StreamName,
    },
    ownership::{ChainOwnership, CloseChainError},
};
use linera_views::batch::{Batch, WriteOperation};
use linera_witty::{wit_export, Instance, RuntimeError};
use tracing::log;

use super::WasmExecutionError;
use crate::{
    BaseRuntime, ContractRuntime, ContractSyncRuntimeHandle, ExecutionError, ServiceRuntime,
    ServiceSyncRuntimeHandle,
};

/// Common host data used as the `UserData` of the system API implementations.
pub struct SystemApiData<Runtime> {
    runtime: Runtime,
    active_promises: HashMap<u32, Box<dyn Any + Send + Sync>>,
    promise_counter: u32,
}

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
    Runtime: ContractRuntime + 'static,
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

    /// Returns the chain ID of the current application creator.
    fn get_application_creator_chain_id(caller: &mut Caller) -> Result<ChainId, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .application_creator_chain_id()
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
    fn read_owner_balance(
        caller: &mut Caller,
        owner: AccountOwner,
    ) -> Result<Amount, RuntimeError> {
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

    /// Opens a new chain, configuring it with the provided `chain_ownership`,
    /// `application_permissions` and initial `balance` (debited from the current chain).
    fn open_chain(
        caller: &mut Caller,
        chain_ownership: ChainOwnership,
        application_permissions: ApplicationPermissions,
        balance: Amount,
    ) -> Result<(MessageId, ChainId), RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .open_chain(chain_ownership, application_permissions, balance)
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

    /// Adds an item to an event stream.
    fn emit(
        caller: &mut Caller,
        name: StreamName,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .emit(name, key, value)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Queries a service and returns the response.
    fn query_service(
        caller: &mut Caller,
        application_id: ApplicationId,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .query_service(application_id, query)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Makes a POST request to the given URL and returns the response body.
    fn http_post(
        caller: &mut Caller,
        query: String,
        content_type: String,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .http_post(&query, content_type, payload)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Rejects the transaction if the current time at block validation is `>= timestamp`. Note
    /// that block validation happens at or after the block timestamp, but isn't necessarily the
    /// same.
    fn assert_before(caller: &mut Caller, timestamp: Timestamp) -> Result<(), RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .assert_before(timestamp)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Reads a data blob from storage.
    fn read_data_blob(caller: &mut Caller, hash: CryptoHash) -> Result<Vec<u8>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .read_data_blob(&hash)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Asserts the existence of a data blob with the given hash.
    fn assert_data_blob_exists(caller: &mut Caller, hash: CryptoHash) -> Result<(), RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .assert_data_blob_exists(&hash)
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

    /// Consume some fuel.
    ///
    /// This is intended for the metering instrumentation, but if the user wants to donate
    /// some extra fuel, more power to them!
    fn consume_fuel(caller: &mut Caller, fuel: u64) -> Result<(), RuntimeError> {
        caller
            .user_data_mut()
            .runtime_mut()
            .consume_fuel(fuel)
            .map_err(|e| RuntimeError::Custom(e.into()))
    }
}

/// An implementation of the system API made available to services.
#[derive(Default)]
pub struct ServiceSystemApi<Caller>(PhantomData<Caller>);

#[linera_witty::wit_export(package = "linera:app")]
impl<Caller, Runtime> ServiceSystemApi<Caller>
where
    Caller: Instance<UserData = SystemApiData<Runtime>>,
    Runtime: ServiceRuntime + 'static,
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

    /// Returns the chain ID of the current application creator.
    fn get_application_creator_chain_id(caller: &mut Caller) -> Result<ChainId, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .application_creator_chain_id()
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
    fn read_owner_balance(
        caller: &mut Caller,
        owner: AccountOwner,
    ) -> Result<Amount, RuntimeError> {
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
    fn read_owner_balances(
        caller: &mut Caller,
    ) -> Result<Vec<(AccountOwner, Amount)>, RuntimeError> {
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

    /// Fetches a blob of bytes from a given URL.
    fn fetch_url(caller: &mut Caller, url: String) -> Result<Vec<u8>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .fetch_url(&url)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Queries a service and returns the response.
    fn query_service(
        caller: &mut Caller,
        application_id: ApplicationId,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .query_service(application_id, query)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Makes a POST request to the given URL and returns the response body.
    fn http_post(
        caller: &mut Caller,
        query: String,
        content_type: String,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .http_post(&query, content_type, payload)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Reads a data blob from storage.
    fn read_data_blob(caller: &mut Caller, hash: CryptoHash) -> Result<Vec<u8>, RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .read_data_blob(&hash)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Asserts the existence of a data blob with the given hash.
    fn assert_data_blob_exists(caller: &mut Caller, hash: CryptoHash) -> Result<(), RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .assert_data_blob_exists(&hash)
            .map_err(|error| RuntimeError::Custom(error.into()))
    }

    /// Aborts the query if the current time at block validation is `>= timestamp`. Note that block
    /// validation happens at or after the block timestamp, but isn't necessarily the same.
    fn assert_before(caller: &mut Caller, timestamp: Timestamp) -> Result<(), RuntimeError> {
        caller
            .user_data_mut()
            .runtime
            .assert_before(timestamp)
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
    Runtime: BaseRuntime + WriteBatch + 'static,
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

    /// Creates a new promise to check if the `keys` are in storage.
    fn contains_keys_new(caller: &mut Caller, keys: Vec<Vec<u8>>) -> Result<u32, RuntimeError> {
        let mut data = caller.user_data_mut();
        let promise = data
            .runtime
            .contains_keys_new(keys)
            .map_err(|error| RuntimeError::Custom(error.into()))?;

        data.register_promise(promise)
    }

    /// Waits for the promise to check if the `keys` are in storage.
    fn contains_keys_wait(caller: &mut Caller, promise_id: u32) -> Result<Vec<bool>, RuntimeError> {
        let mut data = caller.user_data_mut();
        let promise = data.take_promise(promise_id)?;

        data.runtime
            .contains_keys_wait(&promise)
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
    #[expect(clippy::type_complexity)]
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
        WriteBatch::write_batch(&mut caller.user_data_mut().runtime, Batch { operations })
            .map_err(|error| RuntimeError::Custom(error.into()))
    }
}

// TODO(#1977): Remove once the WIT interface does not include `write-batch` in the service system
// API
/// An extension trait to separate the behavior between the contract runtime and the service
/// runtime.
pub trait WriteBatch {
    /// Writes a [`Batch`] of operations to storage.
    fn write_batch(&mut self, batch: Batch) -> Result<(), ExecutionError>;
}

impl WriteBatch for ContractSyncRuntimeHandle {
    fn write_batch(&mut self, batch: Batch) -> Result<(), ExecutionError> {
        ContractRuntime::write_batch(self, batch)
    }
}

impl WriteBatch for ServiceSyncRuntimeHandle {
    fn write_batch(&mut self, _: Batch) -> Result<(), ExecutionError> {
        Err(ExecutionError::ServiceWriteAttempt)
    }
}
