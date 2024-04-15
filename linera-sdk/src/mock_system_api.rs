// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Hooks for mocking system APIs inside unit tests.

#![allow(missing_docs)]
#![allow(clippy::type_complexity)]

use std::{any::Any, marker::PhantomData};

use linera_base::{
    data_types::{Amount, Timestamp},
    identifiers::{ApplicationId, ChainId},
};
use linera_views::batch::WriteOperation;
use linera_witty::{wit_export, wit_import, Instance, Runtime, RuntimeError, RuntimeMemory};

/// A map of resources allocated on the host side.
#[derive(Default)]
pub struct Resources(Vec<Box<dyn Any + Send + 'static>>);

impl Resources {
    /// Adds a resource to the map, returning its handle.
    pub fn insert(&mut self, value: impl Any + Send + 'static) -> i32 {
        let handle = self.0.len().try_into().expect("Resources map overflow");

        self.0.push(Box::new(value));

        handle
    }

    /// Returns an immutable reference to a resource referenced by the provided `handle`.
    pub fn get<T: 'static>(&self, handle: i32) -> &T {
        self.0[usize::try_from(handle).expect("Invalid handle")]
            .downcast_ref()
            .expect("Incorrect handle type")
    }
}

/// The interface application tests implement to intercept system API calls.
#[wit_import(package = "linera:app")]
pub trait MockSystemApi {
    fn mocked_chain_id() -> ChainId;
    fn mocked_application_id() -> ApplicationId;
    fn mocked_application_parameters() -> Vec<u8>;
    fn mocked_read_chain_balance() -> Amount;
    fn mocked_read_system_timestamp() -> Timestamp;
    fn mocked_log(message: String, level: log::Level);
    fn mocked_read_multi_values_bytes(keys: Vec<Vec<u8>>) -> Vec<Option<Vec<u8>>>;
    fn mocked_read_value_bytes(key: Vec<u8>) -> Option<Vec<u8>>;
    fn mocked_find_keys(prefix: Vec<u8>) -> Vec<Vec<u8>>;
    fn mocked_find_key_values(prefix: Vec<u8>) -> Vec<(Vec<u8>, Vec<u8>)>;
    fn mocked_write_batch(operations: Vec<WriteOperation>);
    fn mocked_try_query_application(application: ApplicationId, query: Vec<u8>) -> Vec<u8>;
}

/// Test runner's implementation of the contract system APIs.
#[derive(Default)]
pub struct ContractSystemApi<Caller>(PhantomData<Caller>);

#[wit_export(package = "linera:app")]
impl<Caller> ContractSystemApi<Caller>
where
    Caller: Instance<UserData = Resources> + InstanceForMockSystemApi,
    <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
{
    fn get_chain_id(caller: &mut Caller) -> Result<ChainId, RuntimeError> {
        MockSystemApi::new(caller).mocked_chain_id()
    }

    fn get_application_id(caller: &mut Caller) -> Result<ApplicationId, RuntimeError> {
        MockSystemApi::new(caller).mocked_application_id()
    }

    fn application_parameters(caller: &mut Caller) -> Result<Vec<u8>, RuntimeError> {
        MockSystemApi::new(caller).mocked_application_parameters()
    }

    fn read_chain_balance(caller: &mut Caller) -> Result<Amount, RuntimeError> {
        MockSystemApi::new(caller).mocked_read_chain_balance()
    }

    fn read_system_timestamp(caller: &mut Caller) -> Result<Timestamp, RuntimeError> {
        MockSystemApi::new(caller).mocked_read_system_timestamp()
    }

    fn log(caller: &mut Caller, message: String, level: log::Level) -> Result<(), RuntimeError> {
        MockSystemApi::new(caller).mocked_log(message, level)
    }
}

/// Test runner's implementation of the service system APIs.
#[derive(Default)]
pub struct ServiceSystemApi<Caller>(PhantomData<Caller>);

#[wit_export(package = "linera:app")]
impl<Caller> ServiceSystemApi<Caller>
where
    Caller: Instance<UserData = Resources> + InstanceForMockSystemApi,
    <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
{
    fn get_chain_id(caller: &mut Caller) -> Result<ChainId, RuntimeError> {
        MockSystemApi::new(caller).mocked_chain_id()
    }

    fn get_application_id(caller: &mut Caller) -> Result<ApplicationId, RuntimeError> {
        MockSystemApi::new(caller).mocked_application_id()
    }

    fn get_application_parameters(caller: &mut Caller) -> Result<Vec<u8>, RuntimeError> {
        MockSystemApi::new(caller).mocked_application_parameters()
    }

    fn read_chain_balance(caller: &mut Caller) -> Result<Amount, RuntimeError> {
        MockSystemApi::new(caller).mocked_read_chain_balance()
    }

    fn read_system_timestamp(caller: &mut Caller) -> Result<Timestamp, RuntimeError> {
        MockSystemApi::new(caller).mocked_read_system_timestamp()
    }

    fn try_query_application(
        caller: &mut Caller,
        application_id: ApplicationId,
        query: Vec<u8>,
    ) -> Result<Vec<u8>, RuntimeError> {
        MockSystemApi::new(caller).mocked_try_query_application(application_id, query)
    }

    fn log(caller: &mut Caller, message: String, level: log::Level) -> Result<(), RuntimeError> {
        MockSystemApi::new(caller).mocked_log(message, level)
    }
}

/// Test runner's implementation of the view system APIs shared between contracts and services.
#[derive(Default)]
pub struct ViewSystemApi<Caller>(PhantomData<Caller>);

#[wit_export(package = "linera:app")]
impl<Caller> ViewSystemApi<Caller>
where
    Caller: Instance<UserData = Resources> + InstanceForMockSystemApi,
    <Caller::Runtime as Runtime>::Memory: RuntimeMemory<Caller>,
{
    fn read_multi_values_bytes_new(
        caller: &mut Caller,
        keys: Vec<Vec<u8>>,
    ) -> Result<u32, RuntimeError> {
        Ok(caller.user_data_mut().insert(keys) as u32)
    }

    fn read_multi_values_bytes_wait(
        caller: &mut Caller,
        promise_id: u32,
    ) -> Result<Vec<Option<Vec<u8>>>, RuntimeError> {
        let keys = caller
            .user_data_mut()
            .get::<Vec<Vec<u8>>>(promise_id as i32)
            .clone();

        MockSystemApi::new(caller).mocked_read_multi_values_bytes(keys)
    }

    fn read_value_bytes_new(caller: &mut Caller, key: Vec<u8>) -> Result<u32, RuntimeError> {
        Ok(caller.user_data_mut().insert(key) as u32)
    }

    fn read_value_bytes_wait(
        caller: &mut Caller,
        promise_id: u32,
    ) -> Result<Option<Vec<u8>>, RuntimeError> {
        let key = caller
            .user_data_mut()
            .get::<Vec<u8>>(promise_id as i32)
            .clone();

        MockSystemApi::new(caller).mocked_read_value_bytes(key)
    }

    fn find_keys_new(caller: &mut Caller, prefix: Vec<u8>) -> Result<u32, RuntimeError> {
        Ok(caller.user_data_mut().insert(prefix) as u32)
    }

    fn find_keys_wait(caller: &mut Caller, promise_id: u32) -> Result<Vec<Vec<u8>>, RuntimeError> {
        let prefix = caller
            .user_data_mut()
            .get::<Vec<u8>>(promise_id as i32)
            .clone();

        MockSystemApi::new(caller).mocked_find_keys(prefix)
    }

    fn find_key_values_new(caller: &mut Caller, prefix: Vec<u8>) -> Result<u32, RuntimeError> {
        Ok(caller.user_data_mut().insert(prefix) as u32)
    }

    #[allow(clippy::type_complexity)]
    fn find_key_values_wait(
        caller: &mut Caller,
        promise_id: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RuntimeError> {
        let prefix = caller
            .user_data_mut()
            .get::<Vec<u8>>(promise_id as i32)
            .clone();

        MockSystemApi::new(caller).mocked_find_key_values(prefix)
    }

    fn write_batch(
        caller: &mut Caller,
        operations: Vec<WriteOperation>,
    ) -> Result<(), RuntimeError> {
        MockSystemApi::new(caller).mocked_write_batch(operations)
    }
}
