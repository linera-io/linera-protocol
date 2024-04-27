// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime types to simulate interfacing with the host executing the service.

use std::cell::Cell;

use linera_base::{
    abi::ServiceAbi,
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{ApplicationId, ChainId, Owner},
};

use crate::Service;

/// The runtime available during execution of a query.
pub struct MockServiceRuntime<Application>
where
    Application: Service,
{
    application_parameters: Cell<Option<Application::Parameters>>,
}

impl<Application> MockServiceRuntime<Application>
where
    Application: Service,
{
    /// Creates a new [`MockServiceRuntime`] instance for a service.
    pub(crate) fn new() -> Self {
        MockServiceRuntime {
            application_parameters: Cell::new(None),
        }
    }

    /// Configures the application parameters to return during the test.
    pub fn with_application_parameters(
        self,
        application_parameters: Application::Parameters,
    ) -> Self {
        self.application_parameters
            .set(Some(application_parameters));
        self
    }

    /// Configures the application parameters to return during the test.
    pub fn set_application_parameters(
        &self,
        application_parameters: Application::Parameters,
    ) -> &Self {
        self.application_parameters
            .set(Some(application_parameters));
        self
    }

    /// Returns the application parameters provided when the application was created.
    pub fn application_parameters(&self) -> Application::Parameters {
        Self::fetch_mocked_value(
            &self.application_parameters,
            "Application parameters have not been mocked, \
            please call `MockServiceRuntime::set_application_parameters` first",
        )
    }

    /// Returns the ID of the current application.
    pub fn application_id(&self) -> ApplicationId<Application::Abi> {
        todo!();
    }

    /// Returns the ID of the current chain.
    pub fn chain_id(&self) -> ChainId {
        todo!();
    }

    /// Returns the height of the next block that can be added to the current chain.
    pub fn next_block_height(&self) -> BlockHeight {
        todo!();
    }

    /// Retrieves the current system time, i.e. the timestamp of the block in which this is called.
    pub fn system_time(&self) -> Timestamp {
        todo!();
    }

    /// Returns the current chain balance.
    pub fn chain_balance(&self) -> Amount {
        todo!();
    }

    /// Returns the balance of one of the accounts on this chain.
    pub fn owner_balance(&self, _owner: Owner) -> Amount {
        todo!();
    }

    /// Returns the balances of all accounts on the chain.
    pub fn owner_balances(&self) -> Vec<(Owner, Amount)> {
        todo!();
    }

    /// Returns the owners of accounts on this chain.
    pub fn balance_owners(&self) -> Vec<Owner> {
        todo!();
    }

    /// Queries another application.
    pub fn query_application<A: ServiceAbi>(
        &self,
        _application: ApplicationId<A>,
        _query: &A::Query,
    ) -> A::QueryResponse {
        todo!();
    }

    /// Fetches a blob of bytes from a given URL.
    pub fn fetch_url(&self, _url: &str) -> Vec<u8> {
        todo!();
    }

    /// Loads a mocked value from the `cell` cache or panics with a provided `message`.
    fn fetch_mocked_value<T>(cell: &Cell<Option<T>>, message: &str) -> T
    where
        T: Clone,
    {
        let value = cell.take().expect(message);
        cell.set(Some(value.clone()));
        value
    }
}
