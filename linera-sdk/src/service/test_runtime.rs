// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime types to simulate interfacing with the host executing the service.

use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
};

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
    application_id: Cell<Option<ApplicationId<Application::Abi>>>,
    chain_id: Cell<Option<ChainId>>,
    next_block_height: Cell<Option<BlockHeight>>,
    timestamp: Cell<Option<Timestamp>>,
    chain_balance: Cell<Option<Amount>>,
    owner_balances: RefCell<Option<HashMap<Owner, Amount>>>,
    query_application_handler: RefCell<Option<QueryApplicationHandler>>,
    url_blobs: RefCell<Option<HashMap<String, Vec<u8>>>>,
}

impl<Application> MockServiceRuntime<Application>
where
    Application: Service,
{
    /// Creates a new [`MockServiceRuntime`] instance for a service.
    pub(crate) fn new() -> Self {
        MockServiceRuntime {
            application_parameters: Cell::new(None),
            application_id: Cell::new(None),
            chain_id: Cell::new(None),
            next_block_height: Cell::new(None),
            timestamp: Cell::new(None),
            chain_balance: Cell::new(None),
            owner_balances: RefCell::new(None),
            query_application_handler: RefCell::new(None),
            url_blobs: RefCell::new(None),
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

    /// Configures the application ID to return during the test.
    pub fn with_application_id(self, application_id: ApplicationId<Application::Abi>) -> Self {
        self.application_id.set(Some(application_id));
        self
    }

    /// Configures the application ID to return during the test.
    pub fn set_application_id(&self, application_id: ApplicationId<Application::Abi>) -> &Self {
        self.application_id.set(Some(application_id));
        self
    }

    /// Returns the ID of the current application.
    pub fn application_id(&self) -> ApplicationId<Application::Abi> {
        Self::fetch_mocked_value(
            &self.application_id,
            "Application ID has not been mocked, \
            please call `MockServiceRuntime::set_application_id` first",
        )
    }

    /// Configures the chain ID to return during the test.
    pub fn with_chain_id(self, chain_id: ChainId) -> Self {
        self.chain_id.set(Some(chain_id));
        self
    }

    /// Configures the chain ID to return during the test.
    pub fn set_chain_id(&self, chain_id: ChainId) -> &Self {
        self.chain_id.set(Some(chain_id));
        self
    }

    /// Returns the ID of the current chain.
    pub fn chain_id(&self) -> ChainId {
        Self::fetch_mocked_value(
            &self.chain_id,
            "Chain ID has not been mocked, \
            please call `MockServiceRuntime::set_chain_id` first",
        )
    }

    /// Configures the next block height to return during the test.
    pub fn with_next_block_height(self, next_block_height: BlockHeight) -> Self {
        self.next_block_height.set(Some(next_block_height));
        self
    }

    /// Configures the block height to return during the test.
    pub fn set_next_block_height(&self, next_block_height: BlockHeight) -> &Self {
        self.next_block_height.set(Some(next_block_height));
        self
    }

    /// Returns the height of the next block that can be added to the current chain.
    pub fn next_block_height(&self) -> BlockHeight {
        Self::fetch_mocked_value(
            &self.next_block_height,
            "Next block height has not been mocked, \
            please call `MockServiceRuntime::set_next_block_height` first",
        )
    }

    /// Configures the system time to return during the test.
    pub fn with_system_time(self, timestamp: Timestamp) -> Self {
        self.timestamp.set(Some(timestamp));
        self
    }

    /// Configures the system time to return during the test.
    pub fn set_system_time(&self, timestamp: Timestamp) -> &Self {
        self.timestamp.set(Some(timestamp));
        self
    }

    /// Retrieves the current system time, i.e. the timestamp of the block in which this is called.
    pub fn system_time(&self) -> Timestamp {
        Self::fetch_mocked_value(
            &self.timestamp,
            "System time has not been mocked, \
            please call `MockServiceRuntime::set_system_time` first",
        )
    }

    /// Configures the chain balance to return during the test.
    pub fn with_chain_balance(self, chain_balance: Amount) -> Self {
        self.chain_balance.set(Some(chain_balance));
        self
    }

    /// Configures the chain balance to return during the test.
    pub fn set_chain_balance(&self, chain_balance: Amount) -> &Self {
        self.chain_balance.set(Some(chain_balance));
        self
    }

    /// Returns the current chain balance.
    pub fn chain_balance(&self) -> Amount {
        Self::fetch_mocked_value(
            &self.chain_balance,
            "Chain balance has not been mocked, \
            please call `MockServiceRuntime::set_chain_balance` first",
        )
    }

    /// Configures the balances on the chain to use during the test.
    pub fn with_owner_balances(
        self,
        owner_balances: impl IntoIterator<Item = (Owner, Amount)>,
    ) -> Self {
        *self.owner_balances.borrow_mut() = Some(owner_balances.into_iter().collect());
        self
    }

    /// Configures the balances on the chain to use during the test.
    pub fn set_owner_balances(
        &self,
        owner_balances: impl IntoIterator<Item = (Owner, Amount)>,
    ) -> &Self {
        *self.owner_balances.borrow_mut() = Some(owner_balances.into_iter().collect());
        self
    }

    /// Configures the balance of one account on the chain to use during the test.
    pub fn with_owner_balance(self, owner: Owner, balance: Amount) -> Self {
        self.set_owner_balance(owner, balance);
        self
    }

    /// Configures the balance of one account on the chain to use during the test.
    pub fn set_owner_balance(&self, owner: Owner, balance: Amount) -> &Self {
        self.owner_balances
            .borrow_mut()
            .get_or_insert_with(HashMap::new)
            .insert(owner, balance);
        self
    }

    /// Returns the balance of one of the accounts on this chain.
    pub fn owner_balance(&self, owner: Owner) -> Amount {
        self.owner_balances
            .borrow_mut()
            .as_mut()
            .and_then(|owner_balances| owner_balances.get(&owner).copied())
            .unwrap_or_else(|| {
                panic!(
                    "Balance for owner {owner} was not mocked, \
                    please include a balance for them with a call to \
                    `MockServiceRuntime::set_owner_balance`"
                )
            })
    }

    /// Returns the balances of all accounts on the chain.
    pub fn owner_balances(&self) -> Vec<(Owner, Amount)> {
        self.owner_balances
            .borrow_mut()
            .as_ref()
            .expect(
                "Owner balances have not been mocked, \
                please call `MockServiceRuntime::set_owner_balances` first",
            )
            .iter()
            .map(|(owner, amount)| (*owner, *amount))
            .collect()
    }

    /// Returns the owners of accounts on this chain.
    pub fn balance_owners(&self) -> Vec<Owner> {
        self.owner_balances
            .borrow_mut()
            .as_ref()
            .expect(
                "Owner balances have not been mocked, \
                please call `MockServiceRuntime::set_owner_balances` first",
            )
            .keys()
            .cloned()
            .collect()
    }

    /// Configures the handler for application queries made during the test.
    pub fn with_query_application_handler(
        self,
        handler: impl FnMut(ApplicationId, Vec<u8>) -> Vec<u8> + Send + 'static,
    ) -> Self {
        *self.query_application_handler.borrow_mut() = Some(Box::new(handler));
        self
    }

    /// Configures the handler for application queries made during the test.
    pub fn set_query_application_handler(
        &self,
        handler: impl FnMut(ApplicationId, Vec<u8>) -> Vec<u8> + Send + 'static,
    ) -> &Self {
        *self.query_application_handler.borrow_mut() = Some(Box::new(handler));
        self
    }

    /// Queries another application.
    pub fn query_application<A: ServiceAbi>(
        &self,
        application: ApplicationId<A>,
        query: &A::Query,
    ) -> A::QueryResponse {
        let query_bytes =
            serde_json::to_vec(&query).expect("Failed to serialize query to another application");

        let mut handler_guard = self.query_application_handler.borrow_mut();
        let handler = handler_guard.as_mut().expect(
            "Handler for `query_application` has not been mocked, \
            please call `MockServiceRuntime::set_query_application_handler` first",
        );

        let response_bytes = handler(application.forget_abi(), query_bytes);

        serde_json::from_slice(&response_bytes)
            .expect("Failed to deserialize query response from application")
    }

    /// Configures the blobs returned when fetching from URLs during the test.
    pub fn with_url_blobs(self, url_blobs: impl IntoIterator<Item = (String, Vec<u8>)>) -> Self {
        *self.url_blobs.borrow_mut() = Some(url_blobs.into_iter().collect());
        self
    }

    /// Configures the blobs returned when fetching from URLs during the test.
    pub fn set_url_blobs(&self, url_blobs: impl IntoIterator<Item = (String, Vec<u8>)>) -> &Self {
        *self.url_blobs.borrow_mut() = Some(url_blobs.into_iter().collect());
        self
    }

    /// Configures the `blob` returned when fetching from the `url` during the test.
    pub fn with_url_blob(self, url: impl Into<String>, blob: Vec<u8>) -> Self {
        self.set_url_blob(url, blob);
        self
    }

    /// Configures the `blob` returned when fetching from the `url` during the test.
    pub fn set_url_blob(&self, url: impl Into<String>, blob: Vec<u8>) -> &Self {
        self.url_blobs
            .borrow_mut()
            .get_or_insert_with(HashMap::new)
            .insert(url.into(), blob);
        self
    }

    /// Fetches a blob of bytes from a given URL.
    pub fn fetch_url(&self, url: &str) -> Vec<u8> {
        self.url_blobs
            .borrow_mut()
            .as_mut()
            .and_then(|url_blobs| url_blobs.get(url).cloned())
            .unwrap_or_else(|| {
                panic!(
                    "Blob for URL {url:?} has not been mocked, \
                    please call `MockServiceRuntime::set_url_blob` first"
                )
            })
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

/// A type alias for the handler for application queries.
pub type QueryApplicationHandler = Box<dyn FnMut(ApplicationId, Vec<u8>) -> Vec<u8> + Send>;
