// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime types to interface with the host executing the service.

use std::cell::Cell;

use linera_base::{
    abi::ServiceAbi,
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{AccountOwner, ApplicationId, ChainId},
};

use super::wit::service_system_api as wit;
use crate::{DataBlobHash, KeyValueStore, Service, ViewStorageContext};

/// The runtime available during execution of a query.
pub struct ServiceRuntime<Application>
where
    Application: Service,
{
    application_parameters: Cell<Option<Application::Parameters>>,
    application_id: Cell<Option<ApplicationId<Application::Abi>>>,
    chain_id: Cell<Option<ChainId>>,
    next_block_height: Cell<Option<BlockHeight>>,
    timestamp: Cell<Option<Timestamp>>,
    chain_balance: Cell<Option<Amount>>,
    owner_balances: Cell<Option<Vec<(AccountOwner, Amount)>>>,
    balance_owners: Cell<Option<Vec<AccountOwner>>>,
}

impl<Application> ServiceRuntime<Application>
where
    Application: Service,
{
    /// Creates a new [`ServiceRuntime`] instance for a service.
    pub(crate) fn new() -> Self {
        ServiceRuntime {
            application_parameters: Cell::new(None),
            application_id: Cell::new(None),
            chain_id: Cell::new(None),
            next_block_height: Cell::new(None),
            timestamp: Cell::new(None),
            chain_balance: Cell::new(None),
            owner_balances: Cell::new(None),
            balance_owners: Cell::new(None),
        }
    }

    /// Returns the key-value store to interface with storage.
    pub fn key_value_store(&self) -> KeyValueStore {
        KeyValueStore::for_services()
    }

    /// Returns a storage context suitable for a root view.
    pub fn root_view_storage_context(&self) -> ViewStorageContext {
        ViewStorageContext::new_unsafe(self.key_value_store(), Vec::new(), ())
    }

    /// Returns the application parameters provided when the application was created.
    pub fn application_parameters(&self) -> Application::Parameters {
        Self::fetch_value_through_cache(&self.application_parameters, || {
            let bytes = wit::get_application_parameters();
            serde_json::from_slice(&bytes).expect("Application parameters must be deserializable")
        })
    }

    /// Returns the ID of the current application.
    pub fn application_id(&self) -> ApplicationId<Application::Abi> {
        Self::fetch_value_through_cache(&self.application_id, || {
            ApplicationId::from(wit::get_application_id()).with_abi()
        })
    }

    /// Returns the ID of the current chain.
    pub fn chain_id(&self) -> ChainId {
        Self::fetch_value_through_cache(&self.chain_id, || wit::get_chain_id().into())
    }

    /// Returns the height of the next block that can be added to the current chain.
    pub fn next_block_height(&self) -> BlockHeight {
        Self::fetch_value_through_cache(&self.next_block_height, || {
            wit::get_next_block_height().into()
        })
    }

    /// Retrieves the current system time, i.e. the timestamp of the block in which this is called.
    pub fn system_time(&self) -> Timestamp {
        Self::fetch_value_through_cache(&self.timestamp, || wit::read_system_timestamp().into())
    }

    /// Returns the current chain balance.
    pub fn chain_balance(&self) -> Amount {
        Self::fetch_value_through_cache(&self.chain_balance, || wit::read_chain_balance().into())
    }

    /// Returns the balance of one of the accounts on this chain.
    pub fn owner_balance(&self, owner: AccountOwner) -> Amount {
        wit::read_owner_balance(owner.into()).into()
    }

    /// Returns the balances of all accounts on the chain.
    pub fn owner_balances(&self) -> Vec<(AccountOwner, Amount)> {
        Self::fetch_value_through_cache(&self.owner_balances, || {
            wit::read_owner_balances()
                .into_iter()
                .map(|(owner, amount)| (owner.into(), amount.into()))
                .collect()
        })
    }

    /// Returns the owners of accounts on this chain.
    pub fn balance_owners(&self) -> Vec<AccountOwner> {
        Self::fetch_value_through_cache(&self.balance_owners, || {
            wit::read_balance_owners()
                .into_iter()
                .map(AccountOwner::from)
                .collect()
        })
    }

    /// Queries another application.
    pub fn query_application<A: ServiceAbi>(
        &self,
        application: ApplicationId<A>,
        query: &A::Query,
    ) -> A::QueryResponse {
        let query_bytes =
            serde_json::to_vec(&query).expect("Failed to serialize query to another application");

        let response_bytes =
            wit::try_query_application(application.forget_abi().into(), &query_bytes);

        serde_json::from_slice(&response_bytes)
            .expect("Failed to deserialize query response from application")
    }

    /// Fetches a blob of bytes from a given URL.
    pub fn fetch_url(&self, url: &str) -> Vec<u8> {
        wit::fetch_url(url)
    }

    /// Loads a value from the `cell` cache or fetches it and stores it in the cache.
    fn fetch_value_through_cache<T>(cell: &Cell<Option<T>>, fetch: impl FnOnce() -> T) -> T
    where
        T: Clone,
    {
        let value = cell.take().unwrap_or_else(fetch);
        cell.set(Some(value.clone()));
        value
    }

    /// Reads a data blob with the given hash from storage.
    pub fn read_data_blob(&mut self, hash: DataBlobHash) -> Vec<u8> {
        wit::read_data_blob(hash.0.into())
    }

    /// Asserts that a data blob with the given hash exists in storage.
    pub fn assert_data_blob_exists(&mut self, hash: DataBlobHash) {
        wit::assert_data_blob_exists(hash.0.into())
    }
}
