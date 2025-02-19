// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime types to simulate interfacing with the host executing the service.

use std::{
    collections::{HashMap, VecDeque},
    mem,
    sync::Mutex,
};

use linera_base::{
    abi::ServiceAbi,
    data_types::{Amount, BlockHeight, Timestamp},
    hex, http,
    identifiers::{AccountOwner, ApplicationId, ChainId},
};
use serde::{de::DeserializeOwned, Serialize};

use crate::{DataBlobHash, KeyValueStore, Service, ViewStorageContext};

/// The runtime available during execution of a query.
pub struct MockServiceRuntime<Application>
where
    Application: Service,
{
    application_parameters: Mutex<Option<Application::Parameters>>,
    application_id: Mutex<Option<ApplicationId<Application::Abi>>>,
    chain_id: Mutex<Option<ChainId>>,
    next_block_height: Mutex<Option<BlockHeight>>,
    timestamp: Mutex<Option<Timestamp>>,
    chain_balance: Mutex<Option<Amount>>,
    owner_balances: Mutex<Option<HashMap<AccountOwner, Amount>>>,
    query_application_handler: Mutex<Option<QueryApplicationHandler>>,
    expected_http_requests: VecDeque<(http::Request, http::Response)>,
    url_blobs: Mutex<Option<HashMap<String, Vec<u8>>>>,
    blobs: Mutex<Option<HashMap<DataBlobHash, Vec<u8>>>>,
    scheduled_operations: Mutex<Vec<Vec<u8>>>,
    key_value_store: KeyValueStore,
}

impl<Application> Default for MockServiceRuntime<Application>
where
    Application: Service,
{
    fn default() -> Self {
        MockServiceRuntime::new()
    }
}

impl<Application> MockServiceRuntime<Application>
where
    Application: Service,
{
    /// Creates a new [`MockServiceRuntime`] instance for a service.
    pub fn new() -> Self {
        MockServiceRuntime {
            application_parameters: Mutex::new(None),
            application_id: Mutex::new(None),
            chain_id: Mutex::new(None),
            next_block_height: Mutex::new(None),
            timestamp: Mutex::new(None),
            chain_balance: Mutex::new(None),
            owner_balances: Mutex::new(None),
            query_application_handler: Mutex::new(None),
            expected_http_requests: VecDeque::new(),
            url_blobs: Mutex::new(None),
            blobs: Mutex::new(None),
            scheduled_operations: Mutex::new(vec![]),
            key_value_store: KeyValueStore::mock(),
        }
    }

    /// Returns the key-value store to interface with storage.
    pub fn key_value_store(&self) -> KeyValueStore {
        self.key_value_store.clone()
    }

    /// Returns a storage context suitable for a root view.
    pub fn root_view_storage_context(&self) -> ViewStorageContext {
        ViewStorageContext::new_unsafe(self.key_value_store(), Vec::new(), ())
    }

    /// Configures the application parameters to return during the test.
    pub fn with_application_parameters(
        self,
        application_parameters: Application::Parameters,
    ) -> Self {
        *self.application_parameters.lock().unwrap() = Some(application_parameters);
        self
    }

    /// Configures the application parameters to return during the test.
    pub fn set_application_parameters(
        &self,
        application_parameters: Application::Parameters,
    ) -> &Self {
        *self.application_parameters.lock().unwrap() = Some(application_parameters);
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
        *self.application_id.lock().unwrap() = Some(application_id);
        self
    }

    /// Configures the application ID to return during the test.
    pub fn set_application_id(&self, application_id: ApplicationId<Application::Abi>) -> &Self {
        *self.application_id.lock().unwrap() = Some(application_id);
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
        *self.chain_id.lock().unwrap() = Some(chain_id);
        self
    }

    /// Configures the chain ID to return during the test.
    pub fn set_chain_id(&self, chain_id: ChainId) -> &Self {
        *self.chain_id.lock().unwrap() = Some(chain_id);
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
        *self.next_block_height.lock().unwrap() = Some(next_block_height);
        self
    }

    /// Configures the block height to return during the test.
    pub fn set_next_block_height(&self, next_block_height: BlockHeight) -> &Self {
        *self.next_block_height.lock().unwrap() = Some(next_block_height);
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
        *self.timestamp.lock().unwrap() = Some(timestamp);
        self
    }

    /// Configures the system time to return during the test.
    pub fn set_system_time(&self, timestamp: Timestamp) -> &Self {
        *self.timestamp.lock().unwrap() = Some(timestamp);
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
        *self.chain_balance.lock().unwrap() = Some(chain_balance);
        self
    }

    /// Configures the chain balance to return during the test.
    pub fn set_chain_balance(&self, chain_balance: Amount) -> &Self {
        *self.chain_balance.lock().unwrap() = Some(chain_balance);
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
        owner_balances: impl IntoIterator<Item = (AccountOwner, Amount)>,
    ) -> Self {
        *self.owner_balances.lock().unwrap() = Some(owner_balances.into_iter().collect());
        self
    }

    /// Configures the balances on the chain to use during the test.
    pub fn set_owner_balances(
        &self,
        owner_balances: impl IntoIterator<Item = (AccountOwner, Amount)>,
    ) -> &Self {
        *self.owner_balances.lock().unwrap() = Some(owner_balances.into_iter().collect());
        self
    }

    /// Configures the balance of one account on the chain to use during the test.
    pub fn with_owner_balance(self, owner: AccountOwner, balance: Amount) -> Self {
        self.set_owner_balance(owner, balance);
        self
    }

    /// Configures the balance of one account on the chain to use during the test.
    pub fn set_owner_balance(&self, owner: AccountOwner, balance: Amount) -> &Self {
        self.owner_balances
            .lock()
            .unwrap()
            .get_or_insert_with(HashMap::new)
            .insert(owner, balance);
        self
    }

    /// Returns the balance of one of the accounts on this chain.
    pub fn owner_balance(&self, owner: AccountOwner) -> Amount {
        self.owner_balances
            .lock()
            .unwrap()
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
    pub fn owner_balances(&self) -> Vec<(AccountOwner, Amount)> {
        self.owner_balances
            .lock()
            .unwrap()
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
    pub fn balance_owners(&self) -> Vec<AccountOwner> {
        self.owner_balances
            .lock()
            .unwrap()
            .as_ref()
            .expect(
                "Owner balances have not been mocked, \
                please call `MockServiceRuntime::set_owner_balances` first",
            )
            .keys()
            .cloned()
            .collect()
    }

    /// Schedules an operation to be included in the block being built.
    ///
    /// The operation is specified as an opaque blob of bytes.
    pub fn schedule_raw_operation(&self, operation: Vec<u8>) {
        self.scheduled_operations.lock().unwrap().push(operation);
    }

    /// Schedules an operation to be included in the block being built.
    ///
    /// The operation is serialized using BCS.
    pub fn schedule_operation(&self, operation: &impl Serialize) {
        let bytes = bcs::to_bytes(operation).expect("Failed to serialize application operation");

        self.schedule_raw_operation(bytes);
    }

    /// Returns the list of operations scheduled since the most recent of:
    ///
    /// - the last call to this method;
    /// - the last call to [`Self::scheduled_operations`];
    /// - or since the mock runtime was created.
    pub fn raw_scheduled_operations(&self) -> Vec<Vec<u8>> {
        mem::take(&mut self.scheduled_operations.lock().unwrap())
    }

    /// Returns the list of operations scheduled since the most recent of:
    ///
    /// - the last call to this method;
    /// - the last call to [`Self::raw_scheduled_operations`];
    /// - or since the mock runtime was created.
    ///
    /// All operations are deserialized using BCS into the `Operation` generic type.
    pub fn scheduled_operations<Operation>(&self) -> Vec<Operation>
    where
        Operation: DeserializeOwned,
    {
        self.raw_scheduled_operations()
            .into_iter()
            .enumerate()
            .map(|(index, bytes)| {
                bcs::from_bytes(&bytes).unwrap_or_else(|error| {
                    panic!(
                        "Failed to deserialize scheduled operation #{index} (0x{}): {error}",
                        hex::encode(bytes)
                    )
                })
            })
            .collect()
    }

    /// Configures the handler for application queries made during the test.
    pub fn with_query_application_handler(
        self,
        handler: impl FnMut(ApplicationId, Vec<u8>) -> Vec<u8> + Send + 'static,
    ) -> Self {
        *self.query_application_handler.lock().unwrap() = Some(Box::new(handler));
        self
    }

    /// Configures the handler for application queries made during the test.
    pub fn set_query_application_handler(
        &self,
        handler: impl FnMut(ApplicationId, Vec<u8>) -> Vec<u8> + Send + 'static,
    ) -> &Self {
        *self.query_application_handler.lock().unwrap() = Some(Box::new(handler));
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

        let mut handler_guard = self.query_application_handler.lock().unwrap();
        let handler = handler_guard.as_mut().expect(
            "Handler for `query_application` has not been mocked, \
            please call `MockServiceRuntime::set_query_application_handler` first",
        );

        let response_bytes = handler(application.forget_abi(), query_bytes);

        serde_json::from_slice(&response_bytes)
            .expect("Failed to deserialize query response from application")
    }

    /// Adds an expected `http_request` call, and the response it should return in the test.
    pub fn add_expected_http_request(&mut self, request: http::Request, response: http::Response) {
        self.expected_http_requests.push_back((request, response));
    }

    /// Makes an HTTP `request` as an oracle and returns the HTTP response.
    ///
    /// Should only be used with queries where it is very likely that all validators will receive
    /// the same response, otherwise most block proposals will fail.
    ///
    /// Cannot be used in fast blocks: A block using this call should be proposed by a regular
    /// owner, not a super owner.
    pub fn http_request(&mut self, request: http::Request) -> http::Response {
        let maybe_request = self.expected_http_requests.pop_front();
        let (expected_request, response) = maybe_request.expect("Unexpected HTTP request");
        assert_eq!(request, expected_request);
        response
    }

    /// Configures the blobs returned when fetching from URLs during the test.
    pub fn with_url_blobs(self, url_blobs: impl IntoIterator<Item = (String, Vec<u8>)>) -> Self {
        *self.url_blobs.lock().unwrap() = Some(url_blobs.into_iter().collect());
        self
    }

    /// Configures the blobs returned when fetching from URLs during the test.
    pub fn set_url_blobs(&self, url_blobs: impl IntoIterator<Item = (String, Vec<u8>)>) -> &Self {
        *self.url_blobs.lock().unwrap() = Some(url_blobs.into_iter().collect());
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
            .lock()
            .unwrap()
            .get_or_insert_with(HashMap::new)
            .insert(url.into(), blob);
        self
    }

    /// Fetches a blob of bytes from a given URL.
    pub fn fetch_url(&self, url: &str) -> Vec<u8> {
        self.url_blobs
            .lock()
            .unwrap()
            .as_mut()
            .and_then(|url_blobs| url_blobs.get(url).cloned())
            .unwrap_or_else(|| {
                panic!(
                    "Blob for URL {url:?} has not been mocked, \
                    please call `MockServiceRuntime::set_url_blob` first"
                )
            })
    }

    /// Configures the `blobs` returned when fetching from hashes during the test.
    pub fn with_blobs(self, blobs: impl IntoIterator<Item = (DataBlobHash, Vec<u8>)>) -> Self {
        *self.blobs.lock().unwrap() = Some(blobs.into_iter().collect());
        self
    }

    /// Configures the `blobs` returned when fetching from hashes during the test.
    pub fn set_blobs(&self, blobs: impl IntoIterator<Item = (DataBlobHash, Vec<u8>)>) -> &Self {
        *self.blobs.lock().unwrap() = Some(blobs.into_iter().collect());
        self
    }

    /// Configures the `blob` returned when fetching from the given hash during the test.
    pub fn with_blob(self, hash: impl Into<DataBlobHash>, blob: Vec<u8>) -> Self {
        self.set_blob(hash, blob);
        self
    }

    /// Configures the `blob` returned when fetching from the hash during the test.
    pub fn set_blob(&self, hash: impl Into<DataBlobHash>, blob: Vec<u8>) -> &Self {
        self.blobs
            .lock()
            .unwrap()
            .get_or_insert_with(HashMap::new)
            .insert(hash.into(), blob);
        self
    }

    /// Fetches a blob from a given hash.
    pub fn read_data_blob(&self, hash: DataBlobHash) -> Vec<u8> {
        self.blobs
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|blobs| blobs.get(&hash).cloned())
            .unwrap_or_else(|| {
                panic!(
                    "Blob for hash {hash:?} has not been mocked, \
                    please call `MockServiceRuntime::set_blob` first"
                )
            })
    }

    /// Asserts that a blob with the given hash exists in storage.
    pub fn assert_blob_exists(&self, hash: DataBlobHash) {
        self.blobs
            .lock()
            .unwrap()
            .as_ref()
            .map(|blobs| blobs.contains_key(&hash))
            .unwrap_or_else(|| {
                panic!(
                    "Blob for hash {hash:?} has not been mocked, \
                    please call `MockServiceRuntime::set_blob` first"
                )
            });
    }

    /// Loads a mocked value from the `slot` cache or panics with a provided `message`.
    fn fetch_mocked_value<T>(slot: &Mutex<Option<T>>, message: &str) -> T
    where
        T: Clone,
    {
        slot.lock().unwrap().clone().expect(message)
    }
}

/// A type alias for the handler for application queries.
pub type QueryApplicationHandler = Box<dyn FnMut(ApplicationId, Vec<u8>) -> Vec<u8> + Send>;
