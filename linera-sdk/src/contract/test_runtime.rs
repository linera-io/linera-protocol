// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime types to simulate interfacing with the host executing the contract.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
};

use linera_base::{
    abi::{ContractAbi, ServiceAbi},
    data_types::{
        Amount, ApplicationDescription, ApplicationPermissions, BlockHeight, Bytecode, Resources,
        SendMessageRequest, Timestamp,
    },
    ensure, http,
    identifiers::{
        Account, AccountOwner, ApplicationId, BlobId, ChainId, DataBlobHash, ModuleId, StreamName,
    },
    ownership::{AccountPermissionError, ChainOwnership, ManageChainError},
    vm::VmRuntime,
};
use serde::Serialize;

use crate::{Contract, KeyValueStore, ViewStorageContext};

struct ExpectedPublishModuleCall {
    contract: Bytecode,
    service: Bytecode,
    vm_runtime: VmRuntime,
    module_id: ModuleId,
}

struct ExpectedCreateApplicationCall {
    module_id: ModuleId,
    parameters: Vec<u8>,
    argument: Vec<u8>,
    required_application_ids: Vec<ApplicationId>,
    application_id: ApplicationId,
}

struct ExpectedCreateDataBlobCall {
    bytes: Vec<u8>,
    blob_id: BlobId,
}

/// A mock of the common runtime to interface with the host executing the contract.
pub struct MockContractRuntime<Application>
where
    Application: Contract,
{
    application_parameters: Option<Application::Parameters>,
    application_id: Option<ApplicationId<Application::Abi>>,
    application_creator_chain_id: Option<ChainId>,
    application_descriptions: HashMap<ApplicationId, ApplicationDescription>,
    chain_id: Option<ChainId>,
    authenticated_owner: Option<Option<AccountOwner>>,
    block_height: Option<BlockHeight>,
    round: Option<u32>,
    message_is_bouncing: Option<Option<bool>>,
    message_origin_chain_id: Option<Option<ChainId>>,
    authenticated_caller_id: Option<Option<ApplicationId>>,
    timestamp: Option<Timestamp>,
    chain_balance: Option<Amount>,
    owner_balances: Option<HashMap<AccountOwner, Amount>>,
    chain_ownership: Option<ChainOwnership>,
    application_permissions: Option<ApplicationPermissions>,
    can_manage_chain: Option<bool>,
    call_application_handler: Option<CallApplicationHandler>,
    send_message_requests: Arc<Mutex<Vec<SendMessageRequest<Application::Message>>>>,
    outgoing_transfers: HashMap<Account, Amount>,
    created_events: BTreeMap<StreamName, Vec<Vec<u8>>>,
    events: BTreeMap<(ChainId, StreamName, u32), Vec<u8>>,
    claim_requests: Vec<ClaimRequest>,
    expected_service_queries: VecDeque<(ApplicationId, String, String)>,
    expected_http_requests: VecDeque<(http::Request, http::Response)>,
    expected_read_data_blob_requests: VecDeque<(DataBlobHash, Vec<u8>)>,
    expected_assert_data_blob_exists_requests: VecDeque<(DataBlobHash, Option<()>)>,
    expected_has_empty_storage_requests: VecDeque<(ApplicationId, bool)>,
    expected_open_chain_calls: VecDeque<(ChainOwnership, ApplicationPermissions, Amount, ChainId)>,
    expected_publish_module_calls: VecDeque<ExpectedPublishModuleCall>,
    expected_create_application_calls: VecDeque<ExpectedCreateApplicationCall>,
    expected_create_data_blob_calls: VecDeque<ExpectedCreateDataBlobCall>,
    remaining_fuel: Option<u64>,
    key_value_store: KeyValueStore,
}

impl<Application> Default for MockContractRuntime<Application>
where
    Application: Contract,
{
    fn default() -> Self {
        MockContractRuntime::new()
    }
}

impl<Application> MockContractRuntime<Application>
where
    Application: Contract,
{
    /// Creates a new [`MockContractRuntime`] instance for a contract.
    pub fn new() -> Self {
        MockContractRuntime {
            application_parameters: None,
            application_id: None,
            application_creator_chain_id: None,
            application_descriptions: HashMap::new(),
            chain_id: None,
            authenticated_owner: None,
            block_height: None,
            round: None,
            message_is_bouncing: None,
            message_origin_chain_id: None,
            authenticated_caller_id: None,
            timestamp: None,
            chain_balance: None,
            owner_balances: None,
            chain_ownership: None,
            application_permissions: None,
            can_manage_chain: None,
            call_application_handler: None,
            send_message_requests: Arc::default(),
            outgoing_transfers: HashMap::new(),
            created_events: BTreeMap::new(),
            events: BTreeMap::new(),
            claim_requests: Vec::new(),
            expected_service_queries: VecDeque::new(),
            expected_http_requests: VecDeque::new(),
            expected_read_data_blob_requests: VecDeque::new(),
            expected_assert_data_blob_exists_requests: VecDeque::new(),
            expected_has_empty_storage_requests: VecDeque::new(),
            expected_open_chain_calls: VecDeque::new(),
            expected_publish_module_calls: VecDeque::new(),
            expected_create_application_calls: VecDeque::new(),
            expected_create_data_blob_calls: VecDeque::new(),
            remaining_fuel: None,
            key_value_store: KeyValueStore::mock().to_mut(),
        }
    }

    /// Returns the key-value store to interface with storage.
    pub fn key_value_store(&self) -> KeyValueStore {
        self.key_value_store.clone()
    }

    /// Returns a storage context suitable for a root view.
    pub fn root_view_storage_context(&self) -> ViewStorageContext {
        ViewStorageContext::new_unchecked(self.key_value_store(), Vec::new(), ())
    }

    /// Configures the application parameters to return during the test.
    pub fn with_application_parameters(
        mut self,
        application_parameters: Application::Parameters,
    ) -> Self {
        self.application_parameters = Some(application_parameters);
        self
    }

    /// Configures the application parameters to return during the test.
    pub fn set_application_parameters(
        &mut self,
        application_parameters: Application::Parameters,
    ) -> &mut Self {
        self.application_parameters = Some(application_parameters);
        self
    }

    /// Returns the application parameters provided when the application was created.
    pub fn application_parameters(&mut self) -> Application::Parameters {
        self.application_parameters.clone().expect(
            "Application parameters have not been mocked, \
            please call `MockContractRuntime::set_application_parameters` first",
        )
    }

    /// Configures the application ID to return during the test.
    pub fn with_application_id(mut self, application_id: ApplicationId<Application::Abi>) -> Self {
        self.application_id = Some(application_id);
        self
    }

    /// Configures the application ID to return during the test.
    pub fn set_application_id(
        &mut self,
        application_id: ApplicationId<Application::Abi>,
    ) -> &mut Self {
        self.application_id = Some(application_id);
        self
    }

    /// Returns the ID of the current application.
    pub fn application_id(&mut self) -> ApplicationId<Application::Abi> {
        self.application_id.expect(
            "Application ID has not been mocked, \
            please call `MockContractRuntime::set_application_id` first",
        )
    }

    /// Configures the application creator chain ID to return during the test.
    pub fn with_application_creator_chain_id(mut self, chain_id: ChainId) -> Self {
        self.application_creator_chain_id = Some(chain_id);
        self
    }

    /// Configures the application creator chain ID to return during the test.
    pub fn set_application_creator_chain_id(&mut self, chain_id: ChainId) -> &mut Self {
        self.application_creator_chain_id = Some(chain_id);
        self
    }

    /// Returns the chain ID of the current application creator.
    pub fn application_creator_chain_id(&mut self) -> ChainId {
        self.application_creator_chain_id.expect(
            "Application creator chain ID has not been mocked, \
            please call `MockContractRuntime::set_application_creator_chain_id` first",
        )
    }

    /// Configures the application description to return for a specific application during the test.
    pub fn with_application_description(
        mut self,
        application_id: ApplicationId,
        description: ApplicationDescription,
    ) -> Self {
        self.application_descriptions
            .insert(application_id, description);
        self
    }

    /// Configures the application description to return for a specific application during the test.
    pub fn set_application_description(
        &mut self,
        application_id: ApplicationId,
        description: ApplicationDescription,
    ) -> &mut Self {
        self.application_descriptions
            .insert(application_id, description);
        self
    }

    /// Returns the description of the given application.
    pub fn read_application_description(
        &mut self,
        application_id: ApplicationId,
    ) -> ApplicationDescription {
        self.application_descriptions
            .get(&application_id)
            .cloned()
            .unwrap_or_else(|| {
                panic!(
                    "Application description for {application_id:?} has not been mocked, \
                    please call `MockContractRuntime::set_application_description` first"
                )
            })
    }

    /// Configures the chain ID to return during the test.
    pub fn with_chain_id(mut self, chain_id: ChainId) -> Self {
        self.chain_id = Some(chain_id);
        self
    }

    /// Configures the chain ID to return during the test.
    pub fn set_chain_id(&mut self, chain_id: ChainId) -> &mut Self {
        self.chain_id = Some(chain_id);
        self
    }

    /// Returns the ID of the current chain.
    pub fn chain_id(&mut self) -> ChainId {
        self.chain_id.expect(
            "Chain ID has not been mocked, \
            please call `MockContractRuntime::set_chain_id` first",
        )
    }

    /// Configures the authenticated owner to return during the test.
    pub fn with_authenticated_owner(
        mut self,
        authenticated_owner: impl Into<Option<AccountOwner>>,
    ) -> Self {
        self.authenticated_owner = Some(authenticated_owner.into());
        self
    }

    /// Configures the authenticated owner to return during the test.
    pub fn set_authenticated_owner(
        &mut self,
        authenticated_owner: impl Into<Option<AccountOwner>>,
    ) -> &mut Self {
        self.authenticated_owner = Some(authenticated_owner.into());
        self
    }

    /// Returns the authenticated owner for this execution, if there is one.
    pub fn authenticated_owner(&mut self) -> Option<AccountOwner> {
        self.authenticated_owner.expect(
            "Authenticated owner has not been mocked, \
            please call `MockContractRuntime::set_authenticated_owner` first",
        )
    }

    /// Configures the block height to return during the test.
    pub fn with_block_height(mut self, block_height: BlockHeight) -> Self {
        self.block_height = Some(block_height);
        self
    }

    /// Configures the block height to return during the test.
    pub fn set_block_height(&mut self, block_height: BlockHeight) -> &mut Self {
        self.block_height = Some(block_height);
        self
    }

    /// Configures the multi-leader round number to return during the test.
    pub fn with_round(mut self, round: u32) -> Self {
        self.round = Some(round);
        self
    }

    /// Configures the multi-leader round number to return during the test.
    pub fn set_round(&mut self, round: u32) -> &mut Self {
        self.round = Some(round);
        self
    }

    /// Returns the height of the current block that is executing.
    pub fn block_height(&mut self) -> BlockHeight {
        self.block_height.expect(
            "Block height has not been mocked, \
            please call `MockContractRuntime::set_block_height` first",
        )
    }

    /// Configures the `message_is_bouncing` flag to return during the test.
    pub fn with_message_is_bouncing(
        mut self,
        message_is_bouncing: impl Into<Option<bool>>,
    ) -> Self {
        self.message_is_bouncing = Some(message_is_bouncing.into());
        self
    }

    /// Configures the `message_is_bouncing` flag to return during the test.
    pub fn set_message_is_bouncing(
        &mut self,
        message_is_bouncing: impl Into<Option<bool>>,
    ) -> &mut Self {
        self.message_is_bouncing = Some(message_is_bouncing.into());
        self
    }

    /// Returns [`true`] if the incoming message was rejected from the original destination and is
    /// now bouncing back, or [`None`] if not executing an incoming message.
    pub fn message_is_bouncing(&mut self) -> Option<bool> {
        self.message_is_bouncing.expect(
            "`message_is_bouncing` flag has not been mocked, \
            please call `MockContractRuntime::set_message_is_bouncing` first",
        )
    }

    /// Configures the `message_origin_chain_id` to return during the test.
    pub fn set_message_origin_chain_id(
        &mut self,
        message_origin_chain_id: impl Into<Option<ChainId>>,
    ) -> &mut Self {
        self.message_origin_chain_id = Some(message_origin_chain_id.into());
        self
    }

    /// Returns the chain ID where the incoming message originated from, or [`None`] if not
    /// executing an incoming message.
    pub fn message_origin_chain_id(&mut self) -> Option<ChainId> {
        self.message_origin_chain_id.expect(
            "`message_origin_chain_id` has not been mocked, \
            please call `MockContractRuntime::set_message_origin_chain_id` first",
        )
    }

    /// Configures the authenticated caller ID to return during the test.
    pub fn with_authenticated_caller_id(
        mut self,
        authenticated_caller_id: impl Into<Option<ApplicationId>>,
    ) -> Self {
        self.authenticated_caller_id = Some(authenticated_caller_id.into());
        self
    }

    /// Configures the authenticated caller ID to return during the test.
    pub fn set_authenticated_caller_id(
        &mut self,
        authenticated_caller_id: impl Into<Option<ApplicationId>>,
    ) -> &mut Self {
        self.authenticated_caller_id = Some(authenticated_caller_id.into());
        self
    }

    /// Returns the authenticated caller ID, if the caller configured it and if the current context
    /// is executing a cross-application call.
    pub fn authenticated_caller_id(&mut self) -> Option<ApplicationId> {
        self.authenticated_caller_id.expect(
            "Authenticated caller ID has not been mocked, \
            please call `MockContractRuntime::set_authenticated_caller_id` first",
        )
    }

    /// Verifies that the current execution context authorizes operations on a given account.
    pub fn check_account_permission(
        &mut self,
        owner: AccountOwner,
    ) -> Result<(), AccountPermissionError> {
        ensure!(
            self.authenticated_owner() == Some(owner)
                || self.authenticated_caller_id().map(AccountOwner::from) == Some(owner),
            AccountPermissionError::NotPermitted(owner)
        );
        Ok(())
    }

    /// Configures the system time to return during the test.
    pub fn with_system_time(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Configures the system time to return during the test.
    pub fn set_system_time(&mut self, timestamp: Timestamp) -> &mut Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Retrieves the current system time, i.e. the timestamp of the block in which this is called.
    pub fn system_time(&mut self) -> Timestamp {
        self.timestamp.expect(
            "System time has not been mocked, \
            please call `MockContractRuntime::set_system_time` first",
        )
    }

    /// Configures the chain balance to return during the test.
    pub fn with_chain_balance(mut self, chain_balance: Amount) -> Self {
        self.chain_balance = Some(chain_balance);
        self
    }

    /// Configures the chain balance to return during the test.
    pub fn set_chain_balance(&mut self, chain_balance: Amount) -> &mut Self {
        self.chain_balance = Some(chain_balance);
        self
    }

    /// Returns the current chain balance.
    pub fn chain_balance(&mut self) -> Amount {
        *self.chain_balance_mut()
    }

    /// Returns a mutable reference to the current chain balance.
    fn chain_balance_mut(&mut self) -> &mut Amount {
        self.chain_balance.as_mut().expect(
            "Chain balance has not been mocked, \
            please call `MockContractRuntime::set_chain_balance` first",
        )
    }

    /// Configures the balances on the chain to use during the test.
    pub fn with_owner_balances(
        mut self,
        owner_balances: impl IntoIterator<Item = (AccountOwner, Amount)>,
    ) -> Self {
        self.owner_balances = Some(owner_balances.into_iter().collect());
        self
    }

    /// Configures the balances on the chain to use during the test.
    pub fn set_owner_balances(
        &mut self,
        owner_balances: impl IntoIterator<Item = (AccountOwner, Amount)>,
    ) -> &mut Self {
        self.owner_balances = Some(owner_balances.into_iter().collect());
        self
    }

    /// Configures the balance of one account on the chain to use during the test.
    pub fn with_owner_balance(mut self, owner: AccountOwner, balance: Amount) -> Self {
        self.set_owner_balance(owner, balance);
        self
    }

    /// Configures the balance of one account on the chain to use during the test.
    pub fn set_owner_balance(&mut self, owner: AccountOwner, balance: Amount) -> &mut Self {
        self.owner_balances
            .get_or_insert_with(HashMap::new)
            .insert(owner, balance);
        self
    }

    /// Returns the balance of one of the accounts on this chain.
    pub fn owner_balance(&mut self, owner: AccountOwner) -> Amount {
        *self.owner_balance_mut(owner)
    }

    /// Returns a mutable reference to the balance of one of the accounts on this chain.
    fn owner_balance_mut(&mut self, owner: AccountOwner) -> &mut Amount {
        self.owner_balances
            .as_mut()
            .expect(
                "Owner balances have not been mocked, \
                please call `MockContractRuntime::set_owner_balances` first",
            )
            .get_mut(&owner)
            .unwrap_or_else(|| {
                panic!(
                    "Balance for owner {owner} was not mocked, \
                    please include a balance for them in the call to \
                    `MockContractRuntime::set_owner_balances`"
                )
            })
    }

    /// Schedules a message to be sent to this application on another chain.
    pub fn send_message(&mut self, destination: ChainId, message: Application::Message) {
        self.prepare_message(message).send_to(destination)
    }

    /// Returns a `MessageBuilder` to prepare a message to be sent.
    pub fn prepare_message(
        &mut self,
        message: Application::Message,
    ) -> MessageBuilder<Application::Message> {
        MessageBuilder::new(message, self.send_message_requests.clone())
    }

    /// Returns the list of [`SendMessageRequest`]s created so far during the test.
    pub fn created_send_message_requests(
        &self,
    ) -> MutexGuard<'_, Vec<SendMessageRequest<Application::Message>>> {
        self.send_message_requests
            .try_lock()
            .expect("Unit test should be single-threaded")
    }

    /// Transfers an `amount` of native tokens from `source` owner account (or the current chain's
    /// balance) to `destination`.
    pub fn transfer(&mut self, source: AccountOwner, destination: Account, amount: Amount) {
        self.debit(source, amount);

        if Some(destination.chain_id) == self.chain_id {
            self.credit(destination.owner, amount);
        } else {
            let destination_entry = self.outgoing_transfers.entry(destination).or_default();
            *destination_entry = destination_entry
                .try_add(amount)
                .expect("Outgoing transfer value overflow");
        }
    }

    /// Debits an `amount` of native tokens from a `source` owner account (or the current
    /// chain's balance).
    fn debit(&mut self, source: AccountOwner, amount: Amount) {
        let source_balance = if source == AccountOwner::CHAIN {
            self.chain_balance_mut()
        } else {
            self.owner_balance_mut(source)
        };

        *source_balance = source_balance
            .try_sub(amount)
            .expect("Insufficient funds in source account");
    }

    /// Credits an `amount` of native tokens into a `destination` owner account (or the
    /// current chain's balance).
    fn credit(&mut self, destination: AccountOwner, amount: Amount) {
        let destination_balance = if destination == AccountOwner::CHAIN {
            self.chain_balance_mut()
        } else {
            self.owner_balance_mut(destination)
        };

        *destination_balance = destination_balance
            .try_add(amount)
            .expect("Account balance overflow");
    }

    /// Returns the outgoing transfers scheduled during the test so far.
    pub fn outgoing_transfers(&self) -> &HashMap<Account, Amount> {
        &self.outgoing_transfers
    }

    /// Claims an `amount` of native tokens from a `source` account to a `destination` account.
    pub fn claim(&mut self, source: Account, destination: Account, amount: Amount) {
        if Some(source.chain_id) == self.chain_id {
            self.debit(source.owner, amount);

            if Some(destination.chain_id) == self.chain_id {
                self.credit(destination.owner, amount);
            }
        }

        self.claim_requests.push(ClaimRequest {
            source,
            amount,
            destination,
        });
    }

    /// Returns the list of claims made during the test so far.
    pub fn claim_requests(&self) -> &[ClaimRequest] {
        &self.claim_requests
    }

    /// Configures the chain ownership configuration to return during the test.
    pub fn with_chain_ownership(mut self, chain_ownership: ChainOwnership) -> Self {
        self.chain_ownership = Some(chain_ownership);
        self
    }

    /// Configures the chain ownership configuration to return during the test.
    pub fn set_chain_ownership(&mut self, chain_ownership: ChainOwnership) -> &mut Self {
        self.chain_ownership = Some(chain_ownership);
        self
    }

    /// Retrieves the owner configuration for the current chain.
    pub fn chain_ownership(&mut self) -> ChainOwnership {
        self.chain_ownership.clone().expect(
            "Chain ownership has not been mocked, \
            please call `MockContractRuntime::set_chain_ownership` first",
        )
    }

    /// Configures the application permissions to return during the test.
    pub fn with_application_permissions(
        mut self,
        application_permissions: ApplicationPermissions,
    ) -> Self {
        self.application_permissions = Some(application_permissions);
        self
    }

    /// Configures the application permissions to return during the test.
    pub fn set_application_permissions(
        &mut self,
        application_permissions: ApplicationPermissions,
    ) -> &mut Self {
        self.application_permissions = Some(application_permissions);
        self
    }

    /// Retrieves the application permissions for the current chain.
    pub fn application_permissions(&mut self) -> ApplicationPermissions {
        self.application_permissions.clone().expect(
            "Application permissions have not been mocked, \
            please call `MockContractRuntime::set_application_permissions` first",
        )
    }

    /// Configures if the application being tested is allowed to manage the chain, i.e. close
    /// it, change the application permissions, and change the ownership.
    pub fn with_can_manage_chain(mut self, can_manage_chain: bool) -> Self {
        self.can_manage_chain = Some(can_manage_chain);
        self
    }

    /// Configures if the application being tested is allowed to manage the chain, i.e. close
    /// it, change the application permissions, and change the ownership.
    pub fn set_can_manage_chain(&mut self, can_manage_chain: bool) -> &mut Self {
        self.can_manage_chain = Some(can_manage_chain);
        self
    }

    /// Closes the current chain. Returns an error if the application doesn't have
    /// permission to do so.
    pub fn close_chain(&mut self) -> Result<(), ManageChainError> {
        let authorized = self.can_manage_chain.expect(
            "Authorization to manage the chain has not been mocked, \
            please call `MockContractRuntime::set_can_manage_chain` first",
        );

        if authorized {
            Ok(())
        } else {
            Err(ManageChainError::NotPermitted)
        }
    }

    /// Changes the ownership of the current chain. Returns an error if the application doesn't
    /// have permission to do so.
    pub fn change_ownership(&mut self, ownership: ChainOwnership) -> Result<(), ManageChainError> {
        let authorized = self.can_manage_chain.expect(
            "Authorization to manage the chain has not been mocked, \
            please call `MockContractRuntime::set_can_manage_chain` first",
        );

        if authorized {
            self.chain_ownership = Some(ownership);
            Ok(())
        } else {
            Err(ManageChainError::NotPermitted)
        }
    }

    /// Changes the application permissions on the current chain. Returns an error if the
    /// application doesn't have permission to do so.
    pub fn change_application_permissions(
        &mut self,
        application_permissions: ApplicationPermissions,
    ) -> Result<(), ManageChainError> {
        let authorized = self.can_manage_chain.expect(
            "Authorization to manage the chain has not been mocked, \
            please call `MockContractRuntime::set_can_manage_chain` first",
        );

        if authorized {
            let application_id = self
                .application_id
                .expect("The application doesn't have an ID!")
                .forget_abi();
            self.can_manage_chain = Some(application_permissions.can_manage_chain(&application_id));
            Ok(())
        } else {
            Err(ManageChainError::NotPermitted)
        }
    }

    /// Adds an expected call to `open_chain`, and the child chain ID that should be returned.
    pub fn add_expected_open_chain_call(
        &mut self,
        ownership: ChainOwnership,
        application_permissions: ApplicationPermissions,
        balance: Amount,
        chain_id: ChainId,
    ) {
        self.expected_open_chain_calls.push_back((
            ownership,
            application_permissions,
            balance,
            chain_id,
        ));
    }

    /// Opens a new chain, configuring it with the provided `chain_ownership`,
    /// `application_permissions` and initial `balance` (debited from the current chain).
    pub fn open_chain(
        &mut self,
        ownership: ChainOwnership,
        application_permissions: ApplicationPermissions,
        balance: Amount,
    ) -> ChainId {
        let (expected_ownership, expected_permissions, expected_balance, chain_id) = self
            .expected_open_chain_calls
            .pop_front()
            .expect("Unexpected open_chain call");
        assert_eq!(ownership, expected_ownership);
        assert_eq!(application_permissions, expected_permissions);
        assert_eq!(balance, expected_balance);
        chain_id
    }

    /// Adds a new expected call to `publish_module`.
    pub fn add_expected_publish_module_call(
        &mut self,
        contract: Bytecode,
        service: Bytecode,
        vm_runtime: VmRuntime,
        module_id: ModuleId,
    ) {
        self.expected_publish_module_calls
            .push_back(ExpectedPublishModuleCall {
                contract,
                service,
                vm_runtime,
                module_id,
            });
    }

    /// Adds a new expected call to `create_application`.
    pub fn add_expected_create_application_call<Parameters, InstantiationArgument>(
        &mut self,
        module_id: ModuleId,
        parameters: &Parameters,
        argument: &InstantiationArgument,
        required_application_ids: Vec<ApplicationId>,
        application_id: ApplicationId,
    ) where
        Parameters: Serialize,
        InstantiationArgument: Serialize,
    {
        let parameters = serde_json::to_vec(parameters)
            .expect("Failed to serialize `Parameters` type for a cross-application call");
        let argument = serde_json::to_vec(argument).expect(
            "Failed to serialize `InstantiationArgument` type for a cross-application call",
        );
        self.expected_create_application_calls
            .push_back(ExpectedCreateApplicationCall {
                module_id,
                parameters,
                argument,
                required_application_ids,
                application_id,
            });
    }

    /// Adds a new expected call to `create_data_blob`.
    pub fn add_expected_create_data_blob_call(&mut self, bytes: Vec<u8>, blob_id: BlobId) {
        self.expected_create_data_blob_calls
            .push_back(ExpectedCreateDataBlobCall { bytes, blob_id });
    }

    /// Creates a new module-id on-chain application, based on the supplied bytecode and parameters.
    pub fn publish_module(
        &mut self,
        contract: Bytecode,
        service: Bytecode,
        vm_runtime: VmRuntime,
    ) -> ModuleId {
        let ExpectedPublishModuleCall {
            contract: expected_contract,
            service: expected_service,
            vm_runtime: expected_vm_runtime,
            module_id,
        } = self
            .expected_publish_module_calls
            .pop_front()
            .expect("Unexpected publish_module call");
        assert_eq!(contract, expected_contract);
        assert_eq!(service, expected_service);
        assert_eq!(vm_runtime, expected_vm_runtime);
        module_id
    }

    /// Creates a new on-chain application, based on the supplied module and parameters.
    pub fn create_application<Abi, Parameters, InstantiationArgument>(
        &mut self,
        module_id: ModuleId,
        parameters: &Parameters,
        argument: &InstantiationArgument,
        required_application_ids: Vec<ApplicationId>,
    ) -> ApplicationId<Abi>
    where
        Abi: ContractAbi,
        Parameters: Serialize,
        InstantiationArgument: Serialize,
    {
        let ExpectedCreateApplicationCall {
            module_id: expected_module_id,
            parameters: expected_parameters,
            argument: expected_argument,
            required_application_ids: expected_required_app_ids,
            application_id,
        } = self
            .expected_create_application_calls
            .pop_front()
            .expect("Unexpected create_application call");
        let parameters = serde_json::to_vec(parameters)
            .expect("Failed to serialize `Parameters` type for a cross-application call");
        let argument = serde_json::to_vec(argument).expect(
            "Failed to serialize `InstantiationArgument` type for a cross-application call",
        );
        assert_eq!(module_id, expected_module_id);
        assert_eq!(parameters, expected_parameters);
        assert_eq!(argument, expected_argument);
        assert_eq!(required_application_ids, expected_required_app_ids);
        application_id.with_abi::<Abi>()
    }

    /// Creates a new data blob and returns its hash.
    pub fn create_data_blob(&mut self, bytes: &[u8]) -> DataBlobHash {
        let ExpectedCreateDataBlobCall {
            bytes: expected_bytes,
            blob_id,
        } = self
            .expected_create_data_blob_calls
            .pop_front()
            .expect("Unexpected create_data_blob call");
        assert_eq!(bytes, &expected_bytes);
        DataBlobHash(blob_id.hash)
    }

    /// Configures the handler for cross-application calls made during the test.
    pub fn with_call_application_handler(
        mut self,
        handler: impl FnMut(bool, ApplicationId, Vec<u8>) -> Vec<u8> + 'static,
    ) -> Self {
        self.call_application_handler = Some(Box::new(handler));
        self
    }

    /// Configures the handler for cross-application calls made during the test.
    pub fn set_call_application_handler(
        &mut self,
        handler: impl FnMut(bool, ApplicationId, Vec<u8>) -> Vec<u8> + 'static,
    ) -> &mut Self {
        self.call_application_handler = Some(Box::new(handler));
        self
    }

    /// Calls another application.
    pub fn call_application<A: ContractAbi + Send>(
        &mut self,
        authenticated: bool,
        application: ApplicationId<A>,
        call: &A::Operation,
    ) -> A::Response {
        let call_bytes = <A as ContractAbi>::serialize_operation(call)
            .expect("Failed to serialize `Operation` in test runtime cross-application call");

        let handler = self.call_application_handler.as_mut().expect(
            "Handler for `call_application` has not been mocked, \
            please call `MockContractRuntime::set_call_application_handler` first",
        );
        let response_bytes = handler(authenticated, application.forget_abi(), call_bytes);

        A::deserialize_response(response_bytes)
            .expect("Failed to deserialize `Response` in test runtime cross-application call")
    }

    /// Adds a new item to an event stream. Returns the new event's index in the stream.
    pub fn emit(&mut self, name: StreamName, value: &Application::EventValue) -> u32 {
        let value = bcs::to_bytes(value).expect("Failed to serialize event value");
        let entry = self.created_events.entry(name).or_default();
        entry.push(value);
        entry.len() as u32 - 1
    }

    /// Adds an event to a stream, so that it can be read using `read_event`.
    pub fn add_event(&mut self, chain_id: ChainId, name: StreamName, index: u32, value: &[u8]) {
        self.events.insert((chain_id, name, index), value.to_vec());
    }

    /// Reads an event from a stream. Returns the event's value.
    ///
    /// Panics if the event doesn't exist.
    pub fn read_event(
        &mut self,
        chain_id: ChainId,
        name: StreamName,
        index: u32,
    ) -> Application::EventValue {
        let value = self
            .events
            .get(&(chain_id, name, index))
            .expect("Event not found");
        bcs::from_bytes(value).expect("Failed to deserialize event value")
    }

    /// Subscribes this application to an event stream.
    pub fn subscribe_to_events(
        &mut self,
        _chain_id: ChainId,
        _application_id: ApplicationId,
        _name: StreamName,
    ) {
        // This is a no-op in the mock runtime.
    }

    /// Unsubscribes this application from an event stream.
    pub fn unsubscribe_from_events(
        &mut self,
        _chain_id: ChainId,
        _application_id: ApplicationId,
        _name: StreamName,
    ) {
        // This is a no-op in the mock runtime.
    }

    /// Adds an expected `query_service` call`, and the response it should return in the test.
    pub fn add_expected_service_query<A: ServiceAbi + Send>(
        &mut self,
        application_id: ApplicationId<A>,
        query: A::Query,
        response: A::QueryResponse,
    ) {
        let query = serde_json::to_string(&query).expect("Failed to serialize query");
        let response = serde_json::to_string(&response).expect("Failed to serialize response");
        self.expected_service_queries
            .push_back((application_id.forget_abi(), query, response));
    }

    /// Adds an expected `http_request` call, and the response it should return in the test.
    pub fn add_expected_http_request(&mut self, request: http::Request, response: http::Response) {
        self.expected_http_requests.push_back((request, response));
    }

    /// Adds an expected `read_data_blob` call, and the response it should return in the test.
    pub fn add_expected_read_data_blob_requests(&mut self, hash: DataBlobHash, response: Vec<u8>) {
        self.expected_read_data_blob_requests
            .push_back((hash, response));
    }

    /// Adds an expected `assert_data_blob_exists` call, and the response it should return in the test.
    pub fn add_expected_assert_data_blob_exists_requests(
        &mut self,
        hash: DataBlobHash,
        response: Option<()>,
    ) {
        self.expected_assert_data_blob_exists_requests
            .push_back((hash, response));
    }

    /// Adds an expected `has_empty_storage` call, and the response it should return in the test.
    pub fn add_expected_has_empty_storage_requests(
        &mut self,
        application: ApplicationId,
        response: bool,
    ) {
        self.expected_has_empty_storage_requests
            .push_back((application, response));
    }

    /// Queries an application service as an oracle and returns the response.
    ///
    /// Should only be used with queries where it is very likely that all validators will compute
    /// the same result, otherwise most block proposals will fail.
    ///
    /// Cannot be used in fast blocks: A block using this call should be proposed by a regular
    /// owner, not a super owner.
    pub fn query_service<A: ServiceAbi + Send>(
        &mut self,
        application_id: ApplicationId<A>,
        query: &A::Query,
    ) -> A::QueryResponse {
        let maybe_query = self.expected_service_queries.pop_front();
        let (expected_id, expected_query, response) =
            maybe_query.expect("Unexpected service query");
        assert_eq!(application_id.forget_abi(), expected_id);
        let query = serde_json::to_string(query).expect("Failed to serialize query");
        assert_eq!(query, expected_query);
        serde_json::from_str(&response).expect("Failed to deserialize response")
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

    /// Panics if the current time at block validation is `>= timestamp`. Note that block
    /// validation happens at or after the block timestamp, but isn't necessarily the same.
    ///
    /// Cannot be used in fast blocks: A block using this call should be proposed by a regular
    /// owner, not a super owner.
    pub fn assert_before(&mut self, timestamp: Timestamp) {
        assert!(self.timestamp.is_some_and(|t| t < timestamp))
    }

    /// Reads a data blob with the given hash from storage.
    pub fn read_data_blob(&mut self, hash: DataBlobHash) -> Vec<u8> {
        let maybe_request = self.expected_read_data_blob_requests.pop_front();
        let (expected_hash, response) = maybe_request.expect("Unexpected read_data_blob request");
        assert_eq!(hash, expected_hash);
        response
    }

    /// Asserts that a blob with the given hash exists in storage.
    pub fn assert_data_blob_exists(&mut self, hash: DataBlobHash) {
        let maybe_request = self.expected_assert_data_blob_exists_requests.pop_front();
        let (expected_blob_hash, response) =
            maybe_request.expect("Unexpected assert_data_blob_exists request");
        assert_eq!(hash, expected_blob_hash);
        response.expect("Blob does not exist!");
    }

    /// Returns true if the corresponding contract uses a zero amount of storage.
    pub fn has_empty_storage(&mut self, application: ApplicationId) -> bool {
        let maybe_request = self.expected_has_empty_storage_requests.pop_front();
        let (expected_application_id, response) =
            maybe_request.expect("Unexpected has_empty_storage request");
        assert_eq!(application, expected_application_id);
        response
    }

    /// Returns the multi-leader round in which this block was validated.
    pub fn validation_round(&mut self) -> Option<u32> {
        self.round
    }

    /// Configures the remaining fuel to return during the test.
    pub fn with_remaining_fuel(mut self, remaining_fuel: u64) -> Self {
        self.remaining_fuel = Some(remaining_fuel);
        self
    }

    /// Configures the remaining fuel to return during the test.
    pub fn set_remaining_fuel(&mut self, remaining_fuel: u64) -> &mut Self {
        self.remaining_fuel = Some(remaining_fuel);
        self
    }

    /// Returns the amount of execution fuel remaining before execution is aborted.
    pub fn remaining_fuel(&mut self) -> u64 {
        self.remaining_fuel.unwrap_or(u64::MAX)
    }
}

/// A type alias for the handler for cross-application calls.
pub type CallApplicationHandler = Box<dyn FnMut(bool, ApplicationId, Vec<u8>) -> Vec<u8>>;

/// A helper type that uses the builder pattern to configure how a message is sent, and then
/// sends the message once it is dropped.
#[must_use]
pub struct MessageBuilder<Message>
where
    Message: Serialize,
{
    authenticated: bool,
    is_tracked: bool,
    grant: Resources,
    message: Message,
    send_message_requests: Arc<Mutex<Vec<SendMessageRequest<Message>>>>,
}

impl<Message> MessageBuilder<Message>
where
    Message: Serialize,
{
    /// Creates a new [`MessageBuilder`] instance to send the `message` to the `destination`.
    pub(crate) fn new(
        message: Message,
        send_message_requests: Arc<Mutex<Vec<SendMessageRequest<Message>>>>,
    ) -> Self {
        MessageBuilder {
            authenticated: false,
            is_tracked: false,
            grant: Resources::default(),
            message,
            send_message_requests,
        }
    }

    /// Marks the message to be tracked, so that the sender receives the message back if it is
    /// rejected by the receiver.
    pub fn with_tracking(mut self) -> Self {
        self.is_tracked = true;
        self
    }

    /// Forwards the authenticated owner with the message.
    pub fn with_authentication(mut self) -> Self {
        self.authenticated = true;
        self
    }

    /// Forwards a grant of resources so the receiver can use it to pay for receiving the message.
    pub fn with_grant(mut self, grant: Resources) -> Self {
        self.grant = grant;
        self
    }

    /// Schedules this `Message` to be sent to the `destination`.
    pub fn send_to(self, destination: ChainId) {
        let request = SendMessageRequest {
            destination,
            authenticated: self.authenticated,
            is_tracked: self.is_tracked,
            grant: self.grant,
            message: self.message,
        };

        self.send_message_requests
            .try_lock()
            .expect("Unit test should be single-threaded")
            .push(request);
    }
}

/// A claim request that was scheduled to be sent during this test.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClaimRequest {
    source: Account,
    destination: Account,
    amount: Amount,
}
