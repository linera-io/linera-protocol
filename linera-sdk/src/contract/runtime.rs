// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime types to interface with the host executing the contract.

use linera_base::{
    abi::{ContractAbi, ServiceAbi},
    data_types::{
        Amount, ApplicationDescription, ApplicationPermissions, BlockHeight, Bytecode, Resources,
        SendMessageRequest, Timestamp,
    },
    ensure, http,
    identifiers::{
        Account, AccountOwner, ApplicationId, ChainId, DataBlobHash, ModuleId, StreamName,
    },
    ownership::{AccountPermissionError, ChainOwnership, ManageChainError},
    vm::VmRuntime,
};
use serde::Serialize;

use super::wit::{base_runtime_api as base_wit, contract_runtime_api as contract_wit};
use crate::{Contract, KeyValueStore, ViewStorageContext};

/// The common runtime to interface with the host executing the contract.
///
/// It automatically caches read-only values received from the host.
#[derive(Debug)]
pub struct ContractRuntime<Application>
where
    Application: Contract,
{
    application_parameters: Option<Application::Parameters>,
    application_id: Option<ApplicationId<Application::Abi>>,
    application_creator_chain_id: Option<ChainId>,
    chain_id: Option<ChainId>,
    block_height: Option<BlockHeight>,
    message_is_bouncing: Option<Option<bool>>,
    message_origin_chain_id: Option<Option<ChainId>>,
    timestamp: Option<Timestamp>,
}

impl<Application> ContractRuntime<Application>
where
    Application: Contract,
{
    /// Creates a new [`ContractRuntime`] instance for a contract.
    pub(crate) fn new() -> Self {
        ContractRuntime {
            application_parameters: None,
            application_id: None,
            application_creator_chain_id: None,
            chain_id: None,
            block_height: None,
            message_is_bouncing: None,
            message_origin_chain_id: None,
            timestamp: None,
        }
    }

    /// Returns the key-value store to interface with storage.
    pub fn key_value_store(&self) -> KeyValueStore {
        KeyValueStore::for_contracts()
    }

    /// Returns a storage context suitable for a root view.
    pub fn root_view_storage_context(&self) -> ViewStorageContext {
        ViewStorageContext::new_unchecked(self.key_value_store(), Vec::new(), ())
    }
}

impl<Application> ContractRuntime<Application>
where
    Application: Contract,
{
    /// Returns the application parameters provided when the application was created.
    pub fn application_parameters(&mut self) -> Application::Parameters {
        self.application_parameters
            .get_or_insert_with(|| {
                let bytes = base_wit::application_parameters();
                serde_json::from_slice(&bytes)
                    .expect("Application parameters must be deserializable")
            })
            .clone()
    }

    /// Returns the ID of the current application.
    pub fn application_id(&mut self) -> ApplicationId<Application::Abi> {
        *self
            .application_id
            .get_or_insert_with(|| ApplicationId::from(base_wit::get_application_id()).with_abi())
    }

    /// Returns the chain ID of the current application creator.
    pub fn application_creator_chain_id(&mut self) -> ChainId {
        *self
            .application_creator_chain_id
            .get_or_insert_with(|| base_wit::get_application_creator_chain_id().into())
    }

    /// Returns the description of the given application.
    pub fn read_application_description(
        &mut self,
        application_id: ApplicationId,
    ) -> ApplicationDescription {
        base_wit::read_application_description(application_id.forget_abi().into()).into()
    }

    /// Returns the ID of the current chain.
    pub fn chain_id(&mut self) -> ChainId {
        *self
            .chain_id
            .get_or_insert_with(|| base_wit::get_chain_id().into())
    }

    /// Returns the height of the current block that is executing.
    pub fn block_height(&mut self) -> BlockHeight {
        *self
            .block_height
            .get_or_insert_with(|| base_wit::get_block_height().into())
    }

    /// Retrieves the current system time, i.e. the timestamp of the block in which this is called.
    pub fn system_time(&mut self) -> Timestamp {
        *self
            .timestamp
            .get_or_insert_with(|| base_wit::read_system_timestamp().into())
    }

    /// Returns the current chain balance.
    pub fn chain_balance(&mut self) -> Amount {
        base_wit::read_chain_balance().into()
    }

    /// Returns the balance of one of the accounts on this chain.
    pub fn owner_balance(&mut self, owner: AccountOwner) -> Amount {
        base_wit::read_owner_balance(owner.into()).into()
    }

    /// Retrieves the owner configuration for the current chain.
    pub fn chain_ownership(&mut self) -> ChainOwnership {
        base_wit::get_chain_ownership().into()
    }

    /// Retrieves the application permissions for the current chain.
    pub fn application_permissions(&mut self) -> ApplicationPermissions {
        base_wit::get_application_permissions().into()
    }

    /// Makes an HTTP `request` as an oracle and returns the HTTP response.
    ///
    /// Should only be used with queries where it is very likely that all validators will receive
    /// the same response, otherwise most block proposals will fail.
    ///
    /// Cannot be used in fast blocks: A block using this call should be proposed by a regular
    /// owner, not a super owner.
    pub fn http_request(&mut self, request: http::Request) -> http::Response {
        base_wit::perform_http_request(&request.into()).into()
    }

    /// Panics if the current time at block validation is `>= timestamp`. Note that block
    /// validation happens at or after the block timestamp, but isn't necessarily the same.
    ///
    /// Cannot be used in fast blocks: A block using this call should be proposed by a regular
    /// owner, not a super owner.
    pub fn assert_before(&mut self, timestamp: Timestamp) {
        base_wit::assert_before(timestamp.into());
    }

    /// Reads a data blob with the given hash from storage.
    pub fn read_data_blob(&mut self, hash: DataBlobHash) -> Vec<u8> {
        base_wit::read_data_blob(hash.into())
    }

    /// Asserts that a data blob with the given hash exists in storage.
    pub fn assert_data_blob_exists(&mut self, hash: DataBlobHash) {
        base_wit::assert_data_blob_exists(hash.into())
    }

    /// Returns the amount of execution fuel remaining before execution is aborted.
    pub fn remaining_fuel(&mut self) -> u64 {
        contract_wit::remaining_fuel()
    }

    /// Returns true if the corresponding contract uses a zero amount of storage.
    pub fn has_empty_storage(&mut self, application: ApplicationId) -> bool {
        contract_wit::has_empty_storage(application.into())
    }
}

impl<Application> ContractRuntime<Application>
where
    Application: Contract,
{
    /// Returns the authenticated owner for this execution, if there is one.
    pub fn authenticated_owner(&mut self) -> Option<AccountOwner> {
        contract_wit::authenticated_owner().map(AccountOwner::from)
    }

    /// Returns [`true`] if the incoming message was rejected from the original destination and is
    /// now bouncing back, or [`None`] if not executing an incoming message.
    pub fn message_is_bouncing(&mut self) -> Option<bool> {
        *self
            .message_is_bouncing
            .get_or_insert_with(contract_wit::message_is_bouncing)
    }

    /// Returns the chain ID where the incoming message originated from, or [`None`] if not executing
    /// an incoming message.
    pub fn message_origin_chain_id(&mut self) -> Option<ChainId> {
        *self
            .message_origin_chain_id
            .get_or_insert_with(|| contract_wit::message_origin_chain_id().map(ChainId::from))
    }

    /// Returns the authenticated caller ID, if the caller configured it and if the current context
    /// is executing a cross-application call.
    pub fn authenticated_caller_id(&mut self) -> Option<ApplicationId> {
        contract_wit::authenticated_caller_id().map(ApplicationId::from)
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

    /// Schedules a message to be sent to this application on another chain.
    pub fn send_message(&mut self, destination: ChainId, message: Application::Message) {
        self.prepare_message(message).send_to(destination)
    }

    /// Returns a `MessageBuilder` to prepare a message to be sent.
    pub fn prepare_message(
        &mut self,
        message: Application::Message,
    ) -> MessageBuilder<Application::Message> {
        MessageBuilder::new(message)
    }

    /// Transfers an `amount` of native tokens from `source` owner account (or the current chain's
    /// balance) to `destination`.
    pub fn transfer(&mut self, source: AccountOwner, destination: Account, amount: Amount) {
        contract_wit::transfer(source.into(), destination.into(), amount.into())
    }

    /// Claims an `amount` of native tokens from a `source` account to a `destination` account.
    pub fn claim(&mut self, source: Account, destination: Account, amount: Amount) {
        contract_wit::claim(source.into(), destination.into(), amount.into())
    }

    /// Calls another application.
    pub fn call_application<A: ContractAbi + Send>(
        &mut self,
        authenticated: bool,
        application: ApplicationId<A>,
        call: &A::Operation,
    ) -> A::Response {
        let call_bytes = <A as ContractAbi>::serialize_operation(call)
            .expect("Failed to serialize `Operation` in cross-application call");

        let response_bytes = contract_wit::try_call_application(
            authenticated,
            application.forget_abi().into(),
            &call_bytes,
        );

        A::deserialize_response(response_bytes)
            .expect("Failed to deserialize `Response` in cross-application call")
    }

    /// Adds a new item to an event stream. Returns the new event's index in the stream.
    pub fn emit(&mut self, name: StreamName, value: &Application::EventValue) -> u32 {
        contract_wit::emit(
            &name.into(),
            &bcs::to_bytes(value).expect("Failed to serialize event"),
        )
    }

    /// Reads an event from a stream. Returns the event's value.
    ///
    /// Fails the block if the event doesn't exist.
    pub fn read_event(
        &mut self,
        chain_id: ChainId,
        name: StreamName,
        index: u32,
    ) -> Application::EventValue {
        let event = contract_wit::read_event(chain_id.into(), &name.into(), index);
        bcs::from_bytes(&event).expect("Failed to deserialize event")
    }

    /// Subscribes this application to an event stream.
    pub fn subscribe_to_events(
        &mut self,
        chain_id: ChainId,
        application_id: ApplicationId,
        name: StreamName,
    ) {
        contract_wit::subscribe_to_events(chain_id.into(), application_id.into(), &name.into())
    }

    /// Unsubscribes this application from an event stream.
    pub fn unsubscribe_from_events(
        &mut self,
        chain_id: ChainId,
        application_id: ApplicationId,
        name: StreamName,
    ) {
        contract_wit::unsubscribe_from_events(chain_id.into(), application_id.into(), &name.into())
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
        let query = serde_json::to_vec(query).expect("Failed to serialize service query");
        let response = contract_wit::query_service(application_id.forget_abi().into(), &query);
        serde_json::from_slice(&response).expect("Failed to deserialize service response")
    }

    /// Opens a new chain, configuring it with the provided `chain_ownership`,
    /// `application_permissions` and initial `balance` (debited from the current chain).
    pub fn open_chain(
        &mut self,
        chain_ownership: ChainOwnership,
        application_permissions: ApplicationPermissions,
        balance: Amount,
    ) -> ChainId {
        let chain_id = contract_wit::open_chain(
            &chain_ownership.into(),
            &application_permissions.into(),
            balance.into(),
        );
        chain_id.into()
    }

    /// Closes the current chain. Returns an error if the application doesn't have
    /// permission to do so.
    pub fn close_chain(&mut self) -> Result<(), ManageChainError> {
        contract_wit::close_chain().map_err(|error| error.into())
    }

    /// Changes the ownership of the current chain. Returns an error if the application doesn't
    /// have permission to do so.
    pub fn change_ownership(&mut self, ownership: ChainOwnership) -> Result<(), ManageChainError> {
        contract_wit::change_ownership(&ownership.into()).map_err(|error| error.into())
    }

    /// Changes the application permissions for the current chain.
    pub fn change_application_permissions(
        &mut self,
        application_permissions: ApplicationPermissions,
    ) -> Result<(), ManageChainError> {
        contract_wit::change_application_permissions(&application_permissions.into())
            .map_err(|error| error.into())
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
        let parameters = serde_json::to_vec(parameters)
            .expect("Failed to serialize `Parameters` type for a cross-application call");
        let argument = serde_json::to_vec(argument).expect(
            "Failed to serialize `InstantiationArgument` type for a cross-application call",
        );
        let converted_application_ids: Vec<_> = required_application_ids
            .into_iter()
            .map(From::from)
            .collect();
        let application_id = contract_wit::create_application(
            module_id.into(),
            &parameters,
            &argument,
            &converted_application_ids,
        );
        ApplicationId::from(application_id).with_abi::<Abi>()
    }

    /// Creates a new data blob and returns its hash.
    pub fn create_data_blob(&mut self, bytes: &[u8]) -> DataBlobHash {
        let hash = contract_wit::create_data_blob(bytes);
        hash.into()
    }

    /// Publishes a module with contract and service bytecode and returns the module ID.
    pub fn publish_module(
        &mut self,
        contract: Bytecode,
        service: Bytecode,
        vm_runtime: VmRuntime,
    ) -> ModuleId {
        contract_wit::publish_module(&contract.into(), &service.into(), vm_runtime.into()).into()
    }

    /// Returns the multi-leader round in which this block was validated.
    pub fn validation_round(&mut self) -> Option<u32> {
        contract_wit::validation_round()
    }
}

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
}

impl<Message> MessageBuilder<Message>
where
    Message: Serialize,
{
    /// Creates a new [`MessageBuilder`] instance to send the `message` to the `destination`.
    pub(crate) fn new(message: Message) -> Self {
        MessageBuilder {
            authenticated: false,
            is_tracked: false,
            grant: Resources::default(),
            message,
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
        let serialized_message =
            bcs::to_bytes(&self.message).expect("Failed to serialize message to be sent");

        let raw_message = SendMessageRequest {
            destination,
            authenticated: self.authenticated,
            is_tracked: self.is_tracked,
            grant: self.grant,
            message: serialized_message,
        };

        contract_wit::send_message(&raw_message.into())
    }
}
