// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime types to interface with the host executing the contract.

use linera_base::{
    abi::{ContractAbi, ServiceAbi},
    data_types::{
        Amount, ApplicationPermissions, BlockHeight, Resources, SendMessageRequest, Timestamp,
    },
    http,
    identifiers::{
        Account, AccountOwner, ApplicationId, BytecodeId, ChainId, ChannelName, Destination,
        MessageId, Owner, StreamName,
    },
    ownership::{ChainOwnership, ChangeApplicationPermissionsError, CloseChainError},
};
use serde::Serialize;

use super::wit::contract_system_api as wit;
use crate::{Contract, DataBlobHash, KeyValueStore, ViewStorageContext};

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
    authenticated_signer: Option<Option<Owner>>,
    block_height: Option<BlockHeight>,
    message_is_bouncing: Option<Option<bool>>,
    message_id: Option<Option<MessageId>>,
    authenticated_caller_id: Option<Option<ApplicationId>>,
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
            authenticated_signer: None,
            block_height: None,
            message_is_bouncing: None,
            message_id: None,
            authenticated_caller_id: None,
            timestamp: None,
        }
    }

    /// Returns the key-value store to interface with storage.
    pub fn key_value_store(&self) -> KeyValueStore {
        KeyValueStore::for_contracts()
    }

    /// Returns a storage context suitable for a root view.
    pub fn root_view_storage_context(&self) -> ViewStorageContext {
        ViewStorageContext::new_unsafe(self.key_value_store(), Vec::new(), ())
    }

    /// Returns the application parameters provided when the application was created.
    pub fn application_parameters(&mut self) -> Application::Parameters {
        self.application_parameters
            .get_or_insert_with(|| {
                let bytes = wit::application_parameters();
                serde_json::from_slice(&bytes)
                    .expect("Application parameters must be deserializable")
            })
            .clone()
    }

    /// Returns the ID of the current application.
    pub fn application_id(&mut self) -> ApplicationId<Application::Abi> {
        *self
            .application_id
            .get_or_insert_with(|| ApplicationId::from(wit::get_application_id()).with_abi())
    }

    /// Returns the chain ID of the current application creator.
    pub fn application_creator_chain_id(&mut self) -> ChainId {
        *self
            .application_creator_chain_id
            .get_or_insert_with(|| wit::get_application_creator_chain_id().into())
    }

    /// Returns the ID of the current chain.
    pub fn chain_id(&mut self) -> ChainId {
        *self
            .chain_id
            .get_or_insert_with(|| wit::get_chain_id().into())
    }

    /// Returns the authenticated signer for this execution, if there is one.
    pub fn authenticated_signer(&mut self) -> Option<Owner> {
        *self
            .authenticated_signer
            .get_or_insert_with(|| wit::authenticated_signer().map(Owner::from))
    }

    /// Returns the height of the current block that is executing.
    pub fn block_height(&mut self) -> BlockHeight {
        *self
            .block_height
            .get_or_insert_with(|| wit::get_block_height().into())
    }

    /// Returns the ID of the incoming message that is being handled, or [`None`] if not executing
    /// an incoming message.
    pub fn message_id(&mut self) -> Option<MessageId> {
        *self
            .message_id
            .get_or_insert_with(|| wit::get_message_id().map(MessageId::from))
    }

    /// Returns [`true`] if the incoming message was rejected from the original destination and is
    /// now bouncing back, or [`None`] if not executing an incoming message.
    pub fn message_is_bouncing(&mut self) -> Option<bool> {
        *self
            .message_is_bouncing
            .get_or_insert_with(wit::message_is_bouncing)
    }

    /// Returns the authenticated caller ID, if the caller configured it and if the current context
    /// is executing a cross-application call.
    pub fn authenticated_caller_id(&mut self) -> Option<ApplicationId> {
        *self
            .authenticated_caller_id
            .get_or_insert_with(|| wit::authenticated_caller_id().map(ApplicationId::from))
    }

    /// Retrieves the current system time, i.e. the timestamp of the block in which this is called.
    pub fn system_time(&mut self) -> Timestamp {
        *self
            .timestamp
            .get_or_insert_with(|| wit::read_system_timestamp().into())
    }

    /// Returns the current chain balance.
    pub fn chain_balance(&mut self) -> Amount {
        wit::read_chain_balance().into()
    }

    /// Returns the balance of one of the accounts on this chain.
    pub fn owner_balance(&mut self, owner: AccountOwner) -> Amount {
        wit::read_owner_balance(owner.into()).into()
    }

    /// Schedules a message to be sent to this application on another chain.
    pub fn send_message(
        &mut self,
        destination: impl Into<Destination>,
        message: Application::Message,
    ) {
        self.prepare_message(message).send_to(destination)
    }

    /// Returns a `MessageBuilder` to prepare a message to be sent.
    pub fn prepare_message(
        &mut self,
        message: Application::Message,
    ) -> MessageBuilder<Application::Message> {
        MessageBuilder::new(message)
    }

    /// Subscribes to a message channel from another chain.
    pub fn subscribe(&mut self, chain: ChainId, channel: ChannelName) {
        wit::subscribe(chain.into(), &channel.into());
    }

    /// Unsubscribes to a message channel from another chain.
    pub fn unsubscribe(&mut self, chain: ChainId, channel: ChannelName) {
        wit::unsubscribe(chain.into(), &channel.into());
    }

    /// Transfers an `amount` of native tokens from `source` owner account (or the current chain's
    /// balance) to `destination`.
    pub fn transfer(&mut self, source: Option<AccountOwner>, destination: Account, amount: Amount) {
        wit::transfer(
            source.map(|source| source.into()),
            destination.into(),
            amount.into(),
        )
    }

    /// Claims an `amount` of native tokens from a `source` account to a `destination` account.
    pub fn claim(&mut self, source: Account, destination: Account, amount: Amount) {
        wit::claim(source.into(), destination.into(), amount.into())
    }

    /// Retrieves the owner configuration for the current chain.
    pub fn chain_ownership(&mut self) -> ChainOwnership {
        wit::get_chain_ownership().into()
    }

    /// Closes the current chain. Returns an error if the application doesn't have
    /// permission to do so.
    pub fn close_chain(&mut self) -> Result<(), CloseChainError> {
        wit::close_chain().map_err(|error| error.into())
    }

    /// Opens a new chain, configuring it with the provided `chain_ownership`,
    /// `application_permissions` and initial `balance` (debited from the current chain).
    pub fn open_chain(
        &mut self,
        chain_ownership: ChainOwnership,
        application_permissions: ApplicationPermissions,
        balance: Amount,
    ) -> (MessageId, ChainId) {
        let (message_id, chain_id) = wit::open_chain(
            &chain_ownership.into(),
            &application_permissions.into(),
            balance.into(),
        );
        (message_id.into(), chain_id.into())
    }

    /// Changes the application permissions for the current chain.
    pub fn change_application_permissions(
        &mut self,
        application_permissions: ApplicationPermissions,
    ) -> Result<(), ChangeApplicationPermissionsError> {
        wit::change_application_permissions(&application_permissions.into())
            .map_err(|error| error.into())
    }

    /// Creates a new on-chain application, based on the supplied bytecode and parameters.
    pub fn create_application<Abi, Parameters, InstantiationArgument>(
        &mut self,
        bytecode_id: BytecodeId,
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
        let application_id = wit::create_application(
            bytecode_id.into(),
            &parameters,
            &argument,
            &converted_application_ids,
        );
        ApplicationId::from(application_id).with_abi::<Abi>()
    }

    /// Calls another application.
    pub fn call_application<A: ContractAbi + Send>(
        &mut self,
        authenticated: bool,
        application: ApplicationId<A>,
        call: &A::Operation,
    ) -> A::Response {
        let call_bytes = bcs::to_bytes(call)
            .expect("Failed to serialize `Operation` type for a cross-application call");

        let response_bytes =
            wit::try_call_application(authenticated, application.forget_abi().into(), &call_bytes);

        bcs::from_bytes(&response_bytes)
            .expect("Failed to deserialize `Response` type from cross-application call")
    }

    /// Adds a new item to an event stream.
    pub fn emit(&mut self, name: StreamName, key: &[u8], value: &[u8]) {
        wit::emit(&name.into(), key, value);
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
        query: A::Query,
    ) -> A::QueryResponse {
        let query = serde_json::to_vec(&query).expect("Failed to serialize service query");
        let response = wit::query_service(application_id.forget_abi().into(), &query);
        serde_json::from_slice(&response).expect("Failed to deserialize service response")
    }

    /// Makes an HTTP request to the given URL as an oracle and returns the answer, if any.
    ///
    /// Should only be used with queries where it is very likely that all validators will receive
    /// the same response, otherwise most block proposals will fail.
    ///
    /// Cannot be used in fast blocks: A block using this call should be proposed by a regular
    /// owner, not a super owner.
    pub fn http_request(
        &mut self,
        method: http::Method,
        url: &str,
        headers: Vec<(String, Vec<u8>)>,
        payload: Vec<u8>,
    ) -> Vec<u8> {
        wit::perform_http_request(method.into(), url, &headers, &payload)
    }

    /// Panics if the current time at block validation is `>= timestamp`. Note that block
    /// validation happens at or after the block timestamp, but isn't necessarily the same.
    ///
    /// Cannot be used in fast blocks: A block using this call should be proposed by a regular
    /// owner, not a super owner.
    pub fn assert_before(&mut self, timestamp: Timestamp) {
        wit::assert_before(timestamp.into());
    }

    /// Reads a data blob with the given hash from storage.
    pub fn read_data_blob(&mut self, hash: DataBlobHash) -> Vec<u8> {
        wit::read_data_blob(hash.0.into())
    }

    /// Asserts that a data blob with the given hash exists in storage.
    pub fn assert_data_blob_exists(&mut self, hash: DataBlobHash) {
        wit::assert_data_blob_exists(hash.0.into())
    }

    /// Returns the round in which this block was validated.
    pub fn validation_round(&mut self) -> Option<u32> {
        wit::validation_round()
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

    /// Forwards the authenticated signer with the message.
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
    pub fn send_to(self, destination: impl Into<Destination>) {
        let serialized_message =
            bcs::to_bytes(&self.message).expect("Failed to serialize message to be sent");

        let raw_message = SendMessageRequest {
            destination: destination.into(),
            authenticated: self.authenticated,
            is_tracked: self.is_tracked,
            grant: self.grant,
            message: serialized_message,
        };

        wit::send_message(&raw_message.into())
    }
}
