// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime types to simulate interfacing with the host executing the contract.

use linera_base::{
    abi::ContractAbi,
    data_types::{Amount, BlockHeight, Resources, Timestamp},
    identifiers::{Account, ApplicationId, ChainId, ChannelName, Destination, MessageId, Owner},
    ownership::{ChainOwnership, CloseChainError},
};
use serde::Serialize;

use crate::Contract;

/// A mock of the common runtime to interface with the host executing the contract.
pub struct MockContractRuntime<Application>
where
    Application: Contract,
{
    application_parameters: Option<Application::Parameters>,
    application_id: Option<ApplicationId<Application::Abi>>,
    chain_id: Option<ChainId>,
    authenticated_signer: Option<Option<Owner>>,
    block_height: Option<BlockHeight>,
    message_id: Option<Option<MessageId>>,
    message_is_bouncing: Option<Option<bool>>,
    authenticated_caller_id: Option<Option<ApplicationId>>,
    timestamp: Option<Timestamp>,
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
            chain_id: None,
            authenticated_signer: None,
            block_height: None,
            message_id: None,
            message_is_bouncing: None,
            authenticated_caller_id: None,
            timestamp: None,
        }
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

    /// Configures the authenticated signer to return during the test.
    pub fn with_authenticated_signer(
        mut self,
        authenticated_signer: impl Into<Option<Owner>>,
    ) -> Self {
        self.authenticated_signer = Some(authenticated_signer.into());
        self
    }

    /// Configures the authenticated signer to return during the test.
    pub fn set_authenticated_signer(
        &mut self,
        authenticated_signer: impl Into<Option<Owner>>,
    ) -> &mut Self {
        self.authenticated_signer = Some(authenticated_signer.into());
        self
    }

    /// Returns the authenticated signer for this execution, if there is one.
    pub fn authenticated_signer(&mut self) -> Option<Owner> {
        self.authenticated_signer.expect(
            "Authenticated signer has not been mocked, \
            please call `MockContractRuntime::set_authenticated_signer` first",
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

    /// Returns the height of the current block that is executing.
    pub fn block_height(&mut self) -> BlockHeight {
        self.block_height.expect(
            "Block height has not been mocked, \
            please call `MockContractRuntime::set_block_height` first",
        )
    }

    /// Configures the message ID to return during the test.
    pub fn with_message_id(mut self, message_id: impl Into<Option<MessageId>>) -> Self {
        self.message_id = Some(message_id.into());
        self
    }

    /// Configures the message ID to return during the test.
    pub fn set_message_id(&mut self, message_id: impl Into<Option<MessageId>>) -> &mut Self {
        self.message_id = Some(message_id.into());
        self
    }

    /// Returns the ID of the incoming message that is being handled, or [`None`] if not executing
    /// an incoming message.
    pub fn message_id(&mut self) -> Option<MessageId> {
        self.message_id.expect(
            "Message ID has not been mocked, \
            please call `MockContractRuntime::set_message_id` first",
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

    /// Returns the current chain balance.
    pub fn chain_balance(&mut self) -> Amount {
        todo!();
    }

    /// Returns the balance of one of the accounts on this chain.
    pub fn owner_balance(&mut self, _owner: Owner) -> Amount {
        todo!();
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
    pub fn subscribe(&mut self, _chain: ChainId, _channel: ChannelName) {
        todo!();
    }

    /// Unsubscribes to a message channel from another chain.
    pub fn unsubscribe(&mut self, _chain: ChainId, _channel: ChannelName) {
        todo!();
    }

    /// Transfers an `amount` of native tokens from `source` owner account (or the current chain's
    /// balance) to `destination`.
    pub fn transfer(&mut self, _source: Option<Owner>, _destination: Account, _amount: Amount) {
        todo!();
    }

    /// Claims an `amount` of native tokens from a `source` account to a `destination` account.
    pub fn claim(&mut self, _source: Account, _destination: Account, _amount: Amount) {
        todo!();
    }

    /// Retrieves the owner configuration for the current chain.
    pub fn chain_ownership(&mut self) -> ChainOwnership {
        todo!();
    }

    /// Closes the current chain. Returns an error if the application doesn't have
    /// permission to do so.
    pub fn close_chain(&mut self) -> Result<(), CloseChainError> {
        todo!();
    }

    /// Calls another application.
    pub fn call_application<A: ContractAbi + Send>(
        &mut self,
        _authenticated: bool,
        _application: ApplicationId<A>,
        _call: &A::Operation,
    ) -> A::Response {
        todo!();
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
    pub fn send_to(self, _destination: impl Into<Destination>) {
        todo!();
    }
}
