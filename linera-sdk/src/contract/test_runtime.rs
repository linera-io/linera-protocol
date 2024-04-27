// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Runtime types to simulate interfacing with the host executing the contract.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use linera_base::{
    abi::ContractAbi,
    data_types::{Amount, BlockHeight, Resources, SendMessageRequest, Timestamp},
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
    chain_balance: Option<Amount>,
    owner_balances: Option<HashMap<Owner, Amount>>,
    chain_ownership: Option<ChainOwnership>,
    can_close_chain: Option<bool>,
    call_application_handler: Option<CallApplicationHandler>,
    send_message_requests: Arc<Mutex<Vec<SendMessageRequest<Application::Message>>>>,
    subscribe_requests: Vec<(ChainId, ChannelName)>,
    unsubscribe_requests: Vec<(ChainId, ChannelName)>,
    outgoing_transfers: HashMap<Account, Amount>,
    claim_requests: Vec<ClaimRequest>,
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
            chain_balance: None,
            owner_balances: None,
            chain_ownership: None,
            can_close_chain: None,
            call_application_handler: None,
            send_message_requests: Arc::default(),
            subscribe_requests: Vec::new(),
            unsubscribe_requests: Vec::new(),
            outgoing_transfers: HashMap::new(),
            claim_requests: Vec::new(),
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
        owner_balances: impl IntoIterator<Item = (Owner, Amount)>,
    ) -> Self {
        self.owner_balances = Some(owner_balances.into_iter().collect());
        self
    }

    /// Configures the balances on the chain to use during the test.
    pub fn set_owner_balances(
        &mut self,
        owner_balances: impl IntoIterator<Item = (Owner, Amount)>,
    ) -> &mut Self {
        self.owner_balances = Some(owner_balances.into_iter().collect());
        self
    }

    /// Configures the balance of one account on the chain to use during the test.
    pub fn with_owner_balance(mut self, owner: Owner, balance: Amount) -> Self {
        self.set_owner_balance(owner, balance);
        self
    }

    /// Configures the balance of one account on the chain to use during the test.
    pub fn set_owner_balance(&mut self, owner: Owner, balance: Amount) -> &mut Self {
        self.owner_balances
            .get_or_insert_with(HashMap::new)
            .insert(owner, balance);
        self
    }

    /// Returns the balance of one of the accounts on this chain.
    pub fn owner_balance(&mut self, owner: Owner) -> Amount {
        *self.owner_balance_mut(owner)
    }

    /// Returns a mutable reference to the balance of one of the accounts on this chain.
    fn owner_balance_mut(&mut self, owner: Owner) -> &mut Amount {
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

    /// Subscribes to a message channel from another chain.
    pub fn subscribe(&mut self, chain: ChainId, channel: ChannelName) {
        self.subscribe_requests.push((chain, channel));
    }

    /// Returns the list of requests to subscribe to channels made in the test so far.
    pub fn subscribe_requests(&self) -> &[(ChainId, ChannelName)] {
        &self.subscribe_requests
    }

    /// Unsubscribes to a message channel from another chain.
    pub fn unsubscribe(&mut self, chain: ChainId, channel: ChannelName) {
        self.unsubscribe_requests.push((chain, channel));
    }

    /// Returns the list of requests to unsubscribe to channels made in the test so far.
    pub fn unsubscribe_requests(&self) -> &[(ChainId, ChannelName)] {
        &self.unsubscribe_requests
    }

    /// Transfers an `amount` of native tokens from `source` owner account (or the current chain's
    /// balance) to `destination`.
    pub fn transfer(&mut self, source: Option<Owner>, destination: Account, amount: Amount) {
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
    fn debit(&mut self, source: Option<Owner>, amount: Amount) {
        let source_balance = match source {
            Some(owner) => self.owner_balance_mut(owner),
            None => self.chain_balance_mut(),
        };

        *source_balance = source_balance
            .try_sub(amount)
            .expect("Insufficient funds in source account");
    }

    /// Credits an `amount` of native tokens into a `destination` owner account (or the
    /// current chain's balance).
    fn credit(&mut self, destination: Option<Owner>, amount: Amount) {
        let destination_balance = match destination {
            Some(owner) => self.owner_balance_mut(owner),
            None => self.chain_balance_mut(),
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

    /// Configures if the application being tested is allowed to close the chain its in.
    pub fn with_can_close_chain(mut self, can_close_chain: bool) -> Self {
        self.can_close_chain = Some(can_close_chain);
        self
    }

    /// Configures if the application being tested is allowed to close the chain its in.
    pub fn set_can_close_chain(&mut self, can_close_chain: bool) -> &mut Self {
        self.can_close_chain = Some(can_close_chain);
        self
    }

    /// Closes the current chain. Returns an error if the application doesn't have
    /// permission to do so.
    pub fn close_chain(&mut self) -> Result<(), CloseChainError> {
        let authorized = self.can_close_chain.expect(
            "Authorization to close the chain has not been mocked, \
            please call `MockContractRuntime::set_can_close_chain` first",
        );

        if authorized {
            Ok(())
        } else {
            Err(CloseChainError::NotPermitted)
        }
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
        let call_bytes = bcs::to_bytes(call)
            .expect("Failed to serialize `Operation` type for a cross-application call");

        let handler = self.call_application_handler.as_mut().expect(
            "Handler for `call_application` has not been mocked, \
            please call `MockContractRuntime::set_call_application_handler` first",
        );
        let response_bytes = handler(authenticated, application.forget_abi(), call_bytes);

        bcs::from_bytes(&response_bytes)
            .expect("Failed to deserialize `Response` type from cross-application call")
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
        let request = SendMessageRequest {
            destination: destination.into(),
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
