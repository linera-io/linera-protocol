// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_metrics)]
use std::sync::LazyLock;
use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
    iter,
};

use async_graphql::Enum;
use custom_debug_derive::Debug;
use linera_base::{
    crypto::{CryptoHash, PublicKey},
    data_types::{
        Amount, ApplicationPermissions, ArithmeticError, BlobContent, OracleResponse, Timestamp,
    },
    ensure, hex_debug,
    identifiers::{Account, BlobId, BytecodeId, ChainDescription, ChainId, MessageId, Owner},
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_views::{
    common::Context,
    map_view::HashedMapView,
    register_view::HashedRegisterView,
    set_view::HashedSetView,
    views::{ClonableView, HashableView, View, ViewError},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
#[cfg(with_metrics)]
use {linera_base::prometheus_util, prometheus::IntCounterVec};

#[cfg(test)]
use crate::test_utils::SystemExecutionState;
use crate::{
    committee::{Committee, Epoch},
    ApplicationRegistryView, Bytecode, BytecodeLocation, ChannelName, ChannelSubscription,
    Destination, ExecutionRuntimeContext, MessageContext, MessageKind, OperationContext,
    QueryContext, RawExecutionOutcome, RawOutgoingMessage, TransactionTracker,
    UserApplicationDescription, UserApplicationId,
};

/// The relative index of the `OpenChain` message created by the `OpenChain` operation.
pub static OPEN_CHAIN_MESSAGE_INDEX: u32 = 0;
/// The relative index of the `ApplicationCreated` message created by the `CreateApplication`
/// operation.
pub static CREATE_APPLICATION_MESSAGE_INDEX: u32 = 0;
/// The relative index of the `BytecodePublished` message created by the `PublishBytecode`
/// operation.
pub static PUBLISH_BYTECODE_MESSAGE_INDEX: u32 = 0;

/// The number of times the [`SystemOperation::OpenChain`] was executed.
#[cfg(with_metrics)]
static OPEN_CHAIN_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    prometheus_util::register_int_counter_vec(
        "open_chain_count",
        "The number of times the `OpenChain` operation was executed",
        &[],
    )
    .expect("Counter creation should not fail")
});

/// A view accessing the execution state of the system of a chain.
#[derive(Debug, ClonableView, HashableView)]
pub struct SystemExecutionStateView<C> {
    /// How the chain was created. May be unknown for inactive chains.
    pub description: HashedRegisterView<C, Option<ChainDescription>>,
    /// The number identifying the current configuration.
    pub epoch: HashedRegisterView<C, Option<Epoch>>,
    /// The admin of the chain.
    pub admin_id: HashedRegisterView<C, Option<ChainId>>,
    /// Track the channels that we have subscribed to.
    pub subscriptions: HashedSetView<C, ChannelSubscription>,
    /// The committees that we trust, indexed by epoch number.
    // Not using a `MapView` because the set active of committees is supposed to be
    // small. Plus, currently, we would create the `BTreeMap` anyway in various places
    // (e.g. the `OpenChain` operation).
    pub committees: HashedRegisterView<C, BTreeMap<Epoch, Committee>>,
    /// Ownership of the chain.
    pub ownership: HashedRegisterView<C, ChainOwnership>,
    /// Balance of the chain. (Available to any user able to create blocks in the chain.)
    pub balance: HashedRegisterView<C, Amount>,
    /// Balances attributed to a given owner.
    pub balances: HashedMapView<C, Owner, Amount>,
    /// The timestamp of the most recent block.
    pub timestamp: HashedRegisterView<C, Timestamp>,
    /// Track the locations of known bytecodes as well as the descriptions of known applications.
    pub registry: ApplicationRegistryView<C>,
    /// Whether this chain has been closed.
    pub closed: HashedRegisterView<C, bool>,
    /// Permissions for applications on this chain.
    pub application_permissions: HashedRegisterView<C, ApplicationPermissions>,
}

/// The configuration for a new chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct OpenChainConfig {
    pub ownership: ChainOwnership,
    pub admin_id: ChainId,
    pub epoch: Epoch,
    pub committees: BTreeMap<Epoch, Committee>,
    pub balance: Amount,
    pub application_permissions: ApplicationPermissions,
}

/// A system operation.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum SystemOperation {
    /// Transfers `amount` units of value from the given owner's account to the recipient.
    /// If no owner is given, try to take the units out of the unattributed account.
    Transfer {
        owner: Option<Owner>,
        recipient: Recipient,
        amount: Amount,
        user_data: UserData,
    },
    /// Claims `amount` units of value from the given owner's account in the remote
    /// `target` chain. Depending on its configuration, the `target` chain may refuse to
    /// process the message.
    Claim {
        owner: Owner,
        target_id: ChainId,
        recipient: Recipient,
        amount: Amount,
        user_data: UserData,
    },
    /// Creates (or activates) a new chain.
    /// This will automatically subscribe to the future committees created by `admin_id`.
    OpenChain(OpenChainConfig),
    /// Closes the chain.
    CloseChain,
    /// Changes the ownership of the chain.
    ChangeOwnership {
        /// Super owners can propose fast blocks in the first round, and regular blocks in any round.
        super_owners: Vec<PublicKey>,
        /// The regular owners, with their weights that determine how often they are round leader.
        owners: Vec<(PublicKey, u64)>,
        /// The number of initial rounds after 0 in which all owners are allowed to propose blocks.
        multi_leader_rounds: u32,
        /// The timeout configuration: how long fast, multi-leader and single-leader rounds last.
        timeout_config: TimeoutConfig,
    },
    /// Changes the application permissions configuration on this chain.
    ChangeApplicationPermissions(ApplicationPermissions),
    /// Subscribes to a system channel.
    Subscribe {
        chain_id: ChainId,
        channel: SystemChannel,
    },
    /// Unsubscribes from a system channel.
    Unsubscribe {
        chain_id: ChainId,
        channel: SystemChannel,
    },
    /// Publishes a new application bytecode.
    PublishBytecode {
        contract: Bytecode,
        service: Bytecode,
    },
    /// Publishes a new blob.
    PublishBlob { blob_id: BlobId },
    /// Reads a blob. This is test-only, so we can test without a Wasm application.
    #[cfg(with_testing)]
    ReadBlob { blob_id: BlobId },
    /// Creates a new application.
    CreateApplication {
        bytecode_id: BytecodeId,
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug")]
        parameters: Vec<u8>,
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug")]
        instantiation_argument: Vec<u8>,
        required_application_ids: Vec<UserApplicationId>,
    },
    /// Requests a message from another chain to register a user application on this chain.
    RequestApplication {
        chain_id: ChainId,
        application_id: UserApplicationId,
    },
    /// Operations that are only allowed on the admin chain.
    Admin(AdminOperation),
}

/// Operations that are only allowed on the admin chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum AdminOperation {
    /// Registers a new committee. This will notify the subscribers of the admin chain so that they
    /// can migrate to the new epoch by accepting the resulting `SetCommittees` as an incoming
    /// message in a block.
    CreateCommittee { epoch: Epoch, committee: Committee },
    /// Removes a committee. Once the resulting `SetCommittees` message is accepted by a chain,
    /// blocks from the retired epoch will not be accepted until they are followed (hence
    /// re-certified) by a block certified by a recent committee.
    RemoveCommittee { epoch: Epoch },
}

/// A system message meant to be executed on a remote chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum SystemMessage {
    /// Credits `amount` units of value to the account `target` -- unless the message is
    /// bouncing, in which case `source` is credited instead.
    Credit {
        target: Option<Owner>,
        amount: Amount,
        source: Option<Owner>,
    },
    /// Withdraws `amount` units of value from the account and starts a transfer to credit
    /// the recipient. The message must be properly authenticated. Receiver chains may
    /// refuse it depending on their configuration.
    Withdraw {
        owner: Owner,
        amount: Amount,
        recipient: Recipient,
        user_data: UserData,
    },
    /// Creates (or activates) a new chain.
    OpenChain(OpenChainConfig),
    /// Sets the current epoch and the recognized committees.
    SetCommittees {
        epoch: Epoch,
        committees: BTreeMap<Epoch, Committee>,
    },
    /// Subscribes to a channel.
    Subscribe {
        id: ChainId,
        subscription: ChannelSubscription,
    },
    /// Unsubscribes from a channel.
    Unsubscribe {
        id: ChainId,
        subscription: ChannelSubscription,
    },
    /// Notifies that a new application bytecode was published.
    BytecodePublished { transaction_index: u32 },
    /// Notifies that a new application was created.
    ApplicationCreated,
    /// Shares the locations of published bytecodes.
    BytecodeLocations {
        locations: Vec<(BytecodeId, BytecodeLocation)>,
    },
    /// Shares information about some applications to help the recipient use them.
    /// Applications must be registered after their dependencies.
    RegisterApplications {
        applications: Vec<UserApplicationDescription>,
    },
    /// Does nothing. Used to debug the intended recipients of a block.
    Notify { id: ChainId },
    /// Requests a `RegisterApplication` message from the target chain to register the specified
    /// application on the sender chain.
    RequestApplication(UserApplicationId),
}

impl SystemMessage {
    /// Returns an iterator over all bytecode locations this message introduces to the receiving
    /// chain, given the hash of the certificate that it originates from.
    pub fn bytecode_locations(
        &self,
        certificate_hash: CryptoHash,
    ) -> Box<dyn Iterator<Item = BytecodeLocation> + '_> {
        match self {
            SystemMessage::BytecodePublished { transaction_index } => {
                Box::new(iter::once(BytecodeLocation {
                    certificate_hash,
                    transaction_index: *transaction_index,
                }))
            }
            SystemMessage::BytecodeLocations {
                locations: new_locations,
            } => Box::new(new_locations.iter().map(|(_id, location)| *location)),
            SystemMessage::RegisterApplications { applications } => {
                Box::new(applications.iter().map(|app| app.bytecode_location))
            }
            SystemMessage::Credit { .. }
            | SystemMessage::Withdraw { .. }
            | SystemMessage::OpenChain(_)
            | SystemMessage::SetCommittees { .. }
            | SystemMessage::Subscribe { .. }
            | SystemMessage::Unsubscribe { .. }
            | SystemMessage::ApplicationCreated
            | SystemMessage::Notify { .. }
            | SystemMessage::RequestApplication(_) => Box::new(iter::empty()),
        }
    }
}

/// A query to the system state.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct SystemQuery;

/// The response to a system query.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct SystemResponse {
    pub chain_id: ChainId,
    pub balance: Amount,
}

/// The channels available in the system application.
#[derive(
    Enum, Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize, clap::ValueEnum,
)]
pub enum SystemChannel {
    /// Channel used to broadcast reconfigurations.
    Admin,
    /// Channel used to broadcast new published bytecodes.
    PublishedBytecodes,
}

impl SystemChannel {
    /// The [`ChannelName`] of this [`SystemChannel`].
    pub fn name(&self) -> ChannelName {
        bcs::to_bytes(self)
            .expect("`SystemChannel` can be serialized")
            .into()
    }
}

impl Display for SystemChannel {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        let display_name = match self {
            SystemChannel::Admin => "Admin",
            SystemChannel::PublishedBytecodes => "PublishedBytecodes",
        };

        write!(formatter, "{display_name}")
    }
}

/// The recipient of a transfer.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize)]
pub enum Recipient {
    /// This is mainly a placeholder for future extensions.
    Burn,
    /// Transfers to the balance of the given account.
    Account(Account),
}

impl Recipient {
    /// Returns the default recipient for the given chain (no owner).
    pub fn chain(chain_id: ChainId) -> Recipient {
        Recipient::Account(Account::chain(chain_id))
    }

    /// Returns the default recipient for the root chain with the given index.
    #[cfg(with_testing)]
    pub fn root(index: u32) -> Recipient {
        Recipient::chain(ChainId::root(index))
    }
}

/// Optional user message attached to a transfer.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Hash, Default, Debug, Serialize, Deserialize)]
pub struct UserData(pub Option<[u8; 32]>);

impl UserData {
    pub fn from_option_string(opt_str: Option<String>) -> Result<Self, usize> {
        // Convert the Option<String> to Option<[u8; 32]>
        let option_array = match opt_str {
            Some(s) => {
                // Convert the String to a Vec<u8>
                let vec = s.into_bytes();
                if vec.len() <= 32 {
                    // Create an array from the Vec<u8>
                    let mut array = [b' '; 32];

                    // Copy bytes from the vector into the array
                    let len = vec.len().min(32);
                    array[..len].copy_from_slice(&vec[..len]);

                    Some(array)
                } else {
                    return Err(vec.len());
                }
            }
            None => None,
        };

        // Return the UserData with the converted Option<[u8; 32]>
        Ok(UserData(option_array))
    }
}

#[derive(Error, Debug)]
pub enum SystemExecutionError {
    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),
    #[error(transparent)]
    ViewError(#[from] ViewError),

    #[error("Incorrect chain ID: {0}")]
    IncorrectChainId(ChainId),
    #[error("Invalid admin ID in new chain: {0}")]
    InvalidNewChainAdminId(ChainId),
    #[error("Invalid committees")]
    InvalidCommittees,
    #[error("{epoch:?} is not recognized by chain {chain_id:}")]
    InvalidEpoch { chain_id: ChainId, epoch: Epoch },
    #[error("Transfer must have positive amount")]
    IncorrectTransferAmount,
    #[error("Transfer from owned account must be authenticated by the right signer")]
    UnauthenticatedTransferOwner,
    #[error("The transferred amount must not exceed the current chain balance: {balance}")]
    InsufficientFunding { balance: Amount },
    #[error("Required execution fees exceeded the total funding available: {balance}")]
    InsufficientFundingForFees { balance: Amount },
    #[error("Claim must have positive amount")]
    IncorrectClaimAmount,
    #[error("Claim must be authenticated by the right signer")]
    UnauthenticatedClaimOwner,
    #[error("Admin operations are only allowed on the admin chain.")]
    AdminOperationOnNonAdminChain,
    #[error("Failed to create new committee")]
    InvalidCommitteeCreation,
    #[error("Failed to remove committee")]
    InvalidCommitteeRemoval,
    #[error(
        "Chain {0} tried to subscribe to the admin channel ({1}) of a chain that is not the admin chain"
    )]
    InvalidAdminSubscription(ChainId, SystemChannel),
    #[error("Cannot subscribe to a channel ({1}) on the same chain ({0})")]
    SelfSubscription(ChainId, SystemChannel),
    #[error("Chain {0} tried to subscribe to channel {1} but it is already subscribed")]
    AlreadySubscribedToChannel(ChainId, SystemChannel),
    #[error("Invalid unsubscription request to channel {1} on chain {0}")]
    InvalidUnsubscription(ChainId, SystemChannel),
    #[error("Amount overflow")]
    AmountOverflow,
    #[error("Amount underflow")]
    AmountUnderflow,
    #[error("Chain balance overflow")]
    BalanceOverflow,
    #[error("Chain balance underflow")]
    BalanceUnderflow,
    #[error("Cannot set epoch to a lower value")]
    CannotRewindEpoch,
    #[error("Cannot decrease the chain's timestamp")]
    TicksOutOfOrder,
    #[error("Attempt to create an application using unregistered bytecode identifier {0:?}")]
    UnknownBytecodeId(BytecodeId),
    #[error("Application {0:?} is not registered by the chain")]
    UnknownApplicationId(Box<UserApplicationId>),
    #[error("Chain is not active yet.")]
    InactiveChain,

    #[error("Blob not found on storage read: {0}")]
    BlobNotFoundOnRead(BlobId),
    #[error("Oracle response mismatch")]
    OracleResponseMismatch,
    #[error("No recorded response for oracle query")]
    MissingOracleResponse,
}

impl<C> SystemExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
    C::Extra: ExecutionRuntimeContext,
{
    /// Invariant for the states of active chains.
    pub fn is_active(&self) -> bool {
        self.description.get().is_some()
            && self.ownership.get().is_active()
            && self.current_committee().is_some()
            && self.admin_id.get().is_some()
    }

    /// Returns the current committee, if any.
    pub fn current_committee(&self) -> Option<(Epoch, &Committee)> {
        let epoch = self.epoch.get().as_ref()?;
        let committee = self.committees.get().get(epoch)?;
        Some((*epoch, committee))
    }

    /// Executes the sender's side of an operation and returns a list of actions to be
    /// taken.
    pub async fn execute_operation(
        &mut self,
        context: OperationContext,
        operation: SystemOperation,
        txn_tracker: &mut TransactionTracker,
    ) -> Result<Option<(UserApplicationId, Vec<u8>)>, SystemExecutionError> {
        use SystemOperation::*;
        let mut outcome = RawExecutionOutcome {
            authenticated_signer: context.authenticated_signer,
            refund_grant_to: context.refund_grant_to(),
            ..RawExecutionOutcome::default()
        };
        let mut new_application = None;
        match operation {
            OpenChain(config) => {
                let next_message_id = context.next_message_id(txn_tracker.message_count()?);
                let messages = self.open_chain(config, next_message_id)?;
                outcome.messages.extend(messages);
                #[cfg(with_metrics)]
                OPEN_CHAIN_COUNT.with_label_values(&[]).inc();
            }
            ChangeOwnership {
                super_owners,
                owners,
                multi_leader_rounds,
                timeout_config,
            } => {
                self.ownership.set(ChainOwnership {
                    super_owners: super_owners
                        .into_iter()
                        .map(|public_key| (Owner::from(public_key), public_key))
                        .collect(),
                    owners: owners
                        .into_iter()
                        .map(|(public_key, weight)| (Owner::from(public_key), (public_key, weight)))
                        .collect(),
                    multi_leader_rounds,
                    timeout_config,
                });
            }
            ChangeApplicationPermissions(application_permissions) => {
                self.application_permissions.set(application_permissions);
            }
            CloseChain => {
                let messages = self.close_chain(context.chain_id).await?;
                outcome.messages.extend(messages);
            }
            Transfer {
                owner,
                amount,
                recipient,
                ..
            } => {
                let message = self
                    .transfer(context.authenticated_signer, owner, recipient, amount)
                    .await?;

                if let Some(message) = message {
                    outcome.messages.push(message)
                }
            }
            Claim {
                owner,
                target_id,
                recipient,
                amount,
                user_data,
            } => {
                let message = self
                    .claim(
                        context.authenticated_signer,
                        owner,
                        target_id,
                        recipient,
                        amount,
                        user_data,
                    )
                    .await?;

                outcome.messages.push(message)
            }
            Admin(admin_operation) => {
                ensure!(
                    *self.admin_id.get() == Some(context.chain_id),
                    SystemExecutionError::AdminOperationOnNonAdminChain
                );
                match admin_operation {
                    AdminOperation::CreateCommittee { epoch, committee } => {
                        ensure!(
                            epoch == self.epoch.get().expect("chain is active").try_add_one()?,
                            SystemExecutionError::InvalidCommitteeCreation
                        );
                        self.committees.get_mut().insert(epoch, committee);
                        self.epoch.set(Some(epoch));
                        let message = RawOutgoingMessage {
                            destination: Destination::Subscribers(SystemChannel::Admin.name()),
                            authenticated: false,
                            grant: Amount::ZERO,
                            kind: MessageKind::Protected,
                            message: SystemMessage::SetCommittees {
                                epoch,
                                committees: self.committees.get().clone(),
                            },
                        };
                        outcome.messages.push(message);
                    }
                    AdminOperation::RemoveCommittee { epoch } => {
                        ensure!(
                            self.committees.get_mut().remove(&epoch).is_some(),
                            SystemExecutionError::InvalidCommitteeRemoval
                        );
                        let message = RawOutgoingMessage {
                            destination: Destination::Subscribers(SystemChannel::Admin.name()),
                            authenticated: false,
                            grant: Amount::ZERO,
                            kind: MessageKind::Protected,
                            message: SystemMessage::SetCommittees {
                                epoch: self.epoch.get().expect("chain is active"),
                                committees: self.committees.get().clone(),
                            },
                        };
                        outcome.messages.push(message);
                    }
                }
            }
            Subscribe { chain_id, channel } => {
                ensure!(
                    context.chain_id != chain_id,
                    SystemExecutionError::SelfSubscription(context.chain_id, channel)
                );
                if channel == SystemChannel::Admin {
                    ensure!(
                        self.admin_id.get().as_ref() == Some(&chain_id),
                        SystemExecutionError::InvalidAdminSubscription(context.chain_id, channel)
                    );
                }
                let subscription = ChannelSubscription {
                    chain_id,
                    name: channel.name(),
                };
                ensure!(
                    !self.subscriptions.contains(&subscription).await?,
                    SystemExecutionError::AlreadySubscribedToChannel(context.chain_id, channel)
                );
                self.subscriptions.insert(&subscription)?;
                let message = RawOutgoingMessage {
                    destination: Destination::Recipient(chain_id),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Protected,
                    message: SystemMessage::Subscribe {
                        id: context.chain_id,
                        subscription,
                    },
                };
                outcome.messages.push(message);
            }
            Unsubscribe { chain_id, channel } => {
                let subscription = ChannelSubscription {
                    chain_id,
                    name: channel.name(),
                };
                ensure!(
                    self.subscriptions.contains(&subscription).await?,
                    SystemExecutionError::InvalidUnsubscription(context.chain_id, channel)
                );
                self.subscriptions.remove(&subscription)?;
                let message = RawOutgoingMessage {
                    destination: Destination::Recipient(chain_id),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Protected,
                    message: SystemMessage::Unsubscribe {
                        id: context.chain_id,
                        subscription,
                    },
                };
                outcome.messages.push(message);
            }
            PublishBytecode { .. } => {
                // Send a `BytecodePublished` message to ourself so that we can broadcast
                // the bytecode-id next.
                let message = RawOutgoingMessage {
                    destination: Destination::Recipient(context.chain_id),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Protected,
                    message: SystemMessage::BytecodePublished {
                        transaction_index: context
                            .index
                            .expect("System application can not be called by other applications"),
                    },
                };
                outcome.messages.push(message);
            }
            CreateApplication {
                bytecode_id,
                parameters,
                instantiation_argument,
                required_application_ids,
            } => {
                let id = UserApplicationId {
                    bytecode_id,
                    creation: context.next_message_id(txn_tracker.message_count()?),
                };
                self.registry
                    .register_new_application(
                        id,
                        parameters.clone(),
                        required_application_ids.clone(),
                    )
                    .await?;
                // Send a message to ourself to increment the message ID.
                let message = RawOutgoingMessage {
                    destination: Destination::Recipient(context.chain_id),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Protected,
                    message: SystemMessage::ApplicationCreated,
                };
                outcome.messages.push(message);
                new_application = Some((id, instantiation_argument.clone()));
            }
            RequestApplication {
                chain_id,
                application_id,
            } => {
                let message = RawOutgoingMessage {
                    destination: Destination::Recipient(chain_id),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Simple,
                    message: SystemMessage::RequestApplication(application_id),
                };
                outcome.messages.push(message);
            }
            PublishBlob { blob_id } => {
                let response = OracleResponse::Blob(blob_id);
                if let Some(recorded_response) = txn_tracker.next_replayed_oracle_response()? {
                    ensure!(
                        recorded_response == response,
                        SystemExecutionError::OracleResponseMismatch
                    );
                }
                txn_tracker.add_oracle_response(response);
            }
            #[cfg(with_testing)]
            ReadBlob { blob_id } => {
                let response = OracleResponse::Blob(blob_id);
                if let Some(recorded_response) = txn_tracker.next_replayed_oracle_response()? {
                    ensure!(
                        recorded_response == response,
                        SystemExecutionError::OracleResponseMismatch
                    );
                }
                self.read_blob_content(blob_id).await?;
                txn_tracker.add_oracle_response(response);
            }
        }

        txn_tracker.add_system_outcome(outcome);
        Ok(new_application)
    }

    pub async fn transfer(
        &mut self,
        authenticated_signer: Option<Owner>,
        owner: Option<Owner>,
        recipient: Recipient,
        amount: Amount,
    ) -> Result<Option<RawOutgoingMessage<SystemMessage, Amount>>, SystemExecutionError> {
        if owner.is_some() {
            ensure!(
                authenticated_signer == owner,
                SystemExecutionError::UnauthenticatedTransferOwner
            );
        }
        ensure!(
            amount > Amount::ZERO,
            SystemExecutionError::IncorrectTransferAmount
        );
        let balance = match &owner {
            Some(owner) => self.balances.get_mut_or_default(owner).await?,
            None => self.balance.get_mut(),
        };
        balance
            .try_sub_assign(amount)
            .map_err(|_| SystemExecutionError::InsufficientFunding { balance: *balance })?;
        match recipient {
            Recipient::Account(account) => {
                let message = RawOutgoingMessage {
                    destination: Destination::Recipient(account.chain_id),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Tracked,
                    message: SystemMessage::Credit {
                        amount,
                        source: owner,
                        target: account.owner,
                    },
                };

                Ok(Some(message))
            }
            Recipient::Burn => Ok(None),
        }
    }

    pub async fn claim(
        &self,
        authenticated_signer: Option<Owner>,
        owner: Owner,
        target_id: ChainId,
        recipient: Recipient,
        amount: Amount,
        user_data: UserData,
    ) -> Result<RawOutgoingMessage<SystemMessage, Amount>, SystemExecutionError> {
        ensure!(
            authenticated_signer.as_ref() == Some(&owner),
            SystemExecutionError::UnauthenticatedClaimOwner
        );
        ensure!(
            amount > Amount::ZERO,
            SystemExecutionError::IncorrectClaimAmount
        );

        Ok(RawOutgoingMessage {
            destination: Destination::Recipient(target_id),
            authenticated: true,
            grant: Amount::ZERO,
            kind: MessageKind::Simple,
            message: SystemMessage::Withdraw {
                amount,
                owner,
                user_data,
                recipient,
            },
        })
    }

    /// Executes a cross-chain message that represents the recipient's side of an operation.
    pub async fn execute_message(
        &mut self,
        context: MessageContext,
        message: SystemMessage,
    ) -> Result<RawExecutionOutcome<SystemMessage, Amount>, SystemExecutionError> {
        let mut outcome = RawExecutionOutcome::default();
        use SystemMessage::*;
        match message {
            Credit {
                amount,
                source,
                target,
            } => {
                let receiver = if context.is_bouncing { source } else { target };
                match receiver {
                    None => {
                        let new_balance = self.balance.get().saturating_add(amount);
                        self.balance.set(new_balance);
                    }
                    Some(owner) => {
                        let balance = self.balances.get_mut_or_default(&owner).await?;
                        *balance = balance.saturating_add(amount);
                    }
                }
            }
            Withdraw {
                amount,
                owner,
                user_data: _,
                recipient,
            } => {
                ensure!(
                    context.authenticated_signer == Some(owner),
                    SystemExecutionError::UnauthenticatedClaimOwner
                );

                let balance = self.balances.get_mut_or_default(&owner).await?;
                balance
                    .try_sub_assign(amount)
                    .map_err(|_| SystemExecutionError::InsufficientFunding { balance: *balance })?;
                match recipient {
                    Recipient::Account(account) => {
                        let message = RawOutgoingMessage {
                            destination: Destination::Recipient(account.chain_id),
                            authenticated: false,
                            grant: Amount::ZERO,
                            kind: MessageKind::Tracked,
                            message: SystemMessage::Credit {
                                amount,
                                source: Some(owner),
                                target: account.owner,
                            },
                        };
                        outcome.messages.push(message);
                    }
                    Recipient::Burn => (),
                }
            }
            SetCommittees { epoch, committees } => {
                ensure!(
                    epoch >= self.epoch.get().expect("chain is active"),
                    SystemExecutionError::CannotRewindEpoch
                );
                self.epoch.set(Some(epoch));
                self.committees.set(committees);
            }
            Subscribe { id, subscription } => {
                ensure!(
                    subscription.chain_id == context.chain_id,
                    SystemExecutionError::IncorrectChainId(subscription.chain_id)
                );
                // Notify the subscriber about this block, so that it is included in the
                // received_log of the subscriber and correctly synchronized.
                let message = RawOutgoingMessage {
                    destination: Destination::Recipient(id),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Protected,
                    message: SystemMessage::Notify { id },
                };
                outcome.messages.push(message);
                outcome.subscribe.push((subscription.name.clone(), id));
            }
            Unsubscribe { id, subscription } => {
                ensure!(
                    subscription.chain_id == context.chain_id,
                    SystemExecutionError::IncorrectChainId(subscription.chain_id)
                );
                let message = RawOutgoingMessage {
                    destination: Destination::Recipient(id),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Protected,
                    message: SystemMessage::Notify { id },
                };
                outcome.messages.push(message);
                outcome.unsubscribe.push((subscription.name.clone(), id));
            }
            BytecodePublished { transaction_index } => {
                let bytecode_id = BytecodeId::new(context.message_id);
                let bytecode_location = BytecodeLocation {
                    certificate_hash: context.certificate_hash,
                    transaction_index,
                };
                self.registry
                    .register_published_bytecode(bytecode_id, bytecode_location)?;
                let locations = self.registry.bytecode_locations().await?;
                let message = RawOutgoingMessage {
                    destination: Destination::Subscribers(SystemChannel::PublishedBytecodes.name()),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Simple,
                    message: SystemMessage::BytecodeLocations { locations },
                };
                outcome.messages.push(message);
            }
            BytecodeLocations { locations } => {
                for (id, location) in locations {
                    self.registry.register_published_bytecode(id, location)?;
                }
            }
            RegisterApplications { applications } => {
                for application in applications {
                    self.registry
                        .register_application(application.clone())
                        .await?;
                }
            }
            RequestApplication(application_id) => {
                let applications = self
                    .registry
                    .describe_applications_with_dependencies(
                        vec![application_id],
                        &Default::default(),
                    )
                    .await?;
                let message = RawOutgoingMessage {
                    destination: Destination::Recipient(context.message_id.chain_id),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Simple,
                    message: SystemMessage::RegisterApplications { applications },
                };
                outcome.messages.push(message);
            }
            OpenChain(_) => {
                // This special message is executed immediately when cross-chain requests are received.
            }
            ApplicationCreated | Notify { .. } => (),
        }
        Ok(outcome)
    }

    /// Initializes the system application state on a newly opened chain.
    pub fn initialize_chain(
        &mut self,
        message_id: MessageId,
        timestamp: Timestamp,
        config: OpenChainConfig,
    ) {
        // Guaranteed under BFT assumptions.
        assert!(self.description.get().is_none());
        assert!(!self.ownership.get().is_active());
        assert!(self.committees.get().is_empty());
        let OpenChainConfig {
            ownership,
            admin_id,
            epoch,
            committees,
            balance,
            application_permissions,
        } = config;
        let description = ChainDescription::Child(message_id);
        self.description.set(Some(description));
        self.epoch.set(Some(epoch));
        self.committees.set(committees);
        self.admin_id.set(Some(admin_id));
        self.subscriptions
            .insert(&ChannelSubscription {
                chain_id: admin_id,
                name: SystemChannel::Admin.name(),
            })
            .expect("serialization failed");
        self.ownership.set(ownership);
        self.timestamp.set(timestamp);
        self.balance.set(balance);
        self.application_permissions.set(application_permissions);
    }

    pub async fn handle_query(
        &mut self,
        context: QueryContext,
        _query: SystemQuery,
    ) -> Result<SystemResponse, SystemExecutionError> {
        let response = SystemResponse {
            chain_id: context.chain_id,
            balance: *self.balance.get(),
        };
        Ok(response)
    }

    /// Returns the messages to open a new chain, and subtracts the new chain's balance
    /// from this chain's.
    pub fn open_chain(
        &mut self,
        config: OpenChainConfig,
        next_message_id: MessageId,
    ) -> Result<[RawOutgoingMessage<SystemMessage, Amount>; 2], SystemExecutionError> {
        let child_id = ChainId::child(next_message_id);
        ensure!(
            self.admin_id.get().as_ref() == Some(&config.admin_id),
            SystemExecutionError::InvalidNewChainAdminId(child_id)
        );
        let admin_id = config.admin_id;
        ensure!(
            self.committees.get() == &config.committees,
            SystemExecutionError::InvalidCommittees
        );
        ensure!(
            self.epoch.get().as_ref() == Some(&config.epoch),
            SystemExecutionError::InvalidEpoch {
                chain_id: child_id,
                epoch: config.epoch,
            }
        );
        let balance = self.balance.get_mut();
        balance
            .try_sub_assign(config.balance)
            .map_err(|_| SystemExecutionError::InsufficientFunding { balance: *balance })?;
        let open_chain_message = RawOutgoingMessage {
            destination: Destination::Recipient(child_id),
            authenticated: false,
            grant: Amount::ZERO,
            kind: MessageKind::Protected,
            message: SystemMessage::OpenChain(config),
        };
        let subscription = ChannelSubscription {
            chain_id: admin_id,
            name: SystemChannel::Admin.name(),
        };
        let subscribe_message = RawOutgoingMessage {
            destination: Destination::Recipient(admin_id),
            authenticated: false,
            grant: Amount::ZERO,
            kind: MessageKind::Protected,
            message: SystemMessage::Subscribe {
                id: child_id,
                subscription,
            },
        };
        Ok([open_chain_message, subscribe_message])
    }

    pub async fn close_chain(
        &mut self,
        id: ChainId,
    ) -> Result<Vec<RawOutgoingMessage<SystemMessage, Amount>>, SystemExecutionError> {
        let mut messages = Vec::new();
        // Unsubscribe to all channels.
        self.subscriptions
            .for_each_index(|subscription| {
                let message = RawOutgoingMessage {
                    destination: Destination::Recipient(subscription.chain_id),
                    authenticated: false,
                    grant: Amount::ZERO,
                    kind: MessageKind::Protected,
                    message: SystemMessage::Unsubscribe { id, subscription },
                };
                messages.push(message);
                Ok(())
            })
            .await?;
        self.subscriptions.clear();
        self.closed.set(true);
        Ok(messages)
    }

    pub async fn read_blob_content(
        &mut self,
        blob_id: BlobId,
    ) -> Result<BlobContent, SystemExecutionError> {
        self.context()
            .extra()
            .get_blob(blob_id)
            .await
            .map_err(|_| SystemExecutionError::BlobNotFoundOnRead(blob_id))
            .map(Into::into)
    }

    pub async fn assert_blob_exists(
        &mut self,
        blob_id: BlobId,
    ) -> Result<(), SystemExecutionError> {
        if self.context().extra().contains_blob(blob_id).await? {
            Ok(())
        } else {
            Err(SystemExecutionError::BlobNotFoundOnRead(blob_id))
        }
    }
}

#[cfg(test)]
mod tests {
    use linera_base::{data_types::BlockHeight, identifiers::ApplicationId};
    use linera_views::memory::MemoryContext;

    use super::*;
    use crate::{ExecutionOutcome, ExecutionStateView, TestExecutionRuntimeContext};

    /// Returns an execution state view and a matching operation context, for epoch 1, with root
    /// chain 0 as the admin ID and one empty committee.
    async fn new_view_and_context() -> (
        ExecutionStateView<MemoryContext<TestExecutionRuntimeContext>>,
        OperationContext,
    ) {
        let description = ChainDescription::Root(5);
        let context = OperationContext {
            chain_id: ChainId::from(description),
            authenticated_signer: None,
            authenticated_caller_id: None,
            height: BlockHeight::from(7),
            index: Some(2),
            next_message_index: 3,
        };
        let state = SystemExecutionState {
            description: Some(description),
            epoch: Some(Epoch(1)),
            admin_id: Some(ChainId::root(0)),
            committees: BTreeMap::new(),
            ..SystemExecutionState::default()
        };
        let view = state.into_view().await;
        (view, context)
    }

    #[tokio::test]
    async fn bytecode_message_index() {
        let (mut view, context) = new_view_and_context().await;
        let operation = SystemOperation::PublishBytecode {
            contract: Bytecode::new(vec![]),
            service: Bytecode::new(vec![]),
        };
        let mut txn_tracker = TransactionTracker::default();
        let new_application = view
            .system
            .execute_operation(context, operation, &mut txn_tracker)
            .await
            .unwrap();
        assert_eq!(new_application, None);
        let transaction_index = context
            .index
            .expect("Missing operation index in dummy context");
        let [ExecutionOutcome::System(result)] = &txn_tracker.destructure().unwrap().0[..] else {
            panic!("Unexpected outcome");
        };
        assert_eq!(
            result.messages[PUBLISH_BYTECODE_MESSAGE_INDEX as usize].message,
            SystemMessage::BytecodePublished { transaction_index }
        );
    }

    #[tokio::test]
    async fn application_message_index() {
        let (mut view, context) = new_view_and_context().await;
        let bytecode_id = BytecodeId::new(MessageId {
            chain_id: context.chain_id,
            height: BlockHeight::from(5),
            index: 0,
        });
        let location = BytecodeLocation {
            certificate_hash: CryptoHash::test_hash("certificate"),
            transaction_index: 1,
        };
        view.system
            .registry
            .register_published_bytecode(bytecode_id, location)
            .unwrap();

        let operation = SystemOperation::CreateApplication {
            bytecode_id,
            parameters: vec![],
            instantiation_argument: vec![],
            required_application_ids: vec![],
        };
        let mut txn_tracker = TransactionTracker::default();
        let new_application = view
            .system
            .execute_operation(context, operation, &mut txn_tracker)
            .await
            .unwrap();
        let [ExecutionOutcome::System(result)] = &txn_tracker.destructure().unwrap().0[..] else {
            panic!("Unexpected outcome");
        };
        assert_eq!(
            result.messages[CREATE_APPLICATION_MESSAGE_INDEX as usize].message,
            SystemMessage::ApplicationCreated
        );
        let creation = MessageId {
            chain_id: context.chain_id,
            height: context.height,
            index: context.next_message_index + CREATE_APPLICATION_MESSAGE_INDEX,
        };
        let id = ApplicationId {
            bytecode_id,
            creation,
        };
        assert_eq!(new_application, Some((id, vec![])));
    }

    #[tokio::test]
    async fn open_chain_message_index() {
        let (mut view, context) = new_view_and_context().await;
        let epoch = view.system.epoch.get().unwrap();
        let admin_id = view.system.admin_id.get().unwrap();
        let committees = view.system.committees.get().clone();
        let ownership = ChainOwnership::single(PublicKey::test_key(0));
        let config = OpenChainConfig {
            ownership,
            committees,
            epoch,
            admin_id,
            balance: Amount::ZERO,
            application_permissions: Default::default(),
        };
        let mut txn_tracker = TransactionTracker::default();
        let operation = SystemOperation::OpenChain(config.clone());
        let new_application = view
            .system
            .execute_operation(context, operation, &mut txn_tracker)
            .await
            .unwrap();
        assert_eq!(new_application, None);
        let [ExecutionOutcome::System(result)] = &txn_tracker.destructure().unwrap().0[..] else {
            panic!("Unexpected outcome");
        };
        assert_eq!(
            result.messages[OPEN_CHAIN_MESSAGE_INDEX as usize].message,
            SystemMessage::OpenChain(config)
        );
    }
}
