// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    committee::{Committee, Epoch},
    ApplicationRegistryView, Bytecode, BytecodeLocation, ChainOwnership, ChannelName,
    ChannelSubscription, Destination, MessageContext, OperationContext, QueryContext,
    RawExecutionResult, UserApplicationDescription, UserApplicationId,
};
use async_graphql::Enum;
use custom_debug_derive::Debug;
use linera_base::{
    crypto::{CryptoHash, PublicKey},
    data_types::{Amount, ArithmeticError, Timestamp},
    ensure, hex_debug,
    identifiers::{BytecodeId, ChainDescription, ChainId, MessageId, Owner},
};
use linera_views::{
    common::Context,
    map_view::MapView,
    register_view::RegisterView,
    set_view::SetView,
    views::{HashableView, View, ViewError},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
    iter,
    str::FromStr,
};
use thiserror::Error;

#[cfg(any(test, feature = "test"))]
use {crate::applications::ApplicationRegistry, std::collections::BTreeSet};

/// A view accessing the execution state of the system of a chain.
#[derive(Debug, HashableView)]
pub struct SystemExecutionStateView<C> {
    /// How the chain was created. May be unknown for inactive chains.
    pub description: RegisterView<C, Option<ChainDescription>>,
    /// The number identifying the current configuration.
    pub epoch: RegisterView<C, Option<Epoch>>,
    /// The admin of the chain.
    pub admin_id: RegisterView<C, Option<ChainId>>,
    /// Track the channels that we have subscribed to.
    pub subscriptions: SetView<C, ChannelSubscription>,
    /// The committees that we trust, indexed by epoch number.
    /// Not using a `MapView` because the set active of committees is supposed to be
    /// small. Plus, currently, we would create the `BTreeMap` anyway in various places
    /// (e.g. the `OpenChain` operation).
    pub committees: RegisterView<C, BTreeMap<Epoch, Committee>>,
    /// Ownership of the chain.
    pub ownership: RegisterView<C, ChainOwnership>,
    /// Balance of the chain (unattributed).
    pub balance: RegisterView<C, Amount>,
    /// Balances attributed to a given owner.
    pub balances: MapView<C, Owner, Amount>,
    /// The timestamp of the most recent block.
    pub timestamp: RegisterView<C, Timestamp>,
    /// Track the locations of known bytecodes as well as the descriptions of known applications.
    pub registry: ApplicationRegistryView<C>,
}

/// For testing only.
#[cfg(any(test, feature = "test"))]
#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub struct SystemExecutionState {
    pub description: Option<ChainDescription>,
    pub epoch: Option<Epoch>,
    pub admin_id: Option<ChainId>,
    pub subscriptions: BTreeSet<ChannelSubscription>,
    pub committees: BTreeMap<Epoch, Committee>,
    pub ownership: ChainOwnership,
    pub balance: Amount,
    pub balances: BTreeMap<Owner, Amount>,
    pub timestamp: Timestamp,
    pub registry: ApplicationRegistry,
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
    /// Claims `amount` units of value from the given owner's account in
    /// the remote `target` chain. Depending on its configuration (see also #464), the
    /// `target` chain may refuse to process the message.
    Claim {
        owner: Owner,
        target: ChainId,
        recipient: Recipient,
        amount: Amount,
        user_data: UserData,
    },
    /// Creates (or activates) a new chain by installing the given authentication key.
    /// This will automatically subscribe to the future committees created by `admin_id`.
    OpenChain {
        public_key: PublicKey,
        admin_id: ChainId,
        epoch: Epoch,
        committees: BTreeMap<Epoch, Committee>,
    },
    /// Closes the chain.
    CloseChain,
    /// Changes the authentication key of the chain.
    ChangeOwner { new_public_key: PublicKey },
    /// Changes the authentication key of the chain.
    ChangeMultipleOwners { new_public_keys: Vec<PublicKey> },
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
    /// Creates a new application.
    CreateApplication {
        bytecode_id: BytecodeId,
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug")]
        parameters: Vec<u8>,
        #[serde(with = "serde_bytes")]
        #[debug(with = "hex_debug")]
        initialization_argument: Vec<u8>,
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
    /// Credits `amount` units of value to the account.
    Credit { account: Account, amount: Amount },
    /// Withdraws `amount` units of value from the account and starts a transfer to credit
    /// the recipient. The message must be properly authenticated. Receiver chains may
    /// refuse it depending on their configuration.
    Withdraw {
        account: Account,
        amount: Amount,
        recipient: Recipient,
        user_data: UserData,
    },
    /// Creates (or activates) a new chain by installing the given authentication key.
    OpenChain {
        public_key: PublicKey,
        admin_id: ChainId,
        epoch: Epoch,
        committees: BTreeMap<Epoch, Committee>,
    },
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
    BytecodePublished { operation_index: u32 },
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
            SystemMessage::BytecodePublished { operation_index } => {
                Box::new(iter::once(BytecodeLocation {
                    certificate_hash,
                    operation_index: *operation_index,
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
            | SystemMessage::OpenChain { .. }
            | SystemMessage::SetCommittees { .. }
            | SystemMessage::Subscribe { .. }
            | SystemMessage::Unsubscribe { .. }
            | SystemMessage::ApplicationCreated { .. }
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
#[derive(Enum, Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
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
    /// Transfers to the system balance of the given owner (or any owner) at the given
    /// chain.
    Account(Account),
}

/// A system account.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize)]
pub struct Account {
    /// The chain of the account.
    pub chain_id: ChainId,
    /// The owner of the account.
    pub owner: Option<Owner>,
}

impl Account {
    pub fn chain(chain_id: ChainId) -> Self {
        Account {
            chain_id,
            owner: None,
        }
    }

    pub fn owner(chain_id: ChainId, owner: Owner) -> Self {
        Account {
            chain_id,
            owner: Some(owner),
        }
    }
}

impl std::fmt::Display for Account {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.owner {
            Some(owner) => write!(f, "{}:{}", self.chain_id, owner),
            None => write!(f, "{}", self.chain_id),
        }
    }
}

impl FromStr for Account {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        anyhow::ensure!(
            parts.len() <= 2,
            "Expecting format `chain-id:address` or `chain-id`"
        );
        if parts.len() == 1 {
            Ok(Account::chain(s.parse()?))
        } else {
            let chain_id = parts[0].parse()?;
            let owner = parts[1].parse()?;
            Ok(Account::owner(chain_id, owner))
        }
    }
}

/// Optional user message attached to a transfer.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Hash, Default, Debug, Serialize, Deserialize)]
pub struct UserData(pub Option<[u8; 32]>);

#[derive(Error, Debug)]
pub enum SystemExecutionError {
    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),
    #[error(transparent)]
    ViewError(#[from] ViewError),

    #[error("Invalid new chain id: {0}")]
    InvalidNewChainId(ChainId),
    #[error("Invalid admin id in new chain: {0}")]
    InvalidNewChainAdminId(ChainId),
    #[error("Invalid committees")]
    InvalidCommittees,
    #[error("{epoch:?} is not recognized by chain {chain_id:}")]
    InvalidEpoch { chain_id: ChainId, epoch: Epoch },
    #[error("Transfer must have positive amount")]
    IncorrectTransferAmount,
    #[error("Transfer from owned account must be authenticated by the right signer")]
    UnauthenticatedTransferOwner,
    #[error(
        "The transferred amount must be not exceed the current chain balance: {current_balance}"
    )]
    InsufficientFunding { current_balance: Amount },
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
        "Attempted to subscribe to the admin channel ({1}) of this chain's ({0}) admin chain {1}"
    )]
    InvalidAdminSubscription(ChainId, SystemChannel),
    #[error("Attempted to subscribe to self on channel {1} on chain {0}")]
    SelfSubscription(ChainId, SystemChannel),
    #[error("Attempted to subscribe to a channel which does not exist ({1}) on chain {0}")]
    NoSuchChannel(ChainId, SystemChannel),
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
}

impl<C> SystemExecutionStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
    ViewError: From<C::Error>,
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
        context: &OperationContext,
        operation: &SystemOperation,
    ) -> Result<
        (
            RawExecutionResult<SystemMessage>,
            Option<(UserApplicationId, Vec<u8>)>,
        ),
        SystemExecutionError,
    > {
        use SystemOperation::*;
        let mut result = RawExecutionResult::default();
        let mut new_application = None;
        match operation {
            OpenChain {
                public_key,
                committees,
                admin_id,
                epoch,
            } => {
                let child_id = ChainId::child(context.next_message_id());
                ensure!(
                    self.admin_id.get().as_ref() == Some(admin_id),
                    SystemExecutionError::InvalidNewChainAdminId(child_id)
                );
                ensure!(
                    self.committees.get() == committees,
                    SystemExecutionError::InvalidCommittees
                );
                ensure!(
                    self.epoch.get().as_ref() == Some(epoch),
                    SystemExecutionError::InvalidEpoch {
                        chain_id: child_id,
                        epoch: *epoch
                    }
                );
                let e1 = (
                    Destination::Recipient(child_id),
                    false,
                    SystemMessage::OpenChain {
                        public_key: *public_key,
                        committees: committees.clone(),
                        admin_id: *admin_id,
                        epoch: *epoch,
                    },
                );
                let subscription = ChannelSubscription {
                    chain_id: *admin_id,
                    name: SystemChannel::Admin.name(),
                };
                let e2 = (
                    Destination::Recipient(*admin_id),
                    false,
                    SystemMessage::Subscribe {
                        id: child_id,
                        subscription,
                    },
                );
                result.messages.extend([e1, e2]);
            }
            ChangeOwner { new_public_key } => {
                self.ownership.set(ChainOwnership::single(*new_public_key));
            }
            ChangeMultipleOwners { new_public_keys } => {
                self.ownership
                    .set(ChainOwnership::multiple(new_public_keys.iter().cloned()));
            }
            CloseChain => {
                self.ownership.set(ChainOwnership::default());
                // Unsubscribe to all channels.
                self.subscriptions
                    .for_each_index(|subscription| {
                        result.messages.push((
                            Destination::Recipient(subscription.chain_id),
                            false,
                            SystemMessage::Unsubscribe {
                                id: context.chain_id,
                                subscription,
                            },
                        ));
                        Ok(())
                    })
                    .await?;
                self.subscriptions.clear();
            }
            Transfer {
                owner,
                amount,
                recipient,
                ..
            } => {
                if owner.is_some() {
                    ensure!(
                        &context.authenticated_signer == owner,
                        SystemExecutionError::UnauthenticatedTransferOwner
                    );
                }
                ensure!(
                    *amount > Amount::zero(),
                    SystemExecutionError::IncorrectTransferAmount
                );
                let balance = match &owner {
                    Some(owner) => self.balances.get_mut_or_default(owner).await?,
                    None => self.balance.get_mut(),
                };
                ensure!(
                    *balance >= *amount,
                    SystemExecutionError::InsufficientFunding {
                        current_balance: *balance
                    }
                );
                balance.try_sub_assign(*amount)?;
                if let Recipient::Account(account) = recipient {
                    result.messages.push((
                        Destination::Recipient(account.chain_id),
                        false,
                        SystemMessage::Credit {
                            amount: *amount,
                            account: *account,
                        },
                    ));
                }
            }
            Claim {
                owner,
                target,
                recipient,
                amount,
                user_data,
            } => {
                ensure!(
                    context.authenticated_signer.as_ref() == Some(owner),
                    SystemExecutionError::UnauthenticatedClaimOwner
                );
                ensure!(
                    *amount > Amount::zero(),
                    SystemExecutionError::IncorrectClaimAmount
                );
                result.messages.push((
                    Destination::Recipient(*target),
                    true,
                    SystemMessage::Withdraw {
                        amount: *amount,
                        account: Account {
                            chain_id: *target,
                            owner: Some(*owner),
                        },
                        user_data: user_data.clone(),
                        recipient: *recipient,
                    },
                ));
            }
            Admin(admin_operation) => {
                ensure!(
                    *self.admin_id.get() == Some(context.chain_id),
                    SystemExecutionError::AdminOperationOnNonAdminChain
                );
                match admin_operation {
                    AdminOperation::CreateCommittee { epoch, committee } => {
                        ensure!(
                            *epoch == self.epoch.get().expect("chain is active").try_add_one()?,
                            SystemExecutionError::InvalidCommitteeCreation
                        );
                        self.committees.get_mut().insert(*epoch, committee.clone());
                        self.epoch.set(Some(*epoch));
                        result.messages.push((
                            Destination::Subscribers(SystemChannel::Admin.name()),
                            false,
                            SystemMessage::SetCommittees {
                                epoch: *epoch,
                                committees: self.committees.get().clone(),
                            },
                        ));
                    }
                    AdminOperation::RemoveCommittee { epoch } => {
                        ensure!(
                            self.committees.get_mut().remove(epoch).is_some(),
                            SystemExecutionError::InvalidCommitteeRemoval
                        );
                        result.messages.push((
                            Destination::Subscribers(SystemChannel::Admin.name()),
                            false,
                            SystemMessage::SetCommittees {
                                epoch: self.epoch.get().expect("chain is active"),
                                committees: self.committees.get().clone(),
                            },
                        ));
                    }
                }
            }
            Subscribe { chain_id, channel } => {
                ensure!(
                    context.chain_id != *chain_id,
                    SystemExecutionError::SelfSubscription(context.chain_id, *channel)
                );
                if *channel == SystemChannel::Admin {
                    ensure!(
                        self.admin_id.get().as_ref() == Some(chain_id),
                        SystemExecutionError::InvalidAdminSubscription(context.chain_id, *channel)
                    );
                }
                let subscription = ChannelSubscription {
                    chain_id: *chain_id,
                    name: channel.name(),
                };
                ensure!(
                    !self.subscriptions.contains(&subscription).await?,
                    SystemExecutionError::NoSuchChannel(context.chain_id, *channel)
                );
                self.subscriptions.insert(&subscription)?;
                result.messages.push((
                    Destination::Recipient(*chain_id),
                    false,
                    SystemMessage::Subscribe {
                        id: context.chain_id,
                        subscription,
                    },
                ));
            }
            Unsubscribe { chain_id, channel } => {
                let subscription = ChannelSubscription {
                    chain_id: *chain_id,
                    name: channel.name(),
                };
                ensure!(
                    self.subscriptions.contains(&subscription).await?,
                    SystemExecutionError::InvalidUnsubscription(context.chain_id, *channel)
                );
                self.subscriptions.remove(&subscription)?;
                result.messages.push((
                    Destination::Recipient(*chain_id),
                    false,
                    SystemMessage::Unsubscribe {
                        id: context.chain_id,
                        subscription,
                    },
                ));
            }
            PublishBytecode { .. } => {
                // Send a `BytecodePublished` message to ourself so that we can broadcast
                // the bytecode-id next.
                result.messages.push((
                    Destination::Recipient(context.chain_id),
                    false,
                    SystemMessage::BytecodePublished {
                        operation_index: context.index,
                    },
                ));
            }
            CreateApplication {
                bytecode_id,
                parameters,
                initialization_argument,
                required_application_ids,
            } => {
                let id = UserApplicationId {
                    bytecode_id: *bytecode_id,
                    creation: context.next_message_id(),
                };
                self.registry
                    .register_new_application(
                        id,
                        parameters.clone(),
                        required_application_ids.clone(),
                    )
                    .await?;
                // Send a message to ourself to increment the message ID.
                result.messages.push((
                    Destination::Recipient(context.chain_id),
                    false,
                    SystemMessage::ApplicationCreated,
                ));
                new_application = Some((id, initialization_argument.clone()));
            }
            RequestApplication {
                chain_id,
                application_id,
            } => result.messages.push((
                Destination::Recipient(*chain_id),
                false,
                SystemMessage::RequestApplication(*application_id),
            )),
        }

        Ok((result, new_application))
    }

    /// Executes a cross-chain message that represents the recipient's side of an operation.
    ///
    /// * Messages should not return an error unless it is a temporary failure (e.g.
    /// storage) or a global system failure. An error will fail the entire cross-chain
    /// request, allowing it to be retried later.
    ///
    /// * If execution is impossible for a deterministic reason (e.g. insufficient
    /// funds), messages should fail silently and be skipped (similar to a transaction in
    /// traditional blockchains).
    pub async fn execute_message(
        &mut self,
        context: &MessageContext,
        message: &SystemMessage,
    ) -> Result<RawExecutionResult<SystemMessage>, SystemExecutionError> {
        let mut result = RawExecutionResult::default();
        use SystemMessage::*;
        match message {
            Credit { amount, account } if context.chain_id == account.chain_id => {
                match &account.owner {
                    None => {
                        let new_balance = self.balance.get().saturating_add(*amount);
                        self.balance.set(new_balance);
                    }
                    Some(owner) => {
                        let balance = self.balances.get_mut_or_default(owner).await?;
                        *balance = balance.saturating_add(*amount);
                    }
                }
            }
            Withdraw {
                amount,
                account:
                    Account {
                        owner: Some(owner),
                        chain_id,
                    },
                user_data: _,
                recipient,
            } if chain_id == &context.chain_id
                && context.authenticated_signer.as_ref() == Some(owner) =>
            {
                let balance = self.balances.get_mut_or_default(owner).await?;
                if balance.try_sub_assign(*amount).is_ok() {
                    if let Recipient::Account(account) = recipient {
                        result.messages.push((
                            Destination::Recipient(account.chain_id),
                            false,
                            SystemMessage::Credit {
                                amount: *amount,
                                account: *account,
                            },
                        ));
                    }
                } else {
                    tracing::info!("Withdrawal request was skipped due to lack of funds.");
                }
            }
            SetCommittees { epoch, committees } => {
                ensure!(
                    *epoch >= self.epoch.get().expect("chain is active"),
                    SystemExecutionError::CannotRewindEpoch
                );
                self.epoch.set(Some(*epoch));
                self.committees.set(committees.clone());
            }
            Subscribe { id, subscription } if subscription.chain_id == context.chain_id => {
                // Notify the subscriber about this block, so that it is included in the
                // receive_log of the subscriber and correctly synchronized.
                result.messages.push((
                    Destination::Recipient(*id),
                    false,
                    SystemMessage::Notify { id: *id },
                ));
                result.subscribe.push((subscription.name.clone(), *id));
            }
            Unsubscribe { id, subscription } if subscription.chain_id == context.chain_id => {
                result.messages.push((
                    Destination::Recipient(*id),
                    false,
                    SystemMessage::Notify { id: *id },
                ));
                result.unsubscribe.push((subscription.name.clone(), *id));
            }
            BytecodePublished { operation_index } => {
                let bytecode_id = BytecodeId::new(context.message_id);
                let bytecode_location = BytecodeLocation {
                    certificate_hash: context.certificate_hash,
                    operation_index: *operation_index,
                };
                self.registry
                    .register_published_bytecode(bytecode_id, bytecode_location)?;
                let locations = self.registry.bytecode_locations().await?;
                result.messages.push((
                    Destination::Subscribers(SystemChannel::PublishedBytecodes.name()),
                    false,
                    SystemMessage::BytecodeLocations { locations },
                ));
            }
            BytecodeLocations { locations } => {
                for (id, location) in locations {
                    self.registry.register_published_bytecode(*id, *location)?;
                }
            }
            ApplicationCreated { .. } | Notify { .. } => (),
            OpenChain { .. } => {
                // This special message is executed immediately when cross-chain requests are received.
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
                        vec![*application_id],
                        &Default::default(),
                    )
                    .await?;
                result.messages.push((
                    Destination::Recipient(context.message_id.chain_id),
                    false,
                    SystemMessage::RegisterApplications { applications },
                ));
            }
            _ => {
                tracing::error!(
                    "Skipping unexpected received message: {message:?} with context: {context:?}"
                );
            }
        }
        Ok(result)
    }

    /// Initializes the system application state on a newly opened chain.
    #[allow(clippy::too_many_arguments)]
    pub fn open_chain(
        &mut self,
        message_id: MessageId,
        public_key: PublicKey,
        epoch: Epoch,
        committees: BTreeMap<Epoch, Committee>,
        admin_id: ChainId,
        timestamp: Timestamp,
    ) {
        // Guaranteed under BFT assumptions.
        assert!(self.description.get().is_none());
        assert!(!self.ownership.get().is_active());
        assert!(self.committees.get().is_empty());
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
        self.ownership.set(ChainOwnership::single(public_key));
        self.timestamp.set(timestamp);
    }

    pub async fn query_application(
        &mut self,
        context: &QueryContext,
        _query: &SystemQuery,
    ) -> Result<SystemResponse, SystemExecutionError> {
        let response = SystemResponse {
            chain_id: context.chain_id,
            balance: *self.balance.get(),
        };
        Ok(response)
    }
}
