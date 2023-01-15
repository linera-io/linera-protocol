// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    ApplicationRegistryView, Bytecode, BytecodeId, BytecodeLocation, ChainOwnership, ChannelId,
    ChannelName, Destination, EffectContext, NewApplication, OperationContext, QueryContext,
    RawExecutionResult, UserApplicationId,
};
use linera_base::{
    committee::Committee,
    data_types::{ArithmeticError, ChainDescription, ChainId, EffectId, Epoch, Owner},
    ensure,
};
use linera_views::{
    common::Context,
    register_view::RegisterView,
    set_view::SetView,
    views::{HashableContainerView, View, ViewError},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
};
use thiserror::Error;

#[cfg(any(test, feature = "test"))]
use std::collections::BTreeSet;

/// A view accessing the execution state of the system of a chain.
#[derive(Debug, HashableContainerView)]
pub struct SystemExecutionStateView<C> {
    /// How the chain was created. May be unknown for inactive chains.
    pub description: RegisterView<C, Option<ChainDescription>>,
    /// The number identifying the current configuration.
    pub epoch: RegisterView<C, Option<Epoch>>,
    /// The admin of the chain.
    pub admin_id: RegisterView<C, Option<ChainId>>,
    /// Track the channels that we have subscribed to.
    pub subscriptions: SetView<C, ChannelId>,
    /// The committees that we trust, indexed by epoch number.
    /// Not using a `MapView` because the set active of committees is supposed to be
    /// small. Plus, currently, we would create the `BTreeMap` anyway in various places
    /// (e.g. the `OpenChain` operation).
    pub committees: RegisterView<C, BTreeMap<Epoch, Committee>>,
    /// Ownership of the chain.
    pub ownership: RegisterView<C, ChainOwnership>,
    /// Balance of the chain.
    pub balance: RegisterView<C, Balance>,
}

/// For testing only.
#[cfg(any(test, feature = "test"))]
#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub struct SystemExecutionState {
    pub description: Option<ChainDescription>,
    pub epoch: Option<Epoch>,
    pub admin_id: Option<ChainId>,
    pub subscriptions: BTreeSet<ChannelId>,
    pub committees: BTreeMap<Epoch, Committee>,
    pub ownership: ChainOwnership,
    pub balance: Balance,
}

/// A system operation.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum SystemOperation {
    /// Transfer `amount` units of value to the recipient.
    Transfer {
        recipient: Address,
        amount: Amount,
        user_data: UserData,
    },
    /// Create (or activate) a new chain by installing the given authentication key.
    /// This will automatically subscribe to the future committees created by `admin_id`.
    OpenChain {
        id: ChainId,
        owner: Owner,
        admin_id: ChainId,
        epoch: Epoch,
        committees: BTreeMap<Epoch, Committee>,
    },
    /// Close the chain.
    CloseChain,
    /// Change the authentication key of the chain.
    ChangeOwner { new_owner: Owner },
    /// Change the authentication key of the chain.
    ChangeMultipleOwners { new_owners: Vec<Owner> },
    /// (admin chain only) Register a new committee. This will notify the subscribers of
    /// the admin chain so that they can migrate to the new epoch (by accepting the
    /// notification as an "incoming message" in a next block).
    CreateCommittee {
        admin_id: ChainId,
        epoch: Epoch,
        committee: Committee,
    },
    /// Subscribe to a system channel.
    Subscribe {
        chain_id: ChainId,
        channel: SystemChannel,
    },
    /// Unsubscribe to a system channel.
    Unsubscribe {
        chain_id: ChainId,
        channel: SystemChannel,
    },
    /// (admin chain only) Remove a committee. Once this message is accepted by a chain,
    /// blocks from the retired epoch will not be accepted until they are followed (hence
    /// re-certified) by a block certified by a recent committee.
    RemoveCommittee { admin_id: ChainId, epoch: Epoch },
    /// Publish a new application bytecode.
    PublishBytecode {
        contract: Bytecode,
        service: Bytecode,
    },
    /// Create a new application.
    CreateNewApplication {
        bytecode_id: BytecodeId,
        argument: Vec<u8>,
    },
}

/// The effect of a system operation to be performed on a remote chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum SystemEffect {
    /// Credit `amount` units of value to the recipient.
    Credit { recipient: ChainId, amount: Amount },
    /// Create (or activate) a new chain by installing the given authentication key.
    OpenChain {
        id: ChainId,
        owner: Owner,
        admin_id: ChainId,
        epoch: Epoch,
        committees: BTreeMap<Epoch, Committee>,
    },
    /// Set the current epoch and the recognized committees.
    SetCommittees {
        admin_id: ChainId,
        epoch: Epoch,
        committees: BTreeMap<Epoch, Committee>,
    },
    /// Subscribe to a channel.
    Subscribe { id: ChainId, channel_id: ChannelId },
    /// Unsubscribe to a channel.
    Unsubscribe { id: ChainId, channel_id: ChannelId },
    /// Notify that a new application bytecode was published.
    BytecodePublished,
    /// Does nothing. Used to debug the intended recipients of a block.
    Notify { id: ChainId },
}

/// A query to the system state.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct SystemQuery;

/// The response to a system query.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct SystemResponse {
    pub chain_id: ChainId,
    pub balance: Balance,
}

/// The channels available in the system application.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
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

/// A recipient's address.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize)]
pub enum Address {
    /// This is mainly a placeholder for future extensions.
    Burn,
    /// We currently support only one user account per chain.
    Account(ChainId),
}

/// A non-negative amount of money to be transferred.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct Amount(u64);

/// The balance of a chain.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default, Debug, Serialize, Deserialize,
)]
pub struct Balance(u128);

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
    #[error("Transfers must have positive amount")]
    IncorrectTransferAmount,
    #[error(
        "The transferred amount must be not exceed the current chain balance: {current_balance}"
    )]
    InsufficientFunding { current_balance: u128 },
    #[error("Failed to create new committee")]
    InvalidCommitteeCreation,
    #[error("Failed to remove committee")]
    InvalidCommitteeRemoval,
    #[error("Invalid subscription request to channel {1} on chain {0}")]
    InvalidSubscription(ChainId, SystemChannel),
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
            && self.epoch.get().is_some()
            && self
                .committees
                .get()
                .contains_key(&self.epoch.get().unwrap())
            && self.admin_id.get().is_some()
    }

    /// Execute the sender's side of the operation.
    /// Return a list of recipients who need to be notified.
    pub async fn execute_operation(
        &mut self,
        context: &OperationContext,
        operation: &SystemOperation,
    ) -> Result<(RawExecutionResult<SystemEffect>, Option<NewApplication>), SystemExecutionError>
    {
        use SystemOperation::*;
        let mut result = RawExecutionResult::default();
        let mut new_application = None;
        match operation {
            OpenChain {
                id,
                owner,
                committees,
                admin_id,
                epoch,
            } => {
                let expected_id = ChainId::child((*context).into());
                ensure!(
                    id == &expected_id,
                    SystemExecutionError::InvalidNewChainId(*id)
                );
                ensure!(
                    self.admin_id.get().as_ref() == Some(admin_id),
                    SystemExecutionError::InvalidNewChainAdminId(*id)
                );
                ensure!(
                    self.committees.get() == committees,
                    SystemExecutionError::InvalidCommittees
                );
                ensure!(
                    self.epoch.get().as_ref() == Some(epoch),
                    SystemExecutionError::InvalidEpoch {
                        chain_id: *id,
                        epoch: *epoch
                    }
                );
                let e1 = (
                    Destination::Recipient(*id),
                    SystemEffect::OpenChain {
                        id: *id,
                        owner: *owner,
                        committees: committees.clone(),
                        admin_id: *admin_id,
                        epoch: *epoch,
                    },
                );
                let channel_id = ChannelId {
                    chain_id: *admin_id,
                    name: SystemChannel::Admin.name(),
                };
                let e2 = (
                    Destination::Recipient(*admin_id),
                    SystemEffect::Subscribe {
                        id: *id,
                        channel_id,
                    },
                );
                result.effects = vec![e1, e2];
            }
            ChangeOwner { new_owner } => {
                self.ownership.set(ChainOwnership::single(*new_owner));
            }
            ChangeMultipleOwners { new_owners } => {
                self.ownership
                    .set(ChainOwnership::multiple(new_owners.clone()));
            }
            CloseChain => {
                self.ownership.set(ChainOwnership::default());
                // Unsubscribe to all channels.
                let mut effects = Vec::new();
                self.subscriptions
                    .for_each_index(|channel_id| {
                        effects.push((
                            Destination::Recipient(channel_id.chain_id),
                            SystemEffect::Unsubscribe {
                                id: context.chain_id,
                                channel_id,
                            },
                        ));
                        Ok(())
                    })
                    .await?;
                self.subscriptions.clear();
                result.effects = effects;
            }
            Transfer {
                amount, recipient, ..
            } => {
                ensure!(
                    *amount > Amount::zero(),
                    SystemExecutionError::IncorrectTransferAmount
                );
                ensure!(
                    *self.balance.get() >= (*amount).into(),
                    SystemExecutionError::InsufficientFunding {
                        current_balance: (*self.balance.get()).into()
                    }
                );
                self.balance.get_mut().try_sub_assign((*amount).into())?;
                if let Address::Account(id) = recipient {
                    result.effects = vec![(
                        Destination::Recipient(*id),
                        SystemEffect::Credit {
                            amount: *amount,
                            recipient: *id,
                        },
                    )];
                }
            }
            CreateCommittee {
                admin_id,
                epoch,
                committee,
            } => {
                // We are the admin chain and want to create a committee.
                ensure!(
                    *admin_id == context.chain_id,
                    SystemExecutionError::InvalidCommitteeCreation
                );
                ensure!(
                    Some(admin_id) == self.admin_id.get().as_ref(),
                    SystemExecutionError::InvalidCommitteeCreation
                );
                ensure!(
                    *epoch == self.epoch.get().expect("chain is active").try_add_one()?,
                    SystemExecutionError::InvalidCommitteeCreation
                );
                self.committees.get_mut().insert(*epoch, committee.clone());
                self.epoch.set(Some(*epoch));
                result.effects = vec![(
                    Destination::Subscribers(SystemChannel::Admin.name()),
                    SystemEffect::SetCommittees {
                        admin_id: *admin_id,
                        epoch: self.epoch.get().expect("chain is active"),
                        committees: self.committees.get().clone(),
                    },
                )];
            }
            RemoveCommittee { admin_id, epoch } => {
                // We are the admin chain and want to remove a committee.
                ensure!(
                    *admin_id == context.chain_id,
                    SystemExecutionError::InvalidCommitteeRemoval
                );
                ensure!(
                    Some(admin_id) == self.admin_id.get().as_ref(),
                    SystemExecutionError::InvalidCommitteeRemoval
                );
                ensure!(
                    self.committees.get_mut().remove(epoch).is_some(),
                    SystemExecutionError::InvalidCommitteeRemoval
                );
                result.effects = vec![(
                    Destination::Subscribers(SystemChannel::Admin.name()),
                    SystemEffect::SetCommittees {
                        admin_id: *admin_id,
                        epoch: self.epoch.get().expect("chain is active"),
                        committees: self.committees.get().clone(),
                    },
                )];
            }
            Subscribe { chain_id, channel } => {
                // We should not subscribe to ourself in this case.
                ensure!(
                    context.chain_id != *chain_id,
                    SystemExecutionError::InvalidSubscription(context.chain_id, *channel)
                );
                if *channel == SystemChannel::Admin {
                    // Can only subscribe to the admin channel of this chain's admin chain.
                    ensure!(
                        self.admin_id.get().as_ref() == Some(chain_id),
                        SystemExecutionError::InvalidSubscription(context.chain_id, *channel)
                    );
                }
                let channel_id = ChannelId {
                    chain_id: *chain_id,
                    name: channel.name(),
                };
                ensure!(
                    self.subscriptions.get(&channel_id).await?.is_none(),
                    SystemExecutionError::InvalidSubscription(context.chain_id, *channel)
                );
                self.subscriptions.insert(&channel_id)?;
                result.effects = vec![(
                    Destination::Recipient(*chain_id),
                    SystemEffect::Subscribe {
                        id: context.chain_id,
                        channel_id,
                    },
                )];
            }
            Unsubscribe { chain_id, channel } => {
                let channel_id = ChannelId {
                    chain_id: *chain_id,
                    name: channel.name(),
                };
                ensure!(
                    self.subscriptions.get(&channel_id).await?.is_some(),
                    SystemExecutionError::InvalidUnsubscription(context.chain_id, *channel)
                );
                self.subscriptions.remove(&channel_id)?;
                result.effects = vec![(
                    Destination::Recipient(*chain_id),
                    SystemEffect::Unsubscribe {
                        id: context.chain_id,
                        channel_id,
                    },
                )];
            }
            PublishBytecode { .. } => {
                result.effects = vec![(
                    Destination::Subscribers(SystemChannel::PublishedBytecodes.name()),
                    SystemEffect::BytecodePublished,
                )];
            }
            CreateNewApplication {
                bytecode_id,
                argument,
            } => {
                new_application = Some(NewApplication {
                    id: UserApplicationId {
                        bytecode_id: *bytecode_id,
                        creation: (*context).into(),
                    },
                    initialization_argument: argument.clone(),
                });
            }
        }

        Ok((result, new_application))
    }

    /// Execute the recipient's side of an operation, aka a "remote effect".
    /// Effects must be executed by order of heights in the sender's chain.
    pub fn execute_effect(
        &mut self,
        applications: &mut ApplicationRegistryView<C>,
        context: &EffectContext,
        effect: &SystemEffect,
    ) -> Result<RawExecutionResult<SystemEffect>, SystemExecutionError> {
        let mut result = RawExecutionResult::default();
        use SystemEffect::*;
        match effect {
            Credit { amount, recipient } if context.chain_id == *recipient => {
                let new_balance = self
                    .balance
                    .get()
                    .try_add((*amount).into())
                    .unwrap_or_else(|_| Balance::max());
                self.balance.set(new_balance);
            }
            SetCommittees {
                admin_id,
                epoch,
                committees,
            } if self.admin_id.get().as_ref() == Some(admin_id) => {
                ensure!(
                    *epoch >= self.epoch.get().expect("chain is active"),
                    SystemExecutionError::CannotRewindEpoch
                );
                self.epoch.set(Some(*epoch));
                self.committees.set(committees.clone());
            }
            Subscribe { id, channel_id } if channel_id.chain_id == context.chain_id => {
                // Notify the subscriber about this block, so that it is included in the
                // receive_log of the subscriber and correctly synchronized.
                result.effects = vec![(
                    Destination::Recipient(*id),
                    SystemEffect::Notify { id: *id },
                )];
                result.subscribe = vec![(channel_id.name.clone(), *id)];
            }
            Unsubscribe { id, channel_id } if channel_id.chain_id == context.chain_id => {
                result.effects = vec![(
                    Destination::Recipient(*id),
                    SystemEffect::Notify { id: *id },
                )];
                result.unsubscribe = vec![(channel_id.name.clone(), *id)];
            }
            BytecodePublished => {
                let bytecode_id = BytecodeId(context.effect_id);
                let bytecode_location = BytecodeLocation {
                    certificate_hash: context.certificate_hash,
                    operation_index: context.effect_id.index,
                };
                applications.register_published_bytecode(bytecode_id, bytecode_location)?;
            }
            Notify { .. } => (),
            OpenChain { .. } => {
                // This special effect is executed immediately when cross-chain requests are received.
            }
            _ => {
                log::error!("Skipping unexpected received effect: {effect:?}");
            }
        }
        Ok(result)
    }

    /// Initialize the system application state on a newly opened chain.
    pub fn open_chain(
        &mut self,
        effect_id: EffectId,
        chain_id: ChainId,
        owner: Owner,
        epoch: Epoch,
        committees: BTreeMap<Epoch, Committee>,
        admin_id: ChainId,
    ) {
        // Guaranteed under BFT assumptions.
        assert!(self.description.get().is_none());
        assert!(!self.ownership.get().is_active());
        assert!(self.committees.get().is_empty());
        let description = ChainDescription::Child(effect_id);
        assert_eq!(chain_id, description.into());
        self.description.set(Some(description));
        self.epoch.set(Some(epoch));
        self.committees.set(committees);
        self.admin_id.set(Some(admin_id));
        self.subscriptions
            .insert(&ChannelId {
                chain_id: admin_id,
                name: SystemChannel::Admin.name(),
            })
            .expect("serialization failed");
        self.ownership.set(ChainOwnership::single(owner));
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

impl Amount {
    #[inline]
    pub fn zero() -> Self {
        Amount(0)
    }

    #[inline]
    pub fn try_add(self, other: Self) -> Result<Self, SystemExecutionError> {
        let val = self
            .0
            .checked_add(other.0)
            .ok_or(SystemExecutionError::AmountOverflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_sub(self, other: Self) -> Result<Self, SystemExecutionError> {
        let val = self
            .0
            .checked_sub(other.0)
            .ok_or(SystemExecutionError::AmountUnderflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign(&mut self, other: Self) -> Result<(), SystemExecutionError> {
        self.0 = self
            .0
            .checked_add(other.0)
            .ok_or(SystemExecutionError::AmountOverflow)?;
        Ok(())
    }

    #[inline]
    pub fn try_sub_assign(&mut self, other: Self) -> Result<(), SystemExecutionError> {
        self.0 = self
            .0
            .checked_sub(other.0)
            .ok_or(SystemExecutionError::AmountUnderflow)?;
        Ok(())
    }
}

impl Balance {
    #[inline]
    pub fn zero() -> Self {
        Balance(0)
    }

    #[inline]
    pub fn max() -> Self {
        Balance(std::u128::MAX)
    }

    #[inline]
    pub fn try_add(self, other: Self) -> Result<Self, SystemExecutionError> {
        let val = self
            .0
            .checked_add(other.0)
            .ok_or(SystemExecutionError::BalanceOverflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_sub(self, other: Self) -> Result<Self, SystemExecutionError> {
        let val = self
            .0
            .checked_sub(other.0)
            .ok_or(SystemExecutionError::BalanceUnderflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign(&mut self, other: Self) -> Result<(), SystemExecutionError> {
        self.0 = self
            .0
            .checked_add(other.0)
            .ok_or(SystemExecutionError::BalanceOverflow)?;
        Ok(())
    }

    #[inline]
    pub fn try_sub_assign(&mut self, other: Self) -> Result<(), SystemExecutionError> {
        self.0 = self
            .0
            .checked_sub(other.0)
            .ok_or(SystemExecutionError::BalanceUnderflow)?;
        Ok(())
    }
}

impl std::fmt::Display for Balance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for Amount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Balance {
    type Err = std::num::ParseIntError;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        Ok(Self(u128::from_str(src)?))
    }
}

impl std::str::FromStr for Amount {
    type Err = std::num::ParseIntError;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        Ok(Self(u64::from_str(src)?))
    }
}

impl From<Amount> for u64 {
    fn from(val: Amount) -> Self {
        val.0
    }
}

impl From<Amount> for Balance {
    fn from(val: Amount) -> Self {
        Balance(val.0 as u128)
    }
}

impl TryFrom<Balance> for Amount {
    type Error = std::num::TryFromIntError;

    fn try_from(val: Balance) -> Result<Self, Self::Error> {
        Ok(Amount(val.0.try_into()?))
    }
}

impl From<u64> for Amount {
    fn from(value: u64) -> Self {
        Amount(value)
    }
}

impl From<u128> for Balance {
    fn from(value: u128) -> Self {
        Balance(value)
    }
}

impl From<Balance> for u128 {
    fn from(value: Balance) -> Self {
        value.0
    }
}
