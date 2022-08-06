// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    committee::Committee,
    crypto::*,
    ensure,
    error::Error,
    manager::ChainManager,
    messages::{BlockHeight, ChainId, ChannelId, EffectId, Epoch, Medium, Origin, Owner},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Execution state of a chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub struct ExecutionState {
    /// The UID of the chain.
    pub chain_id: ChainId,
    /// The number identifying the current configuration.
    pub epoch: Option<Epoch>,
    /// The admin of the chain.
    pub admin_id: Option<ChainId>,
    /// Track the channels that we have subscribed to.
    /// We avoid BTreeSet<String> because of a Serde/BCS limitation.
    pub subscriptions: BTreeMap<ChannelId, ()>,
    /// The committees that we trust, indexed by epoch number.
    pub committees: BTreeMap<Epoch, Committee>,
    /// Manager of the chain.
    pub manager: ChainManager,
    /// Balance of the chain.
    pub balance: Balance,
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

/// The name of the channel for the admin chain to broadcast reconfigurations.
pub const ADMIN_CHANNEL: &str = "ADMIN";

/// A chain operation.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Operation {
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
    /// Subscribe to future committees created by `admin_id`. Same as OpenChain but useful
    /// for root chains (other than admin_id) created in the genesis config.
    SubscribeToNewCommittees { admin_id: ChainId },
    /// Unsubscribe to future committees created by `admin_id`. (This is not really useful
    /// and only meant for testing.)
    UnsubscribeToNewCommittees { admin_id: ChainId },
    /// (admin chain only) Remove a committee. Once this message is accepted by a chain,
    /// blocks from the retired epoch will not be accepted until they are followed (hence
    /// re-certified) by a block certified by a recent committee.
    RemoveCommittee { admin_id: ChainId, epoch: Epoch },
}

/// The effect of an operation to be performed on a remote chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Effect {
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
    Subscribe { id: ChainId, channel: ChannelId },
    /// Unsubscribe to a channel.
    Unsubscribe { id: ChainId, channel: ChannelId },
    /// Does nothing. Used to debug the intended recipients of a block.
    Notify { id: ChainId },
}

impl BcsSignable for ExecutionState {}

impl ExecutionState {
    pub fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            epoch: None,
            admin_id: None,
            subscriptions: BTreeMap::new(),
            committees: BTreeMap::new(),
            manager: ChainManager::default(),
            balance: Balance::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct ApplicationResult {
    pub effects: Vec<Effect>,
    pub recipients: Vec<ChainId>,
    pub subscribe: Option<(String, ChainId)>,
    pub unsubscribe: Option<(String, ChainId)>,
    pub need_channel_broadcast: Vec<String>,
}

impl ExecutionState {
    pub(crate) fn is_recipient(&self, origin: &Origin, effect: &Effect) -> bool {
        use Effect::*;
        match (&origin.medium, effect) {
            (Medium::Direct, Credit { recipient, .. }) => {
                // We are the recipient of the transfer.
                self.chain_id == *recipient
            }
            (Medium::Direct, OpenChain { id, .. }) => {
                // We are the created chain.
                self.chain_id == *id
            }
            (Medium::Direct, Subscribe { channel, .. } | Unsubscribe { channel, .. }) => {
                // We are the owner of the channel.
                self.chain_id == channel.chain_id
            }
            (Medium::Channel(channel), SetCommittees { admin_id, .. }) => {
                // We are managed by this admin chain.
                channel == ADMIN_CHANNEL
                    && self.chain_id != *admin_id
                    && self.admin_id.as_ref() == Some(admin_id)
            }
            (Medium::Direct, Notify { id }) => self.chain_id == *id,
            _ => false,
        }
    }

    /// Execute the sender's side of the operation.
    /// Return a list of recipients who need to be notified.
    pub(crate) fn apply_operation(
        &mut self,
        height: BlockHeight,
        index: usize,
        operation: &Operation,
    ) -> Result<ApplicationResult, Error> {
        let operation_id = EffectId {
            chain_id: self.chain_id,
            height,
            index,
        };
        match operation {
            Operation::OpenChain {
                id,
                owner,
                committees,
                admin_id,
                epoch,
            } => {
                let expected_id = ChainId::child(operation_id);
                ensure!(id == &expected_id, Error::InvalidNewChainId(*id));
                ensure!(
                    self.admin_id.as_ref() == Some(admin_id),
                    Error::InvalidNewChainAdminId(*id)
                );
                ensure!(&self.committees == committees, Error::InvalidCommittees);
                ensure!(
                    self.epoch.as_ref() == Some(epoch),
                    Error::InvalidEpoch {
                        chain_id: *id,
                        epoch: *epoch
                    }
                );
                let e1 = Effect::OpenChain {
                    id: *id,
                    owner: *owner,
                    committees: committees.clone(),
                    admin_id: *admin_id,
                    epoch: *epoch,
                };
                let e2 = Effect::Subscribe {
                    id: *id,
                    channel: ChannelId {
                        chain_id: *admin_id,
                        name: ADMIN_CHANNEL.into(),
                    },
                };
                let application = ApplicationResult {
                    effects: vec![e1, e2],
                    recipients: vec![*id, *admin_id],
                    subscribe: None,
                    unsubscribe: None,
                    need_channel_broadcast: Vec::new(),
                };
                Ok(application)
            }
            Operation::ChangeOwner { new_owner } => {
                self.manager = ChainManager::single(*new_owner);
                Ok(ApplicationResult::default())
            }
            Operation::ChangeMultipleOwners { new_owners } => {
                self.manager = ChainManager::multiple(new_owners.clone());
                Ok(ApplicationResult::default())
            }
            Operation::CloseChain => {
                self.manager = ChainManager::default();
                // Unsubscribe to all channels.
                let subscriptions = std::mem::take(&mut self.subscriptions);
                let mut effects = Vec::new();
                let mut recipients = Vec::new();
                for (channel, ()) in subscriptions {
                    recipients.push(channel.chain_id);
                    effects.push(Effect::Unsubscribe {
                        id: self.chain_id,
                        channel,
                    });
                }
                let application = ApplicationResult {
                    effects,
                    recipients,
                    subscribe: None,
                    unsubscribe: None,
                    need_channel_broadcast: Vec::new(),
                };
                Ok(application)
            }
            Operation::Transfer {
                amount, recipient, ..
            } => {
                ensure!(*amount > Amount::zero(), Error::IncorrectTransferAmount);
                ensure!(
                    self.balance >= (*amount).into(),
                    Error::InsufficientFunding {
                        current_balance: self.balance
                    }
                );
                self.balance.try_sub_assign((*amount).into())?;
                let application = match recipient {
                    Address::Burn => ApplicationResult::default(),
                    Address::Account(id) => ApplicationResult {
                        effects: vec![Effect::Credit {
                            amount: *amount,
                            recipient: *id,
                        }],
                        recipients: vec![*id],
                        subscribe: None,
                        unsubscribe: None,
                        need_channel_broadcast: Vec::new(),
                    },
                };
                Ok(application)
            }
            Operation::CreateCommittee {
                admin_id,
                epoch,
                committee,
            } => {
                // We are the admin chain and want to create a committee.
                ensure!(*admin_id == self.chain_id, Error::InvalidCommitteeCreation);
                ensure!(
                    Some(admin_id) == self.admin_id.as_ref(),
                    Error::InvalidCommitteeCreation
                );
                ensure!(
                    *epoch == self.epoch.expect("chain is active").try_add_one()?,
                    Error::InvalidCommitteeCreation
                );
                self.committees.insert(*epoch, committee.clone());
                self.epoch = Some(*epoch);
                let application = ApplicationResult {
                    effects: vec![Effect::SetCommittees {
                        admin_id: *admin_id,
                        epoch: self.epoch.expect("chain is active"),
                        committees: self.committees.clone(),
                    }],
                    recipients: Vec::new(),
                    subscribe: None,
                    unsubscribe: None,
                    // Notify our subscribers.
                    need_channel_broadcast: vec![ADMIN_CHANNEL.into()],
                };
                Ok(application)
            }
            Operation::RemoveCommittee { admin_id, epoch } => {
                // We are the admin chain and want to remove a committee.
                ensure!(*admin_id == self.chain_id, Error::InvalidCommitteeRemoval);
                ensure!(
                    Some(admin_id) == self.admin_id.as_ref(),
                    Error::InvalidCommitteeRemoval
                );
                ensure!(
                    self.committees.remove(epoch).is_some(),
                    Error::InvalidCommitteeRemoval
                );
                let application = ApplicationResult {
                    effects: vec![Effect::SetCommittees {
                        admin_id: *admin_id,
                        epoch: self.epoch.expect("chain is active"),
                        committees: self.committees.clone(),
                    }],
                    recipients: Vec::new(),
                    subscribe: None,
                    unsubscribe: None,
                    // Notify our subscribers.
                    need_channel_broadcast: vec![ADMIN_CHANNEL.into()],
                };
                Ok(application)
            }
            Operation::SubscribeToNewCommittees { admin_id } => {
                // We should not subscribe to ourself in this case.
                ensure!(
                    self.chain_id != *admin_id,
                    Error::InvalidSubscriptionToNewCommittees(self.chain_id)
                );
                ensure!(
                    self.admin_id.as_ref() == Some(admin_id),
                    Error::InvalidSubscriptionToNewCommittees(self.chain_id)
                );
                let channel_id = ChannelId {
                    chain_id: *admin_id,
                    name: ADMIN_CHANNEL.into(),
                };
                ensure!(
                    !self.subscriptions.contains_key(&channel_id),
                    Error::InvalidSubscriptionToNewCommittees(self.chain_id)
                );
                self.subscriptions.insert(channel_id, ());
                let application = ApplicationResult {
                    effects: vec![Effect::Subscribe {
                        id: self.chain_id,
                        channel: ChannelId {
                            chain_id: *admin_id,
                            name: ADMIN_CHANNEL.into(),
                        },
                    }],
                    recipients: vec![*admin_id],
                    subscribe: None,
                    unsubscribe: None,
                    need_channel_broadcast: Vec::new(),
                };
                Ok(application)
            }
            Operation::UnsubscribeToNewCommittees { admin_id } => {
                let channel_id = ChannelId {
                    chain_id: *admin_id,
                    name: ADMIN_CHANNEL.into(),
                };
                ensure!(
                    self.subscriptions.contains_key(&channel_id),
                    Error::InvalidUnsubscriptionToNewCommittees(self.chain_id)
                );
                self.subscriptions.remove(&channel_id);
                let application = ApplicationResult {
                    effects: vec![Effect::Unsubscribe {
                        id: self.chain_id,
                        channel: ChannelId {
                            chain_id: *admin_id,
                            name: ADMIN_CHANNEL.into(),
                        },
                    }],
                    recipients: vec![*admin_id],
                    subscribe: None,
                    unsubscribe: None,
                    need_channel_broadcast: Vec::new(),
                };
                Ok(application)
            }
        }
    }

    /// Execute the recipient's side of an operation, aka a "remote effect".
    /// Effects must be executed by order of heights in the sender's chain.
    pub(crate) fn apply_effect(&mut self, effect: &Effect) -> Result<ApplicationResult, Error> {
        match effect {
            Effect::Credit { amount, recipient } if self.chain_id == *recipient => {
                self.balance = self
                    .balance
                    .try_add((*amount).into())
                    .unwrap_or_else(|_| Balance::max());
                Ok(ApplicationResult::default())
            }
            Effect::SetCommittees {
                admin_id,
                epoch,
                committees,
            } if self.admin_id.as_ref() == Some(admin_id) => {
                // This chain was not yet subscribed at the time earlier epochs were broadcast.
                ensure!(
                    *epoch >= self.epoch.expect("chain is active"),
                    Error::InvalidCrossChainRequest
                );
                self.epoch = Some(*epoch);
                self.committees = committees.clone();
                Ok(ApplicationResult::default())
            }
            Effect::Subscribe { id, channel } if channel.chain_id == self.chain_id => {
                // Notify the subscriber about this block, so that it is included in the
                // receive_log of the subscriber and correctly synchronized.
                let application = ApplicationResult {
                    effects: vec![Effect::Notify { id: *id }],
                    recipients: vec![*id],
                    subscribe: Some((channel.name.clone(), *id)),
                    unsubscribe: None,
                    need_channel_broadcast: Vec::new(),
                };
                Ok(application)
            }
            Effect::Unsubscribe { id, channel } if channel.chain_id == self.chain_id => {
                let application = ApplicationResult {
                    effects: vec![Effect::Notify { id: *id }],
                    recipients: vec![*id], // Notify the subscriber.
                    subscribe: None,
                    unsubscribe: Some((channel.name.clone(), *id)),
                    need_channel_broadcast: Vec::new(),
                };
                Ok(application)
            }
            Effect::Notify { .. } => Ok(ApplicationResult::default()),
            Effect::OpenChain { .. } => {
                // This special effect is executed immediately when cross-chain requests are received.
                Ok(ApplicationResult::default())
            }
            _ => {
                log::error!("Skipping unexpected received effect: {effect:?}");
                Ok(ApplicationResult::default())
            }
        }
    }
}

impl Amount {
    #[inline]
    pub fn zero() -> Self {
        Amount(0)
    }

    #[inline]
    pub fn try_add(self, other: Self) -> Result<Self, Error> {
        let val = self.0.checked_add(other.0).ok_or(Error::AmountOverflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_sub(self, other: Self) -> Result<Self, Error> {
        let val = self.0.checked_sub(other.0).ok_or(Error::AmountUnderflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign(&mut self, other: Self) -> Result<(), Error> {
        self.0 = self.0.checked_add(other.0).ok_or(Error::AmountOverflow)?;
        Ok(())
    }

    #[inline]
    pub fn try_sub_assign(&mut self, other: Self) -> Result<(), Error> {
        self.0 = self.0.checked_sub(other.0).ok_or(Error::AmountUnderflow)?;
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
    pub fn try_add(self, other: Self) -> Result<Self, Error> {
        let val = self.0.checked_add(other.0).ok_or(Error::BalanceOverflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_sub(self, other: Self) -> Result<Self, Error> {
        let val = self.0.checked_sub(other.0).ok_or(Error::BalanceUnderflow)?;
        Ok(Self(val))
    }

    #[inline]
    pub fn try_add_assign(&mut self, other: Self) -> Result<(), Error> {
        self.0 = self.0.checked_add(other.0).ok_or(Error::BalanceOverflow)?;
        Ok(())
    }

    #[inline]
    pub fn try_sub_assign(&mut self, other: Self) -> Result<(), Error> {
        self.0 = self.0.checked_sub(other.0).ok_or(Error::BalanceUnderflow)?;
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
