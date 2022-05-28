// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    committee::Committee,
    crypto::*,
    ensure,
    error::Error,
    manager::ChainManager,
    messages::{BlockHeight, ChainId, EffectId, Epoch, Owner},
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
    /// Whether our reconfigurations are managed by a "beacon" chain, or if we are it and
    /// managing other chains.
    pub status: Option<ChainStatus>,
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
    SubscribeToNewCommittees { id: ChainId, admin_id: ChainId },
    /// (admin chain only) Remove a committee. Once this message is accepted by a chain,
    /// blocks from the retired epoch will not be accepted until they are followed (hence
    /// re-certified) by a block certified by a recent committee.
    RemoveCommittee { admin_id: ChainId, epoch: Epoch },
}

/// The administrative status of this chain w.r.t reconfigurations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(any(test, feature = "test"), derive(Eq, PartialEq))]
pub enum ChainStatus {
    ManagedBy { admin_id: ChainId, subscribed: bool },
    Managing { subscribers: Vec<ChainId> },
}

/// The effect of an operation to be performed on a remote chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Effect {
    /// Transfer `amount` units of value to the recipient.
    Transfer { recipient: ChainId, amount: Amount },
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
    /// Subscribe to future committees created by `admin_id`.
    SubscribeToNewCommittees { id: ChainId, admin_id: ChainId },
}

impl BcsSignable for ExecutionState {}

impl ExecutionState {
    pub fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            epoch: None,
            status: None,
            committees: BTreeMap::new(),
            manager: ChainManager::default(),
            balance: Balance::default(),
        }
    }
}

impl ExecutionState {
    pub fn admin_id(&self) -> Result<ChainId, Error> {
        match self
            .status
            .as_ref()
            .ok_or(Error::InactiveChain(self.chain_id))?
        {
            ChainStatus::ManagedBy { admin_id, .. } => Ok(*admin_id),
            ChainStatus::Managing { .. } => Ok(self.chain_id),
        }
    }

    pub(crate) fn is_recipient(&self, effect: &Effect) -> bool {
        use Effect::*;
        match effect {
            Transfer { recipient, .. } => {
                // We are the recipient of the transfer.
                self.chain_id == *recipient
            }
            OpenChain { id, .. } => {
                // We are the created chain.
                self.chain_id == *id
            }
            SubscribeToNewCommittees { admin_id, .. } => {
                // We are the admin chain.
                self.chain_id == *admin_id
            }
            SetCommittees { admin_id, .. } => {
                // We are subscribed to this admin chain.
                match self.status.as_ref() {
                    Some(ChainStatus::ManagedBy {
                        admin_id: id,
                        subscribed: s,
                    }) => *s && admin_id == id,
                    _ => false,
                }
            }
        }
    }

    /// Execute the sender's side of the operation.
    /// Return a list of recipients who need to be notified.
    pub(crate) fn apply_operation(
        &mut self,
        chain_id: ChainId,
        height: BlockHeight,
        index: usize,
        operation: &Operation,
    ) -> Result<(Vec<Effect>, Vec<ChainId>), Error> {
        let operation_id = EffectId {
            chain_id,
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
                    self.admin_id() == Ok(*admin_id),
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
                let e2 = Effect::SubscribeToNewCommittees {
                    id: *id,
                    admin_id: *admin_id,
                };
                Ok((vec![e1, e2], vec![*id, *admin_id]))
            }
            Operation::ChangeOwner { new_owner } => {
                self.manager = ChainManager::single(*new_owner);
                Ok((Vec::new(), Vec::new()))
            }
            Operation::ChangeMultipleOwners { new_owners } => {
                self.manager = ChainManager::multiple(new_owners.clone());
                Ok((Vec::new(), Vec::new()))
            }
            Operation::CloseChain => {
                self.manager = ChainManager::default();
                Ok((Vec::new(), Vec::new()))
            }
            Operation::Transfer {
                amount, recipient, ..
            } => {
                ensure!(
                    matches!(self.status, Some(ChainStatus::ManagedBy { .. })),
                    Error::UnsupportedOperation
                );
                ensure!(*amount > Amount::zero(), Error::IncorrectTransferAmount);
                ensure!(
                    self.balance >= (*amount).into(),
                    Error::InsufficientFunding {
                        current_balance: self.balance
                    }
                );
                self.balance.try_sub_assign((*amount).into())?;
                match recipient {
                    Address::Burn => Ok((Vec::new(), Vec::new())),
                    Address::Account(id) => Ok((
                        vec![Effect::Transfer {
                            amount: *amount,
                            recipient: *id,
                        }],
                        vec![*id],
                    )),
                }
            }
            Operation::CreateCommittee {
                admin_id,
                epoch,
                committee,
            } => {
                // We are the admin chain and want to create a committee.
                ensure!(*admin_id == chain_id, Error::InvalidCommitteeCreation);
                ensure!(
                    *epoch == self.epoch.expect("chain is active").try_add_one()?,
                    Error::InvalidCommitteeCreation
                );
                let recipients = match &self.status {
                    Some(ChainStatus::Managing { subscribers }) => subscribers.clone(),
                    _ => return Err(Error::InvalidCommitteeCreation),
                };
                self.committees.insert(*epoch, committee.clone());
                self.epoch = Some(*epoch);
                // Notify our subscribers.
                Ok((
                    vec![Effect::SetCommittees {
                        admin_id: *admin_id,
                        epoch: self.epoch.expect("chain is active"),
                        committees: self.committees.clone(),
                    }],
                    recipients,
                ))
            }
            Operation::RemoveCommittee { admin_id, epoch } => {
                // We are the admin chain and want to remove a committee.
                ensure!(*admin_id == chain_id, Error::InvalidCommitteeRemoval);
                let recipients = match &mut self.status {
                    Some(ChainStatus::Managing { subscribers, .. }) => subscribers.clone(),
                    _ => return Err(Error::InvalidCommitteeRemoval),
                };
                ensure!(
                    self.committees.remove(epoch).is_some(),
                    Error::InvalidCommitteeRemoval
                );
                // Notify our subscribers.
                Ok((
                    vec![Effect::SetCommittees {
                        admin_id: *admin_id,
                        epoch: self.epoch.expect("chain is active"),
                        committees: self.committees.clone(),
                    }],
                    recipients,
                ))
            }
            Operation::SubscribeToNewCommittees { id, admin_id } => {
                ensure!(
                    *id == chain_id || id != admin_id,
                    Error::InvalidSubscriptionToNewCommittees(*id)
                );
                match &mut self.status {
                    Some(ChainStatus::ManagedBy {
                        admin_id: id,
                        subscribed,
                    }) if admin_id == id && !*subscribed => {
                        // Flip the value to prevent multiple subscriptions.
                        *subscribed = true;
                    }
                    _ => return Err(Error::InvalidSubscriptionToNewCommittees(*id)),
                }
                Ok((
                    vec![Effect::SubscribeToNewCommittees {
                        id: *id,
                        admin_id: *admin_id,
                    }],
                    vec![*admin_id],
                ))
            }
        }
    }

    /// Execute the recipient's side of an operation, aka a "remote effect".
    /// Effects must be executed by order of heights in the sender's chain.
    pub(crate) fn apply_effect(
        &mut self,
        chain_id: ChainId,
        effect: &Effect,
    ) -> Result<Vec<(ChainId, Effect)>, Error> {
        match effect {
            Effect::Transfer { amount, recipient } if chain_id == *recipient => {
                self.balance = self
                    .balance
                    .try_add((*amount).into())
                    .unwrap_or_else(|_| Balance::max());
                Ok(Vec::new())
            }
            Effect::SubscribeToNewCommittees { id, admin_id } if *admin_id == chain_id => {
                // We are the admin chain and are being notified that a subchain was just created.
                match &mut self.status {
                    Some(ChainStatus::Managing { subscribers, .. }) => {
                        subscribers.push(*id);
                        Ok(vec![(
                            *id,
                            Effect::SetCommittees {
                                admin_id: *admin_id,
                                epoch: self.epoch.expect("chain is active"),
                                committees: self.committees.clone(),
                            },
                        )])
                    }
                    _ => Err(Error::InvalidCrossChainRequest),
                }
            }
            Effect::SetCommittees {
                admin_id,
                epoch,
                committees,
            } if matches!(
                &self.status,
                Some(ChainStatus::ManagedBy { admin_id: id, subscribed: s }) if *s && admin_id == id
            ) =>
            {
                ensure!(
                    *epoch >= self.epoch.expect("chain is active"),
                    Error::InvalidCrossChainRequest
                );
                self.epoch = Some(*epoch);
                self.committees = committees.clone();
                Ok(Vec::new())
            }
            _ => {
                log::error!("Skipping unexpected received effect: {effect:?}");
                Ok(Vec::new())
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
