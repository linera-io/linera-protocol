// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    committee::Committee,
    crypto::*,
    ensure,
    error::Error,
    manager::ChainManager,
    messages::{BlockHeight, ChainId, OperationId, Owner},
};
use serde::{Deserialize, Serialize};

/// Execution state of a chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ExecutionState {
    /// The UID of the chain.
    pub chain_id: ChainId,
    /// Whether our reconfigurations are managed by a "beacon" chain, or if we are it and
    /// managing other chains.
    pub status: Option<ChainStatus>,
    /// The committees that we trust.
    pub committees: Vec<Committee>,
    /// Manager of the chain.
    pub manager: ChainManager,
    /// Balance of the chain.
    pub balance: Balance,
}

/// A recipient's address.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize)]
pub enum Address {
    Burn,             // for demo purposes
    Account(ChainId), // TODO: support several accounts per chain
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

/// An chain operation.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Operation {
    /// Transfer `amount` units of value to the recipient.
    Transfer {
        recipient: Address,
        amount: Amount,
        user_data: UserData,
    },
    /// Create (or activate) a new chain by installing the given authentication key.
    /// This will automatically subscribe to future committees created by `admin_id`.
    OpenChain {
        id: ChainId,
        owner: Owner,
        admin_id: ChainId,
        committees: Vec<Committee>,
    },
    /// Close the chain.
    CloseChain,
    /// Change the authentication key of the chain.
    ChangeOwner { new_owner: Owner },
    /// Change the authentication key of the chain.
    ChangeMultipleOwners { new_owners: Vec<Owner> },
    /// Register a new committee.
    NewCommittee {
        admin_id: ChainId,
        committee: Committee,
    },
    /// Subscribe to future committees created by `admin_id`. Same as OpenChain but useful
    /// for root chains (other than admin_id) created in the genenis config.
    SubscribeToNewCommittees {
        id: ChainId,
        admin_id: ChainId,
        committees: Vec<Committee>,
    },
}

impl BcsSignable for ExecutionState {}

impl ExecutionState {
    pub fn new(chain_id: ChainId) -> Self {
        Self {
            chain_id,
            status: None,
            committees: Vec::new(),
            manager: ChainManager::default(),
            balance: Balance::default(),
        }
    }
}

/// The administrative status of this chain w.r.t reconfigurations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum ChainStatus {
    ManagedBy { admin_id: ChainId, subscribed: bool },
    Managing { subscribers: Vec<ChainId> },
}

impl ExecutionState {
    pub fn admin_id(&self) -> Option<ChainId> {
        match self.status.as_ref()? {
            ChainStatus::ManagedBy { admin_id, .. } => Some(*admin_id),
            ChainStatus::Managing { .. } => Some(self.chain_id),
        }
    }

    pub(crate) fn is_recipient(&self, operation: &Operation) -> bool {
        use Operation::*;
        match operation {
            Transfer {
                recipient: Address::Account(id),
                ..
            } => {
                // We are the recipient of the transfer
                self.chain_id == *id
            }
            OpenChain { id, admin_id, .. } => {
                // We are the admin or the created chain
                self.chain_id == *id || self.chain_id == *admin_id
            }
            SubscribeToNewCommittees { admin_id, .. } => {
                // We are the admin chain
                self.chain_id == *admin_id
            }
            NewCommittee { admin_id, .. } => {
                // We have the same admin chain
                Some(admin_id) == self.admin_id().as_ref()
            }
            _ => false,
        }
    }

    /// Execute the sender's side of the operation.
    /// Return a list of recipients who need to be notified.
    pub(crate) fn apply_operation_as_sender(
        &mut self,
        chain_id: ChainId,
        height: BlockHeight,
        index: usize,
        operation: &Operation,
    ) -> Result<Vec<ChainId>, Error> {
        let operation_id = OperationId {
            chain_id,
            height,
            index,
        };
        match operation {
            Operation::OpenChain {
                id,
                committees,
                admin_id,
                ..
            } => {
                let expected_id = ChainId::child(operation_id);
                ensure!(id == &expected_id, Error::InvalidNewChainId(*id));
                ensure!(
                    Some(admin_id) == self.admin_id().as_ref(),
                    Error::InvalidNewChainAdminId(*id)
                );
                ensure!(&self.committees == committees, Error::InvalidCommittees);
                Ok(vec![*id, *admin_id])
            }
            Operation::ChangeOwner { new_owner } => {
                self.manager = ChainManager::single(*new_owner);
                Ok(Vec::new())
            }
            Operation::ChangeMultipleOwners { new_owners } => {
                self.manager = ChainManager::multiple(new_owners.clone());
                Ok(Vec::new())
            }
            Operation::CloseChain => {
                self.manager = ChainManager::default();
                Ok(Vec::new())
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
                    Address::Burn => Ok(Vec::new()),
                    Address::Account(id) => Ok(vec![*id]),
                }
            }
            Operation::NewCommittee {
                admin_id,
                committee,
            } => {
                // We are the admin chain and want to create a committee.
                ensure!(*admin_id == chain_id, Error::InvalidCommitteeCreation);
                ensure!(
                    committee.origin == Some(operation_id),
                    Error::InvalidCommitteeCreation
                );
                // Notify our subscribers (plus ourself) to do the migration.
                let mut recipients = match &self.status {
                    Some(ChainStatus::Managing { subscribers }) => subscribers.clone(),
                    _ => return Err(Error::InvalidCommitteeCreation),
                };
                recipients.push(chain_id);
                Ok(recipients)
            }
            Operation::SubscribeToNewCommittees {
                id,
                admin_id,
                committees,
            } => {
                ensure!(
                    *id == chain_id || id != admin_id,
                    Error::InvalidSubscriptionToNewCommittees(*id)
                );
                match &mut self.status {
                    Some(ChainStatus::ManagedBy {
                        admin_id: id,
                        subscribed,
                    }) if *admin_id == *id && !*subscribed => {
                        // Flip the value to prevent multiple subscriptions.
                        *subscribed = true;
                    }
                    _ => return Err(Error::InvalidSubscriptionToNewCommittees(*id)),
                }
                ensure!(&self.committees == committees, Error::InvalidCommittees);
                Ok(vec![*admin_id])
            }
        }
    }

    /// Execute the recipient's side of an operation.
    /// Operations must be executed by order of heights in the sender's chain.
    pub(crate) fn apply_operation_as_recipient(
        &mut self,
        chain_id: ChainId,
        operation: &Operation,
    ) -> Result<Vec<(ChainId, Vec<BlockHeight>)>, Error> {
        match operation {
            Operation::Transfer { amount, .. } => {
                self.balance = self
                    .balance
                    .try_add((*amount).into())
                    .unwrap_or_else(|_| Balance::max());
                Ok(Vec::new())
            }
            Operation::OpenChain {
                id,
                admin_id,
                committees,
                ..
            }
            | Operation::SubscribeToNewCommittees {
                id,
                admin_id,
                committees,
                ..
            } if *admin_id == chain_id => {
                // We are the admin chain and are being notified that a subchain was just created.
                // First, register the new chain as a subscriber.
                match &mut self.status {
                    Some(ChainStatus::Managing { subscribers }) => {
                        subscribers.push(*id);
                    }
                    _ => return Err(Error::InvalidCrossChainRequest),
                }
                // Second, see if the new chain is missing some of our recently created
                // committees. Now is the time to issue the corresponding notifications.
                assert!(committees
                    .iter()
                    .zip(self.committees.iter())
                    .all(|(c1, c2)| c1.origin == c2.origin));
                if committees.len() < self.committees.len() {
                    let heights = self.committees[committees.len()..]
                        .iter()
                        .filter_map(|c| c.origin.as_ref().map(|operation| operation.height))
                        .collect();
                    Ok(vec![(*id, heights)])
                } else {
                    Ok(Vec::new())
                }
            }
            Operation::NewCommittee { committee, .. } => {
                self.committees.push(committee.clone());
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
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
