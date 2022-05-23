// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    base_types::*, committee::Committee, ensure, error::Error, manager::ChainManager, messages::*,
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
