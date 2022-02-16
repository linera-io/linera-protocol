// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{base_types::*, ensure, error::Error, messages::*};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

/// State of an account.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct AccountState {
    /// The UID of the account.
    pub id: AccountId,
    /// Owner of the account. An account without owner cannot execute operations.
    pub owner: Option<AccountOwner>,
    /// Balance of the account.
    pub balance: Balance,
    /// Sequence number tracking requests.
    pub next_sequence_number: SequenceNumber,
    /// Whether we have signed a request for this sequence number already.
    pub pending: Option<Vote>,
    /// Hashes of all confirmed certificates for this sender.
    pub confirmed_log: Vec<HashValue>,
    /// Hashes of all confirmed certificates as a receiver.
    pub received_log: Vec<HashValue>,

    /// Keep sending these confirmed certificates until they are confirmed by receivers.
    pub keep_sending: HashSet<HashValue>,
    /// Same as received_log but used for deduplication.
    pub received_keys: HashSet<HashValue>,
}

impl AccountState {
    pub(crate) fn make_account_info(&self) -> AccountInfoResponse {
        AccountInfoResponse {
            account_id: self.id.clone(),
            owner: self.owner,
            balance: self.balance,
            next_sequence_number: self.next_sequence_number,
            pending: self.pending.clone(),
            count_received_certificates: self.received_log.len(),
            queried_certificate: None,
            queried_received_certificates: Vec::new(),
        }
    }

    pub fn new(id: AccountId) -> Self {
        Self {
            id,
            owner: None,
            balance: Balance::default(),
            next_sequence_number: SequenceNumber::new(),
            pending: None,
            confirmed_log: Vec::new(),
            received_log: Vec::new(),
            keep_sending: HashSet::new(),
            received_keys: HashSet::new(),
        }
    }

    pub fn create(id: AccountId, owner: AccountOwner, balance: Balance) -> Self {
        let mut account = Self::new(id);
        account.owner = Some(owner);
        account.balance = balance;
        account
    }

    /// Verify that the operation is valid and return the value to certify.
    pub(crate) fn validate_operation(&self, request: Request) -> Result<Value, Error> {
        let value = match &request.operation {
            Operation::Transfer { amount, .. } => {
                ensure!(*amount > Amount::zero(), Error::IncorrectTransferAmount);
                ensure!(
                    self.balance >= (*amount).into(),
                    Error::InsufficientFunding {
                        current_balance: self.balance
                    }
                );
                Value::Confirm(request)
            }
            Operation::OpenAccount { new_id, .. } => {
                let expected_id = request.account_id.make_child(request.sequence_number);
                ensure!(
                    new_id == &expected_id,
                    Error::InvalidNewAccountId(new_id.clone())
                );
                Value::Confirm(request)
            }
            Operation::StartConsensusInstance {
                new_id,
                functionality: Functionality::AtomicSwap { accounts },
            } => {
                // Verify the new UID.
                let expected_id = request.account_id.make_child(request.sequence_number);
                ensure!(
                    new_id == &expected_id,
                    Error::InvalidNewAccountId(new_id.clone())
                );
                // Make sure accounts are unique.
                let numbers = accounts
                    .clone()
                    .into_iter()
                    .collect::<BTreeMap<AccountId, _>>();
                ensure!(numbers.len() == accounts.len(), Error::InvalidRequestOrder);
                Value::Confirm(request)
            }
            Operation::Skip | Operation::CloseAccount | Operation::ChangeOwner { .. } => {
                // Nothing to check.
                Value::Confirm(request)
            }
            Operation::LockInto { .. } => {
                // Nothing to check.
                Value::Lock(request)
            }
        };
        Ok(value)
    }

    /// Execute the sender's side of the operation.
    pub(crate) fn apply_operation_as_sender(
        &mut self,
        operation: &Operation,
        key: HashValue,
    ) -> Result<(), Error> {
        match operation {
            Operation::OpenAccount { .. }
            | Operation::StartConsensusInstance { .. }
            | Operation::Skip => (),
            Operation::ChangeOwner { new_owner } => {
                self.owner = Some(*new_owner);
            }
            Operation::CloseAccount => {
                self.owner = None;
            }
            Operation::Transfer { amount, .. } => {
                self.balance.try_sub_assign((*amount).into())?;
            }
            Operation::LockInto { .. } => {
                // impossible under BFT assumptions.
                unreachable!("Spend and lock operation are never confirmed");
            }
        };
        self.confirmed_log.push(key);
        Ok(())
    }

    /// Execute the recipient's side of an operation.
    pub(crate) fn apply_operation_as_recipient(
        &mut self,
        operation: &Operation,
        key: HashValue,
    ) -> Result<bool, Error> {
        if self.received_keys.contains(&key) {
            // Confirmation already happened.
            return Ok(false);
        }
        match operation {
            Operation::Transfer { amount, .. } => {
                self.balance = self
                    .balance
                    .try_add((*amount).into())
                    .unwrap_or_else(|_| Balance::max());
            }
            Operation::OpenAccount { new_owner, .. } => {
                assert!(self.owner.is_none()); // guaranteed under BFT assumptions.
                self.owner = Some(*new_owner);
            }
            _ => unreachable!("Not an operation with recipients"),
        }
        self.received_keys.insert(key);
        self.received_log.push(key);
        Ok(true)
    }
}
