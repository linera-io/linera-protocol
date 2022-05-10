// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    base_types::*, committee::Committee, ensure, error::Error, manager::ChainManager, messages::*,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// State of a chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ChainState {
    /// The UID of the chain.
    pub id: ChainId,
    /// Execution state.
    pub state: ExecutionState,
    /// Hash of the latest certified block in this chain, if any.
    pub block_hash: Option<HashValue>,
    /// Sequence number tracking blocks.
    pub next_block_height: BlockHeight,

    /// Hashes of all certified blocks for this sender.
    /// This ends with `block_hash` and has length `usize::from(next_block_height)`.
    pub confirmed_log: Vec<HashValue>,
    /// Hashes of all certified blocks known as a receiver (local ordering).
    pub received_log: Vec<HashValue>,

    /// Mailboxes used to send messages, indexed by recipient.
    pub outboxes: HashMap<ChainId, OutboxState>,
    /// Mailboxes used to receive messages, indexed by sender.
    pub inboxes: HashMap<ChainId, InboxState>,
}

/// An outbox used to send messages to another chain. NOTE: Messages are implied by the
/// execution of blocks, so currently we just send the certified blocks over and let the
/// receivers figure out what was the message for them.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct OutboxState {
    /// Keep sending these certified blocks of ours until they are acknowledged by
    /// receivers. Keep the height around so that we can quickly dequeue.
    pub queue: VecDeque<(BlockHeight, HashValue)>,
}

/// Aninbox used to receive and execute messages from another chain.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct InboxState {
    /// We have already received the cross-chain requests and enqueued all the messages
    /// below this height.
    pub next_height_to_receive: BlockHeight,
}

/// Execution state of a chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ExecutionState {
    /// Current committee, if any.
    pub committee: Option<Committee>,
    /// Manager of the chain.
    pub manager: ChainManager,
    /// Balance of the chain.
    pub balance: Balance,
}

impl ChainState {
    pub fn make_chain_info(&self, key_pair: Option<&KeyPair>) -> ChainInfoResponse {
        let info = ChainInfo {
            chain_id: self.id.clone(),
            manager: self.state.manager.clone(),
            balance: self.state.balance,
            queried_committee: None,
            block_hash: self.block_hash,
            next_block_height: self.next_block_height,
            queried_sent_certificates: Vec::new(),
            count_received_certificates: self.received_log.len(),
            queried_received_certificates: Vec::new(),
        };
        ChainInfoResponse::new(info, key_pair)
    }

    pub fn new(id: ChainId) -> Self {
        let state = ExecutionState {
            committee: None,
            manager: ChainManager::None,
            balance: Balance::default(),
        };
        Self {
            id,
            state,
            block_hash: None,
            next_block_height: BlockHeight::new(),
            confirmed_log: Vec::new(),
            received_log: Vec::new(),
            inboxes: HashMap::new(),
            outboxes: HashMap::new(),
        }
    }

    pub fn create(committee: Committee, id: ChainId, owner: Owner, balance: Balance) -> Self {
        let mut chain = Self::new(id);
        chain.state.committee = Some(committee);
        chain.state.manager = ChainManager::single(owner);
        chain.state.balance = balance;
        chain
    }

    /// Verify that the operation is valid and return the value to certify.
    pub fn validate_operation(&self, block: &Block) -> Result<(), Error> {
        match &block.operation {
            Operation::Transfer { amount, .. } => {
                ensure!(*amount > Amount::zero(), Error::IncorrectTransferAmount);
                ensure!(
                    self.state.balance >= (*amount).into(),
                    Error::InsufficientFunding {
                        current_balance: self.state.balance
                    }
                );
            }
            Operation::OpenChain { id, committee, .. } => {
                let expected_id = block.chain_id.make_child(block.height);
                ensure!(id == &expected_id, Error::InvalidNewChainId(id.clone()));
                ensure!(
                    self.state.committee.as_ref() == Some(committee),
                    Error::InvalidCommittee
                );
            }
            Operation::CloseChain
            | Operation::ChangeOwner { .. }
            | Operation::ChangeMultipleOwners { .. } => {
                // Nothing to check.
            }
        }
        Ok(())
    }

    /// Whether an invalid operation for this block can become valid later.
    pub fn is_retriable_validation_error(block: &Block, error: &Error) -> bool {
        match (&block.operation, error) {
            (Operation::Transfer { .. }, Error::InsufficientFunding { .. }) => true,
            (Operation::Transfer { .. }, _) => false,
            (
                Operation::OpenChain { .. }
                | Operation::CloseChain
                | Operation::ChangeOwner { .. }
                | Operation::ChangeMultipleOwners { .. },
                _,
            ) => false,
        }
    }

    /// Execute the sender's side of the operation.
    pub fn apply_operation_as_sender(
        &mut self,
        operation: &Operation,
        key: HashValue,
    ) -> Result<(), Error> {
        match operation {
            Operation::OpenChain { .. } => (),
            Operation::ChangeOwner { new_owner } => {
                self.state.manager = ChainManager::single(*new_owner);
            }
            Operation::ChangeMultipleOwners { new_owners } => {
                self.state.manager = ChainManager::multiple(new_owners.iter().cloned().collect());
            }
            Operation::CloseChain => {
                self.state.manager = ChainManager::default();
            }
            Operation::Transfer { amount, .. } => {
                self.state.balance.try_sub_assign((*amount).into())?;
            }
        };
        self.confirmed_log.push(key);
        Ok(())
    }

    /// Execute the recipient's side of an operation.
    /// Returns true if the operation changed the chain state.
    /// Operations must be executed by order of heights in the sender's chain.
    pub fn apply_operation_as_recipient(
        &mut self,
        operation: &Operation,
        key: HashValue,
        sender_id: ChainId,
        block_height: BlockHeight,
    ) -> Result<bool, Error> {
        let inbox = self.inboxes.entry(sender_id).or_default();
        if block_height < inbox.next_height_to_receive {
            // Confirmation already happened.
            return Ok(false);
        }
        match operation {
            Operation::Transfer { amount, .. } => {
                self.state.balance = self
                    .state
                    .balance
                    .try_add((*amount).into())
                    .unwrap_or_else(|_| Balance::max());
            }
            Operation::OpenChain {
                owner, committee, ..
            } => {
                assert!(!self.state.manager.is_active()); // guaranteed under BFT assumptions.
                assert!(self.state.committee.is_none());
                self.state.committee = Some(committee.clone());
                self.state.manager = ChainManager::single(*owner);
            }
            _ => unreachable!("Not an operation with recipients"),
        }
        self.received_log.push(key);
        inbox.next_height_to_receive = block_height.try_add_one()?;
        Ok(true)
    }
}
