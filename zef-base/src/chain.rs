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

/// An inbox used to receive and execute messages from another chain.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct InboxState {
    /// We have already received the cross-chain requests and enqueued all the messages
    /// below this height.
    pub next_height_to_receive: BlockHeight,
    /// These messages have been received but not yet picked by a block to be executed.
    pub received: VecDeque<(BlockHeight, Operation)>,
    /// These messages have been executed but the cross-chain requests have not been
    /// received yet.
    pub expected: VecDeque<(BlockHeight, Operation)>,
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
            queried_pending_messages: Vec::new(),
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

    /// Verify that this chain is up-to-date and all the messages executed ahead of time
    /// have been properly received by now.
    pub fn validate_incoming_messages(&self) -> Result<(), Error> {
        for (sender_id, inbox) in &self.inboxes {
            ensure!(
                inbox.expected.is_empty(),
                Error::MissingCrossChainUpdate {
                    sender_id: sender_id.clone(),
                    height: inbox.expected.front().unwrap().0,
                }
            );
        }
        Ok(())
    }

    /// Whether an invalid operation for this block can become valid later.
    pub fn is_retriable_validation_error(error: &Error) -> bool {
        matches!(error, Error::MissingCrossChainUpdate { .. })
    }

    /// Schedule the recipient's side of an operation for execution, unless it was already
    /// executed. Returns true if the call changed the chain state. Operations must be
    /// received by order of heights in the sender's chain.
    pub fn receive_message(
        &mut self,
        sender_id: ChainId,
        height: BlockHeight,
        operation: Operation,
        key: HashValue,
    ) -> Result<bool, Error> {
        let inbox = self.inboxes.entry(sender_id).or_default();
        if height < inbox.next_height_to_receive {
            // We already received this message.
            return Ok(false);
        }
        inbox.next_height_to_receive = height.try_add_one()?;
        self.received_log.push(key);
        // Chain creation is a special operation that can only be executed (once) in this callback.
        if let Operation::OpenChain {
            owner, committee, ..
        } = &operation
        {
            assert!(!self.state.manager.is_active()); // guaranteed under BFT assumptions.
            assert!(self.state.committee.is_none());
            self.state.committee = Some(committee.clone());
            self.state.manager = ChainManager::single(*owner);
            // Proceed to scheduling the operation for "execution". Although it won't do
            // anything, it's simpler than asking block producers to skip this kind of
            // incoming message.
        }
        // Find if the message was executed ahead of time.
        if let Some((expected_height, expected_operation)) = inbox.expected.front() {
            if height == *expected_height && operation == *expected_operation {
                // We already executed this message. Remove it from the queue.
                inbox.expected.pop_front();
                return Ok(true);
            }
            // Should be unreachable under BFT assumptions.
            panic!("Given the confirmed blocks that we have seen, we were expecting a different message to come first.");
        }
        // Otherwise, schedule the message for execution.
        inbox.received.push_back((height, operation));
        Ok(true)
    }

    /// Execute a new block: first the incoming messages, then the main operation.
    pub fn execute_block(&mut self, block: &Block) -> Result<(), Error> {
        for message in &block.incoming_messages {
            let inbox = self.inboxes.entry(message.sender_id.clone()).or_default();
            match inbox.received.front() {
                Some((height, operation)) => {
                    ensure!(
                        message.height == *height && message.operation == *operation,
                        Error::InvalidMessage {
                            sender_id: message.sender_id.clone(),
                            height: message.height,
                        }
                    );
                    inbox.received.pop_front().unwrap();
                }
                None => {
                    inbox
                        .expected
                        .push_back((message.height, message.operation.clone()));
                }
            }
            self.apply_operation_as_recipient(&message.operation)?;
        }
        self.apply_operation_as_sender(block)?;
        Ok(())
    }

    /// Execute the sender's side of the operation.
    fn apply_operation_as_sender(&mut self, block: &Block) -> Result<(), Error> {
        match &block.operation {
            Operation::OpenChain { id, committee, .. } => {
                let expected_id = block.chain_id.make_child(block.height);
                ensure!(id == &expected_id, Error::InvalidNewChainId(id.clone()));
                ensure!(
                    self.state.committee.as_ref() == Some(committee),
                    Error::InvalidCommittee
                );
            }
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
                ensure!(*amount > Amount::zero(), Error::IncorrectTransferAmount);
                ensure!(
                    self.state.balance >= (*amount).into(),
                    Error::InsufficientFunding {
                        current_balance: self.state.balance
                    }
                );
                self.state.balance.try_sub_assign((*amount).into())?;
            }
        };
        Ok(())
    }

    /// Execute the recipient's side of an operation.
    /// Returns true if the operation changed the chain state.
    /// Operations must be executed by order of heights in the sender's chain.
    fn apply_operation_as_recipient(&mut self, operation: &Operation) -> Result<(), Error> {
        if let Operation::Transfer { amount, .. } = operation {
            self.state.balance = self
                .state
                .balance
                .try_add((*amount).into())
                .unwrap_or_else(|_| Balance::max());
        }
        Ok(())
    }
}
