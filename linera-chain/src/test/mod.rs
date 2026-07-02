// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities

mod http_server;

use std::collections::BTreeMap;

use linera_base::{
    crypto::{CryptoHash, Signer, ValidatorPublicKey},
    data_types::{Amount, Blob, BlockHeight, Epoch, Event, OracleResponse, Round, Timestamp},
    identifiers::{Account, AccountOwner, ChainId},
};
use linera_execution::{Message, MessageKind, Operation, OutgoingMessage, SystemOperation};

pub use self::http_server::HttpServer;
use crate::{
    block::{Block, ConfirmedBlock},
    data_types::{
        BlockExecutionOutcome, BlockProposal, IncomingBundle, OperationResult, PostedMessage,
        ProposedBlock, Transaction, Vote,
    },
    types::{CertificateValue, GenericCertificate},
};

/// Creates a new child of the given block, with the same timestamp.
pub fn make_child_block(parent: &ConfirmedBlock) -> ProposedBlock {
    let parent_header = &parent.block().header;
    ProposedBlock {
        epoch: parent_header.epoch,
        chain_id: parent_header.chain_id,
        transactions: vec![],
        previous_block_hash: Some(parent.hash()),
        height: parent_header.height.try_add_one().unwrap(),
        authenticated_owner: parent_header.authenticated_owner,
        timestamp: parent_header.timestamp,
    }
}

/// Creates a block at height 0 for a new chain.
pub fn make_first_block(chain_id: ChainId) -> ProposedBlock {
    ProposedBlock {
        epoch: Epoch::ZERO,
        chain_id,
        transactions: vec![],
        previous_block_hash: None,
        height: BlockHeight::ZERO,
        authenticated_owner: None,
        timestamp: Timestamp::default(),
    }
}

/// Builds a [`Block`] for tests with a header that stays consistent with its body (the
/// header is computed via [`Block::new`]), so it round-trips through serialization and
/// storage. Tests start from an empty body and add messages, events, and so on.
pub struct BlockBuilder {
    block: ProposedBlock,
    outcome: BlockExecutionOutcome,
}

impl BlockBuilder {
    /// Starts building a block at the given chain and height with an empty body.
    pub fn new(chain_id: ChainId, height: BlockHeight) -> Self {
        BlockBuilder {
            block: ProposedBlock {
                epoch: Epoch::ZERO,
                chain_id,
                transactions: vec![],
                previous_block_hash: None,
                height,
                authenticated_owner: None,
                timestamp: Timestamp::default(),
            },
            outcome: BlockExecutionOutcome {
                state_hash: CryptoHash::default(),
                messages: vec![],
                previous_message_blocks: BTreeMap::new(),
                previous_event_blocks: BTreeMap::new(),
                oracle_responses: vec![],
                events: vec![],
                blobs: vec![],
                operation_results: vec![],
            },
        }
    }

    /// Sets the execution state hash recorded in the header.
    pub fn with_state_hash(mut self, state_hash: CryptoHash) -> Self {
        self.outcome.state_hash = state_hash;
        self
    }

    /// Appends a transaction to the block's inputs.
    pub fn with_transaction(mut self, transaction: Transaction) -> Self {
        self.block.transactions.push(transaction);
        self
    }

    /// Appends one transaction's outgoing messages to the body.
    pub fn with_messages(mut self, messages: Vec<OutgoingMessage>) -> Self {
        self.outcome.messages.push(messages);
        self
    }

    /// Appends one transaction's events to the body.
    pub fn with_events(mut self, events: Vec<Event>) -> Self {
        self.outcome.events.push(events);
        self
    }

    /// Appends one transaction's oracle responses to the body.
    pub fn with_oracle_responses(mut self, oracle_responses: Vec<OracleResponse>) -> Self {
        self.outcome.oracle_responses.push(oracle_responses);
        self
    }

    /// Appends one transaction's created blobs to the body.
    pub fn with_blobs(mut self, blobs: Vec<Blob>) -> Self {
        self.outcome.blobs.push(blobs);
        self
    }

    /// Appends an operation result to the body.
    pub fn with_operation_result(mut self, operation_result: OperationResult) -> Self {
        self.outcome.operation_results.push(operation_result);
        self
    }

    /// Builds the block, computing a header that is consistent with the body.
    pub fn build(self) -> Block {
        self.outcome.with(self.block)
    }
}

/// A helper trait to simplify constructing blocks for tests.
#[allow(async_fn_in_trait)]
pub trait BlockTestExt: Sized {
    /// Returns the block with the given authenticated owner.
    fn with_authenticated_owner(self, authenticated_owner: Option<AccountOwner>) -> Self;

    /// Returns the block with the given operation appended at the end.
    fn with_operation(self, operation: impl Into<Operation>) -> Self;

    /// Returns the block with a transfer operation appended at the end.
    fn with_transfer(self, owner: AccountOwner, recipient: Account, amount: Amount) -> Self;

    /// Returns the block with a simple transfer operation appended at the end.
    fn with_simple_transfer(self, chain_id: ChainId, amount: Amount) -> Self;

    /// Returns the block with the given message appended at the end.
    fn with_incoming_bundle(self, incoming_bundle: IncomingBundle) -> Self;

    /// Returns the block with the given messages appended at the end.
    fn with_incoming_bundles(
        self,
        incoming_bundles: impl IntoIterator<Item = IncomingBundle>,
    ) -> Self;

    /// Returns the block with the specified timestamp.
    fn with_timestamp(self, timestamp: impl Into<Timestamp>) -> Self;

    /// Returns the block with the specified epoch.
    fn with_epoch(self, epoch: impl Into<Epoch>) -> Self;

    /// Returns the block with the burn operation (transfer to a special address) appended at the end.
    fn with_burn(self, amount: Amount) -> Self;

    /// Returns a block proposal in the first round in a default ownership configuration
    /// (`Round::MultiLeader(0)`) without any hashed certificate values or validated block.
    async fn into_first_proposal<S: Signer + ?Sized>(
        self,
        owner: AccountOwner,
        signer: &S,
    ) -> Result<BlockProposal, S::Error> {
        self.into_proposal_with_round(owner, signer, Round::MultiLeader(0))
            .await
    }

    /// Returns a block proposal without any hashed certificate values or validated block.
    async fn into_proposal_with_round<S: Signer + ?Sized>(
        self,
        owner: AccountOwner,
        signer: &S,
        round: Round,
    ) -> Result<BlockProposal, S::Error>;
}

impl BlockTestExt for ProposedBlock {
    fn with_authenticated_owner(mut self, authenticated_owner: Option<AccountOwner>) -> Self {
        self.authenticated_owner = authenticated_owner;
        self
    }

    fn with_operation(mut self, operation: impl Into<Operation>) -> Self {
        self.transactions
            .push(Transaction::ExecuteOperation(operation.into()));
        self
    }

    fn with_transfer(self, owner: AccountOwner, recipient: Account, amount: Amount) -> Self {
        self.with_operation(SystemOperation::Transfer {
            owner,
            recipient,
            amount,
        })
    }

    fn with_simple_transfer(self, chain_id: ChainId, amount: Amount) -> Self {
        self.with_transfer(AccountOwner::CHAIN, Account::chain(chain_id), amount)
    }

    fn with_burn(self, amount: Amount) -> Self {
        let recipient = Account::burn_address(self.chain_id);
        self.with_operation(SystemOperation::Transfer {
            owner: AccountOwner::CHAIN,
            recipient,
            amount,
        })
    }

    fn with_incoming_bundle(mut self, incoming_bundle: IncomingBundle) -> Self {
        self.transactions
            .push(Transaction::ReceiveMessages(incoming_bundle));
        self
    }

    fn with_incoming_bundles(
        mut self,
        incoming_bundles: impl IntoIterator<Item = IncomingBundle>,
    ) -> Self {
        self.transactions.extend(
            incoming_bundles
                .into_iter()
                .map(Transaction::ReceiveMessages),
        );
        self
    }

    fn with_timestamp(mut self, timestamp: impl Into<Timestamp>) -> Self {
        self.timestamp = timestamp.into();
        self
    }

    fn with_epoch(mut self, epoch: impl Into<Epoch>) -> Self {
        self.epoch = epoch.into();
        self
    }

    async fn into_proposal_with_round<S: Signer + ?Sized>(
        self,
        owner: AccountOwner,
        signer: &S,
        round: Round,
    ) -> Result<BlockProposal, S::Error> {
        BlockProposal::new_initial(owner, round, self, signer).await
    }
}

/// Helper trait to simplify creating certificates from votes in tests.
pub trait VoteTestExt<T: CertificateValue>: Sized {
    /// Returns a certificate for a committee consisting only of this validator.
    fn into_certificate(self, public_key: ValidatorPublicKey) -> GenericCertificate<T>;
}

impl<T: CertificateValue> VoteTestExt<T> for Vote<T> {
    fn into_certificate(self, public_key: ValidatorPublicKey) -> GenericCertificate<T> {
        // Preserve the vote's own signed unlocking round and first-round attestation, so this
        // works for any vote — a plain confirmation, a validated vote justified by an unlocking
        // round, or a first-round-attested confirmation — not just the default payload.
        GenericCertificate::new_with_unlocking_round_and_first_round(
            self.value,
            self.round,
            self.unlocking_round,
            self.first_round,
            vec![(public_key, self.signature)],
        )
    }
}

/// Helper trait to simplify constructing messages for tests.
pub trait MessageTestExt: Sized {
    /// Wraps the message into a [`PostedMessage`] with the given kind.
    fn to_posted(self, kind: MessageKind) -> PostedMessage;
}

impl<T: Into<Message>> MessageTestExt for T {
    fn to_posted(self, kind: MessageKind) -> PostedMessage {
        PostedMessage {
            authenticated_owner: None,
            grant: Amount::ZERO,
            refund_grant_to: None,
            kind,
            message: self.into(),
        }
    }
}
