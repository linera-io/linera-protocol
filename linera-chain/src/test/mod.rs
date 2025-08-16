// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities

mod http_server;

use linera_base::{
    crypto::{AccountPublicKey, Signer, ValidatorPublicKey},
    data_types::{Amount, BlockHeight, Epoch, Round, Timestamp},
    identifiers::{Account, AccountOwner, ChainId},
};
use linera_execution::{
    committee::{Committee, ValidatorState},
    Message, MessageKind, Operation, ResourceControlPolicy, SystemOperation,
};

pub use self::http_server::HttpServer;
use crate::{
    block::ConfirmedBlock,
    data_types::{
        BlockProposal, IncomingBundle, PostedMessage, ProposedBlock, SignatureAggregator,
        Transaction, Vote,
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
        authenticated_signer: parent_header.authenticated_signer,
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
        authenticated_signer: None,
        timestamp: Timestamp::default(),
    }
}

/// A helper trait to simplify constructing blocks for tests.
#[allow(async_fn_in_trait)]
pub trait BlockTestExt: Sized {
    /// Returns the block with the given authenticated signer.
    fn with_authenticated_signer(self, authenticated_signer: Option<AccountOwner>) -> Self;

    /// Returns the block with the given operation appended at the end.
    fn with_operation(self, operation: impl Into<Operation>) -> Self;

    /// Returns the block with a transfer operation appended at the end.
    fn with_transfer(self, owner: AccountOwner, recipient: Account, amount: Amount) -> Self;

    /// Returns the block with a simple transfer operation appended at the end.
    fn with_simple_transfer(self, chain_id: ChainId, amount: Amount) -> Self;

    /// Returns the block with the given message appended at the end.
    fn with_incoming_bundle(self, incoming_bundle: IncomingBundle) -> Self;

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
    fn with_authenticated_signer(mut self, authenticated_signer: Option<AccountOwner>) -> Self {
        self.authenticated_signer = authenticated_signer;
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

pub trait VoteTestExt<T: CertificateValue>: Sized {
    /// Returns a certificate for a committee consisting only of this validator.
    fn into_certificate(self, public_key: ValidatorPublicKey) -> GenericCertificate<T>;
}

impl<T: CertificateValue> VoteTestExt<T> for Vote<T> {
    fn into_certificate(self, public_key: ValidatorPublicKey) -> GenericCertificate<T> {
        let state = ValidatorState {
            network_address: "".to_string(),
            votes: 100,
            account_public_key: AccountPublicKey::test_key(1),
        };
        let committee = Committee::new(
            vec![(public_key, state)].into_iter().collect(),
            ResourceControlPolicy::only_fuel(),
        );
        SignatureAggregator::new(self.value, self.round, &committee)
            .append(public_key, self.signature)
            .unwrap()
            .unwrap()
    }
}

/// Helper trait to simplify constructing messages for tests.
pub trait MessageTestExt: Sized {
    fn to_posted(self, index: u32, kind: MessageKind) -> PostedMessage;
}

impl<T: Into<Message>> MessageTestExt for T {
    fn to_posted(self, index: u32, kind: MessageKind) -> PostedMessage {
        PostedMessage {
            authenticated_signer: None,
            grant: Amount::ZERO,
            refund_grant_to: None,
            kind,
            index,
            message: self.into(),
        }
    }
}
