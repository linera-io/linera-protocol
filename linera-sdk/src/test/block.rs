// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A builder of [`Block`]s which are then signed to become [`Certificate`]s.
//!
//! Helps with the construction of blocks, adding operations and

use std::mem;

use linera_base::{
    crypto::PublicKey,
    data_types::{ApplicationPermissions, Round, Timestamp},
    identifiers::{ApplicationId, ChainId, MessageId, Owner},
    ownership::TimeoutConfig,
};
use linera_chain::data_types::{
    Block, Certificate, HashedCertificateValue, IncomingMessage, LiteVote, MessageAction,
    SignatureAggregator,
};
use linera_execution::{system::SystemOperation, Operation};

use super::TestValidator;
use crate::ToBcsBytes;

/// A helper type to build [`Block`]s using the builder pattern, and then signing them into
/// [`Certificate`]s using a [`TestValidator`].
pub struct BlockBuilder {
    block: Block,
    incoming_messages: Vec<(MessageId, MessageAction)>,
    validator: TestValidator,
}

impl BlockBuilder {
    /// Creates a new [`BlockBuilder`], initializing the block so that it belongs to a microchain.
    ///
    /// Initializes the block so that it belongs to the microchain identified by `chain_id` and
    /// owned by `owner`. It becomes the block after the specified `previous_block`, or the genesis
    /// block if [`None`] is specified.
    ///
    /// # Notes
    ///
    /// This is an internal method, because the [`BlockBuilder`] instance should be built by an
    /// [`ActiveChain`]. External users should only be able to add operations and messages to the
    /// block.
    pub(crate) fn new(
        chain_id: ChainId,
        owner: Owner,
        previous_block: Option<&Certificate>,
        validator: TestValidator,
    ) -> Self {
        let previous_block_hash = previous_block.map(|certificate| certificate.hash());
        let height = previous_block
            .map(|certificate| {
                certificate
                    .value()
                    .height()
                    .try_add_one()
                    .expect("Block height limit reached")
            })
            .unwrap_or_default();

        BlockBuilder {
            block: Block {
                epoch: 0.into(),
                chain_id,
                incoming_messages: vec![],
                operations: vec![],
                previous_block_hash,
                height,
                authenticated_signer: Some(owner),
                timestamp: Timestamp::from(0),
            },
            incoming_messages: Vec::new(),
            validator,
        }
    }

    /// Configures the timestamp of this block.
    pub fn with_timestamp(&mut self, timestamp: Timestamp) -> &mut Self {
        self.block.timestamp = timestamp;
        self
    }

    /// Adds a [`SystemOperation`] to this block.
    pub(crate) fn with_system_operation(&mut self, operation: SystemOperation) -> &mut Self {
        self.block.operations.push(operation.into());
        self
    }

    /// Adds a request to register an application on this chain.
    pub fn with_request_for_application<Abi>(
        &mut self,
        application: ApplicationId<Abi>,
    ) -> &mut Self {
        self.with_system_operation(SystemOperation::RequestApplication {
            chain_id: application.creation.chain_id,
            application_id: application.forget_abi(),
        })
    }

    /// Adds an operation to change this chain's ownership.
    pub fn with_owner_change(
        &mut self,
        super_owners: Vec<PublicKey>,
        owners: Vec<(PublicKey, u64)>,
        multi_leader_rounds: u32,
        timeout_config: TimeoutConfig,
    ) -> &mut Self {
        self.with_system_operation(SystemOperation::ChangeOwnership {
            super_owners,
            owners,
            multi_leader_rounds,
            timeout_config,
        })
    }

    /// Adds an application permissions change to this block.
    pub fn with_change_application_permissions(
        &mut self,
        permissions: ApplicationPermissions,
    ) -> &mut Self {
        self.with_system_operation(SystemOperation::ChangeApplicationPermissions(permissions))
    }

    /// Adds a user `operation` to this block.
    ///
    /// The operation is serialized using [`bcs`] and added to the block, marked to be executed by
    /// `application`.
    pub fn with_operation<Abi>(
        &mut self,
        application_id: ApplicationId<Abi>,
        operation: impl ToBcsBytes,
    ) -> &mut Self {
        self.block.operations.push(Operation::User {
            application_id: application_id.forget_abi(),
            bytes: operation
                .to_bcs_bytes()
                .expect("Failed to serialize operation"),
        });
        self
    }

    /// Receives an incoming message referenced by the [`MessageId`].
    ///
    /// The block that produces the message must have already been executed by the test validator,
    /// so that the message is already in the inbox of the microchain this block belongs to.
    pub fn with_incoming_message(&mut self, message_id: MessageId) -> &mut Self {
        self.incoming_messages
            .push((message_id, MessageAction::Accept));
        self
    }

    /// Rejects an incoming message referenced by the [`MessageId`].
    ///
    /// The block that produces the message must have already been executed by the test validator,
    /// so that the message is already in the inbox of the microchain this block belongs to.
    pub fn with_message_rejection(&mut self, message_id: MessageId) -> &mut Self {
        self.incoming_messages
            .push((message_id, MessageAction::Reject));
        self
    }

    /// Receives multiple incoming messages referenced by the [`MessageId`]s.
    ///
    /// The blocks that produce the messages must have already been executed by the test validator,
    /// so that the messages are already in the inbox of the microchain this block belongs to.
    pub fn with_incoming_messages(
        &mut self,
        message_ids: impl IntoIterator<Item = MessageId>,
    ) -> &mut Self {
        self.incoming_messages.extend(
            message_ids
                .into_iter()
                .map(|message_id| (message_id, MessageAction::Accept)),
        );
        self
    }

    /// Receives incoming messages by specifying them directly.
    ///
    /// This is an internal method that bypasses the check to see if the messages are already
    /// present in the inboxes of the microchain that owns this block.
    pub(crate) fn with_raw_messages(
        &mut self,
        messages: impl IntoIterator<Item = IncomingMessage>,
    ) -> &mut Self {
        self.block.incoming_messages.extend(messages);
        self
    }

    /// Tries to sign the prepared [`Block`] with the [`TestValidator`]'s keys and return the
    /// resulting [`Certificate`]. Returns an error if block execution fails.
    pub(crate) async fn try_sign(mut self) -> anyhow::Result<(Certificate, Vec<MessageId>)> {
        self.collect_incoming_messages().await;

        let (executed_block, _) = self
            .validator
            .worker()
            .stage_block_execution(self.block)
            .await?;

        let message_ids = (0..executed_block
            .messages()
            .iter()
            .map(Vec::len)
            .sum::<usize>() as u32)
            .map(|index| executed_block.message_id(index))
            .collect();

        let value = HashedCertificateValue::new_confirmed(executed_block);
        let vote = LiteVote::new(value.lite(), Round::Fast, self.validator.key_pair());
        let mut builder = SignatureAggregator::new(value, Round::Fast, self.validator.committee());
        let certificate = builder
            .append(vote.validator, vote.signature)
            .expect("Failed to sign block")
            .expect("Committee has more than one test validator");

        Ok((certificate, message_ids))
    }

    /// Collects and adds the previously requested messages to this block.
    ///
    /// The requested messages must already all be in the inboxes of the microchain that owns this
    /// block.
    async fn collect_incoming_messages(&mut self) {
        let chain_id = self.block.chain_id;

        for (message_id, action) in mem::take(&mut self.incoming_messages) {
            let mut message = self
                .validator
                .worker()
                .find_incoming_message(chain_id, message_id)
                .await
                .expect("Failed to find message to receive in block")
                .expect("Message that block should consume has not been emitted");

            message.action = action;

            self.block.incoming_messages.push(message);
        }
    }
}
